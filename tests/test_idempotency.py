"""IdempotencyMiddleware: at-least-once dedup behind a structural Protocol port.

Three-state acquire semantics:
- ACQUIRED    -> process
- COMPLETED   -> SkipMessage (ack duplicate, no redelivery)
- IN_PROGRESS -> IdempotencyInProgressError (fail record; SQS redelivers after
                 the in-flight attempt settles — skipping here could lose the
                 message if the in-flight attempt fails)
"""

import asyncio

import pytest

from fastsqs import (
    FastSQS,
    FastSQSError,
    IdempotencyInProgressError,
    SkipMessage,
    SQSEvent,
)
from fastsqs.middleware import (
    AcquireResult,
    IdempotencyMiddleware,
    IdempotencyStore,
    InMemoryIdempotencyStore,
)
from fastsqs.testing import SQSTestClient


class Task(SQSEvent):
    task_id: str


def _make_app(store, processed, **middleware_kwargs) -> FastSQS:
    app = FastSQS()
    app.add_middleware(IdempotencyMiddleware(store, **middleware_kwargs))

    @app.route(Task)
    async def handle(msg: Task):
        processed.append(msg.task_id)

    return app


def _task(event_id: str = "evt-1", task_id: str = "t-1") -> dict:
    return {"type": "task", "id": event_id, "task_id": task_id}


def test_in_progress_error_is_a_fastsqs_error():
    assert issubclass(IdempotencyInProgressError, FastSQSError)


def test_duplicate_delivery_after_completion_is_skipped():
    processed = []
    app = _make_app(InMemoryIdempotencyStore(), processed)
    client = SQSTestClient(app)

    first = client.send(_task())
    second = client.send(_task())

    assert first == {"batchItemFailures": []}
    assert second == {"batchItemFailures": []}
    assert processed == ["t-1"]


def test_distinct_keys_both_process():
    processed = []
    app = _make_app(InMemoryIdempotencyStore(), processed)
    client = SQSTestClient(app)

    client.send(_task(event_id="evt-1", task_id="a"))
    client.send(_task(event_id="evt-2", task_id="b"))

    assert processed == ["a", "b"]


def test_resolved_key_is_exposed_in_ctx_state():
    seen_keys = []
    app = FastSQS()
    app.add_middleware(IdempotencyMiddleware(InMemoryIdempotencyStore()))

    @app.route(Task)
    async def handle(msg: Task, ctx):
        seen_keys.append(ctx.state.idempotency_key)

    SQSTestClient(app).send(_task(event_id="evt-42"))

    assert seen_keys == ["evt-42"]


def test_handler_failure_releases_claim_so_redelivery_retries():
    attempts = []
    app = FastSQS()
    app.add_middleware(IdempotencyMiddleware(InMemoryIdempotencyStore()))

    @app.route(Task)
    async def handle(msg: Task):
        attempts.append(msg.task_id)
        if len(attempts) == 1:
            raise ValueError("transient")

    client = SQSTestClient(app)
    first = client.send(_task(), message_id="m-1")
    second = client.send(_task())

    assert first == {"batchItemFailures": [{"itemIdentifier": "m-1"}]}
    assert second == {"batchItemFailures": []}
    assert attempts == ["t-1", "t-1"]


def test_concurrent_duplicate_in_same_batch_fails_not_skips():
    # While the first copy is IN-FLIGHT, the duplicate must FAIL (redeliver
    # later), not skip: the in-flight attempt might still fail.
    processed = []
    app = FastSQS()
    app.add_middleware(IdempotencyMiddleware(InMemoryIdempotencyStore()))

    @app.route(Task)
    async def handle(msg: Task):
        await asyncio.sleep(0)  # yield so the duplicate runs while in-flight
        processed.append(msg.task_id)

    result = SQSTestClient(app).send_batch([_task(), _task()])

    assert result == {"batchItemFailures": [{"itemIdentifier": "m1"}]}
    assert processed == ["t-1"]


def test_key_path_supports_dot_paths():
    processed = []
    store = InMemoryIdempotencyStore()
    app = _make_app(store, processed, key_path="metadata.eventId")
    client = SQSTestClient(app)

    envelope = {"type": "task", "task_id": "t-1", "metadata": {"eventId": "e-9"}}
    client.send(envelope)
    client.send(envelope)

    assert processed == ["t-1"]


def test_missing_key_fails_record_by_default():
    processed = []
    app = _make_app(InMemoryIdempotencyStore(), processed)

    result = SQSTestClient(app).send(
        {"type": "task", "task_id": "t-1"}, message_id="m-3"
    )

    assert result == {"batchItemFailures": [{"itemIdentifier": "m-3"}]}
    assert processed == []


def test_missing_key_processes_without_dedup_when_not_required():
    processed = []
    app = _make_app(InMemoryIdempotencyStore(), processed, require_key=False)
    client = SQSTestClient(app)

    client.send({"type": "task", "task_id": "a"})
    client.send({"type": "task", "task_id": "a"})

    assert processed == ["a", "a"]


def test_completed_claim_expires_after_ttl():
    now = [1000.0]
    store = InMemoryIdempotencyStore(clock=lambda: now[0])
    processed = []
    app = _make_app(store, processed, completed_ttl_seconds=100)
    client = SQSTestClient(app)

    client.send(_task())
    now[0] += 50
    client.send(_task())  # inside dedup window -> skipped
    now[0] += 100
    client.send(_task())  # window expired -> processes again

    assert processed == ["t-1", "t-1"]


def test_expired_in_progress_lease_can_be_reacquired():
    # A crashed worker (Lambda timeout = SIGKILL, no cleanup) must not lock the
    # key forever: the IN_PROGRESS claim is a lease, reacquirable after
    # in_progress_ttl_seconds. Simulate the dead worker by acquiring directly
    # on the store and never releasing.
    now = [1000.0]
    store = InMemoryIdempotencyStore(clock=lambda: now[0])
    processed = []
    app = _make_app(store, processed, in_progress_ttl_seconds=60)
    client = SQSTestClient(app)

    asyncio.run(store.try_acquire("evt-1", 60))

    while_leased = client.send(_task(), message_id="m-a")
    assert while_leased == {"batchItemFailures": [{"itemIdentifier": "m-a"}]}
    assert processed == []

    now[0] += 120  # lease expired
    after_expiry = client.send(_task())

    assert after_expiry == {"batchItemFailures": []}
    assert processed == ["t-1"]


def test_handler_skip_message_marks_key_completed():
    processed = []
    app = FastSQS()
    app.add_middleware(IdempotencyMiddleware(InMemoryIdempotencyStore()))

    @app.route(Task)
    async def handle(msg: Task):
        processed.append(msg.task_id)
        raise SkipMessage("nothing to do")

    client = SQSTestClient(app)
    client.send(_task())
    client.send(_task())  # duplicate of a skipped-but-completed record

    assert processed == ["t-1"]


def test_store_that_does_not_satisfy_protocol_is_rejected():
    with pytest.raises(TypeError):
        IdempotencyMiddleware(object())


def test_custom_store_works_structurally_without_inheritance():
    # A plain class with the right methods satisfies the Protocol port — no
    # fastsqs base classes involved. Also pins the after() calls: mark_complete
    # on success, forget on failure.
    class SpyStore:
        def __init__(self):
            self.calls = []

        async def try_acquire(self, key, ttl_seconds):
            self.calls.append(("try_acquire", key, ttl_seconds))
            return AcquireResult.ACQUIRED

        async def mark_complete(self, key, ttl_seconds):
            self.calls.append(("mark_complete", key, ttl_seconds))

        async def forget(self, key):
            self.calls.append(("forget", key))

    store = SpyStore()
    assert isinstance(store, IdempotencyStore)

    app = FastSQS()
    app.add_middleware(
        IdempotencyMiddleware(
            store, in_progress_ttl_seconds=300, completed_ttl_seconds=3600
        )
    )

    @app.route(Task)
    async def handle(msg: Task, payload: dict):
        if payload.get("boom"):
            raise ValueError("boom")

    client = SQSTestClient(app)
    client.send(_task(event_id="ok"))
    client.send({**_task(event_id="bad"), "boom": True})

    assert store.calls == [
        ("try_acquire", "ok", 300),
        ("mark_complete", "ok", 3600),
        ("try_acquire", "bad", 300),
        ("forget", "bad"),
    ]
