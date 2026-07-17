"""SkipMessage: ack a record as SUCCESS without (further) processing.

Raised from a middleware ``before`` or from a handler, the record must NOT
land in batchItemFailures (no redelivery) — the semantic a dedup/idempotency
middleware needs for duplicates.
"""

import logging

from fastsqs import FastSQS, FastSQSError, SkipMessage, SQSEvent
from fastsqs.middleware import Middleware, TimingMiddleware
from fastsqs.testing import RecordSpec, SQSTestClient


class Task(SQSEvent):
    task_id: str


def test_skip_message_is_not_a_fastsqs_error():
    # Control-flow signal, NOT an error: a user's blanket `except FastSQSError`
    # error handling must never swallow an ack.
    assert not issubclass(SkipMessage, FastSQSError)
    assert issubclass(SkipMessage, Exception)


def test_handler_raising_skip_message_acks_the_record():
    app = FastSQS()

    @app.route(Task)
    async def handle(msg: Task):
        raise SkipMessage("already processed")

    result = SQSTestClient(app).send({"type": "task", "task_id": "1"})

    assert result == {"batchItemFailures": []}


def test_before_middleware_raising_skip_message_skips_handler():
    calls = []

    class Skipper(Middleware):
        async def before(self, payload, record, context, ctx):
            raise SkipMessage("duplicate")

    app = FastSQS()
    app.add_middleware(Skipper())

    @app.route(Task)
    async def handle(msg: Task):
        calls.append("handler")

    result = SQSTestClient(app).send({"type": "task", "task_id": "1"})

    assert result == {"batchItemFailures": []}
    assert calls == []


def test_entered_middlewares_unwind_with_the_skip_as_error():
    seen = []

    class Recorder(Middleware):
        async def after(self, payload, record, context, ctx, error):
            seen.append(error)

    class Skipper(Middleware):
        async def before(self, payload, record, context, ctx):
            raise SkipMessage("duplicate")

    app = FastSQS()
    app.add_middleware(Recorder())  # enters before Skipper raises
    app.add_middleware(Skipper())

    @app.route(Task)
    async def handle(msg: Task):
        pass

    result = SQSTestClient(app).send({"type": "task", "task_id": "1"})

    assert result == {"batchItemFailures": []}
    assert len(seen) == 1
    assert isinstance(seen[0], SkipMessage)


def test_skip_does_not_halt_fifo_group():
    processed = []

    app = FastSQS()

    @app.route(Task)
    async def handle(msg: Task, payload: dict):
        if payload.get("skip"):
            raise SkipMessage("duplicate")
        processed.append(msg.task_id)

    result = SQSTestClient(app).send_batch(
        [
            RecordSpec({"type": "task", "task_id": "1", "skip": True}, group_id="g1"),
            RecordSpec({"type": "task", "task_id": "2"}, group_id="g1"),
        ]
    )

    assert result == {"batchItemFailures": []}
    assert processed == ["2"]


def test_skip_with_partial_batch_failure_disabled_does_not_fail_batch():
    app = FastSQS(partial_batch_failure=False)

    @app.route(Task)
    async def handle(msg: Task):
        raise SkipMessage("duplicate")

    result = SQSTestClient(app).send({"type": "task", "task_id": "1"})

    assert result == {"batchItemFailures": []}


def test_timing_middleware_logs_skipped_status(caplog):
    app = FastSQS()
    app.add_middleware(TimingMiddleware())

    @app.route(Task)
    async def handle(msg: Task):
        raise SkipMessage("duplicate")

    with caplog.at_level(logging.INFO, logger="fastsqs"):
        SQSTestClient(app).send({"type": "task", "task_id": "1"})

    assert any("status=SKIPPED" in message for message in caplog.messages)
