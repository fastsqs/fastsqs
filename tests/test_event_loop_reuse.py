"""handler() must not leave the calling thread without a registered event loop.

asyncio.run() unregisters the thread's loop on exit, breaking loop consumers
that run after fastsqs in the same Lambda sandbox (e.g. Mangum multiplexed in
the same handler). handler() owns a persistent per-thread loop instead.
"""

import asyncio
import threading

import pytest

from fastsqs import BatchFailedError, FastSQS


def make_event(message_type="noop"):
    return {
        "Records": [
            {
                "messageId": "mid-1",
                "receiptHandle": "rh-1",
                "body": '{"type": "%s"}' % message_type,
                "attributes": {},
                "messageAttributes": {},
                "eventSource": "aws:sqs",
                "eventSourceARN": "arn:aws:sqs:us-east-1:000000000000:q",
                "awsRegion": "us-east-1",
            }
        ]
    }


@pytest.fixture()
def app():
    app = FastSQS()
    loops = []

    @app.route("noop")
    async def noop(payload):
        loops.append(asyncio.get_running_loop())

    app.seen_loops = loops
    return app


def test_thread_keeps_registered_loop_after_handler(app):
    asyncio.set_event_loop(None)

    app.handler(make_event(), None)

    loop = asyncio.get_event_loop()
    assert loop is not None
    assert not loop.is_closed()


def test_loop_is_reused_across_invocations(app):
    asyncio.set_event_loop(None)

    app.handler(make_event(), None)
    app.handler(make_event(), None)

    assert len(app.seen_loops) == 2
    assert app.seen_loops[0] is app.seen_loops[1]
    assert not app.seen_loops[0].is_closed()


def test_closed_registered_loop_is_replaced(app):
    stale = asyncio.new_event_loop()
    asyncio.set_event_loop(stale)
    stale.close()

    app.handler(make_event(), None)

    assert app.seen_loops[0] is not stale
    assert asyncio.get_event_loop() is app.seen_loops[0]


def test_each_thread_gets_its_own_loop(app):
    asyncio.set_event_loop(None)
    app.handler(make_event(), None)

    def run_in_thread():
        app.handler(make_event(), None)

    worker = threading.Thread(target=run_in_thread)
    worker.start()
    worker.join()

    assert len(app.seen_loops) == 2
    assert app.seen_loops[0] is not app.seen_loops[1]


def test_handler_inside_running_loop_still_raises(app):
    async def call_inside_loop():
        app.handler(make_event(), None)

    with pytest.raises(RuntimeError, match="async_handler"):
        asyncio.run(call_inside_loop())


def test_reregisters_loop_after_external_unset(app):
    asyncio.set_event_loop(None)
    app.handler(make_event(), None)

    # Qualquer asyncio.run alheio desregistra o loop da thread ao sair.
    asyncio.run(asyncio.sleep(0))

    app.handler(make_event(), None)

    assert app.seen_loops[0] is app.seen_loops[1]
    assert asyncio.get_event_loop() is app.seen_loops[1]


def test_handler_reusable_after_batch_failure():
    app = FastSQS(partial_batch_failure=False)
    loops = []

    @app.route("boom")
    async def boom(payload):
        raise ValueError("boom")

    @app.route("noop")
    async def noop(payload):
        loops.append(asyncio.get_running_loop())

    asyncio.set_event_loop(None)
    with pytest.raises(BatchFailedError):
        app.handler(make_event("boom"), None)

    app.handler(make_event("noop"), None)

    assert loops
    assert not loops[0].is_closed()
    assert asyncio.get_event_loop() is loops[0]
