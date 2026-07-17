"""TracingMiddleware: W3C Trace Context propagation into ctx.state.trace.

Reads ``traceparent``/``tracestate`` from SQS message attributes (the
transport-level channel producer instrumentation uses), falling back to a
top-level payload key (the CloudEvents extension-attribute convention).
Tracing must NEVER fail processing — malformed values are just ignored.
"""

from fastsqs import FastSQS, SQSEvent
from fastsqs.middleware import TraceContext, TracingMiddleware
from fastsqs.testing import SQSTestClient


class Task(SQSEvent):
    task_id: str


TRACEPARENT = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
TRACE_ID = "4bf92f3577b34da6a3ce929d0e0e4736"
PARENT_ID = "00f067aa0ba902b7"


def _capture_app(traces: list) -> FastSQS:
    app = FastSQS()
    app.add_middleware(TracingMiddleware())

    @app.route(Task)
    async def handle(msg: Task, ctx):
        traces.append(ctx.state.get("trace"))

    return app


def _attr(value: str) -> dict:
    return {"stringValue": value, "dataType": "String"}


def test_traceparent_read_from_message_attributes():
    traces = []
    app = _capture_app(traces)

    result = SQSTestClient(app).send(
        {"type": "task", "task_id": "1"},
        message_attributes={"traceparent": _attr(TRACEPARENT)},
    )

    assert result == {"batchItemFailures": []}
    trace = traces[0]
    assert isinstance(trace, TraceContext)
    assert trace.traceparent == TRACEPARENT
    assert trace.trace_id == TRACE_ID
    assert trace.parent_id == PARENT_ID
    assert trace.sampled is True
    assert trace.tracestate is None


def test_traceparent_attribute_name_is_case_insensitive():
    traces = []
    app = _capture_app(traces)

    SQSTestClient(app).send(
        {"type": "task", "task_id": "1"},
        message_attributes={"TraceParent": _attr(TRACEPARENT)},
    )

    assert traces[0] is not None
    assert traces[0].trace_id == TRACE_ID


def test_traceparent_falls_back_to_payload_extension_attribute():
    traces = []
    app = _capture_app(traces)

    SQSTestClient(app).send(
        {"type": "task", "task_id": "1", "traceparent": TRACEPARENT}
    )

    assert traces[0] is not None
    assert traces[0].trace_id == TRACE_ID


def test_message_attribute_wins_over_payload():
    other = "00-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-bbbbbbbbbbbbbbbb-00"
    traces = []
    app = _capture_app(traces)

    SQSTestClient(app).send(
        {"type": "task", "task_id": "1", "traceparent": other},
        message_attributes={"traceparent": _attr(TRACEPARENT)},
    )

    assert traces[0].trace_id == TRACE_ID


def test_unsampled_flag_parses_false():
    unsampled = f"00-{TRACE_ID}-{PARENT_ID}-00"
    traces = []
    app = _capture_app(traces)

    SQSTestClient(app).send(
        {"type": "task", "task_id": "1", "traceparent": unsampled}
    )

    assert traces[0].sampled is False


def test_tracestate_is_captured_alongside():
    traces = []
    app = _capture_app(traces)

    SQSTestClient(app).send(
        {"type": "task", "task_id": "1"},
        message_attributes={
            "traceparent": _attr(TRACEPARENT),
            "tracestate": _attr("vendor=opaque"),
        },
    )

    assert traces[0].tracestate == "vendor=opaque"


def test_absent_traceparent_leaves_trace_unset():
    traces = []
    app = _capture_app(traces)

    result = SQSTestClient(app).send({"type": "task", "task_id": "1"})

    assert result == {"batchItemFailures": []}
    assert traces == [None]


def test_malformed_traceparent_is_ignored_and_never_fails_processing():
    traces = []
    app = _capture_app(traces)

    result = SQSTestClient(app).send(
        {"type": "task", "task_id": "1", "traceparent": "not-a-traceparent"}
    )

    assert result == {"batchItemFailures": []}
    assert traces == [None]


def test_all_zero_trace_id_is_rejected_per_spec():
    invalid = f"00-{'0' * 32}-{PARENT_ID}-01"
    traces = []
    app = _capture_app(traces)

    SQSTestClient(app).send({"type": "task", "task_id": "1", "traceparent": invalid})

    assert traces == [None]
