# Propagate trace context

Distributed traces survive an SQS hop when the producer sends the W3C `traceparent` along with the message. `TracingMiddleware` picks it up on the consumer side and exposes it as a typed `TraceContext` at `ctx.state.trace` — FastSQS takes no tracing dependency; you hand the context to whatever tracer you use.

```python
from fastsqs import Context, FastSQS, SQSEvent, TracingMiddleware

app = FastSQS()
app.add_middleware(TracingMiddleware())


class OrderCreated(SQSEvent):
    order_id: str


@app.route(OrderCreated)
async def handle_order(msg: OrderCreated, ctx: Context):
    trace = ctx.state.get("trace")
    if trace is not None:
        print(trace.trace_id, trace.parent_id, trace.sampled)


def handler(event, context):
    return app.handler(event, context)
```

## Where the value comes from

Two sources, in precedence order:

1. **SQS message attributes** (`traceparent`, matched case-insensitively) — the transport-level channel producer instrumentation uses:

    ```python
    sqs.send_message(
        QueueUrl=queue_url,
        MessageBody=json.dumps(payload),
        MessageAttributes={
            "traceparent": {"DataType": "String", "StringValue": traceparent},
        },
    )
    ```

2. **Top-level payload keys** — the CloudEvents extension-attribute convention (`{"traceparent": "..."}` next to `type` and `data`).

`tracestate` is captured the same way and carried verbatim on `TraceContext.tracestate`.

Malformed or absent values never fail processing: `ctx.state.get("trace")` is simply `None`. Invalid per spec (and therefore ignored): a non-matching format, version `ff`, and all-zero trace or parent ids.

## Hand off to OpenTelemetry

`TraceContext` is propagator-ready — rebuild the carrier and extract:

```python
from opentelemetry import trace as otel_trace
from opentelemetry.trace.propagation.tracecontext import TraceContextTextMapPropagator

tracer = otel_trace.get_tracer("orders-consumer")


@app.route(OrderCreated)
async def handle_order(msg: OrderCreated, ctx: Context):
    trace = ctx.state.get("trace")
    carrier = {}
    if trace is not None:
        carrier["traceparent"] = trace.traceparent
        if trace.tracestate:
            carrier["tracestate"] = trace.tracestate
    parent = TraceContextTextMapPropagator().extract(carrier)

    with tracer.start_as_current_span("handle_order", context=parent):
        ...
```

The consumer span joins the producer's trace, and the whole flow — API call, queue hop, handler — reads as one trace in your backend.
