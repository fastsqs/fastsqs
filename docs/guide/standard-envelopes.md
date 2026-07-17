# Consume standard event envelopes

Two envelope shapes dominate event-driven systems: [CloudEvents 1.0](https://cloudevents.io/) (the CNCF standard) and the house-style `metadata` + `data` envelope. FastSQS consumes both — CloudEvents through the `CloudEvent[T]` model, and metadata envelopes through dot-path discriminators.

## CloudEvents 1.0

A CloudEvents structured-mode JSON message carries its routing type at the top level, under `type`, which is FastSQS's default discriminator — so routing works out of the box. Model the event with `CloudEvent[T]`: the spec attributes are declared for you, and `data` is typed by the generic parameter.

```python
from pydantic import BaseModel

from fastsqs import CloudEvent, FastSQS

app = FastSQS()


class PaymentData(BaseModel):
    payment_id: str
    amount: int
    currency: str


class PaymentApproved(CloudEvent[PaymentData]):
    __message_type__ = "com.acme.payment.approved.v1"


@app.route(PaymentApproved)
async def handle_payment(msg: PaymentApproved):
    print(msg.id, msg.source, msg.data.amount)


def handler(event, context):
    return app.handler(event, context)
```

This message routes to `handle_payment`:

```json
{
  "specversion": "1.0",
  "id": "b7f6f13c-0e2a-4f8e-9d1e-2a41c3b90f11",
  "source": "/payments/checkout",
  "type": "com.acme.payment.approved.v1",
  "time": "2026-07-17T14:32:00Z",
  "data": {"payment_id": "p-1", "amount": 15990, "currency": "BRL"}
}
```

Details worth knowing:

- `id`, `source`, and `type` are required; `specversion` defaults to `"1.0"`. A message missing `data` (or failing `data` validation) becomes a batch failure like any other invalid message.
- `time` parses into a timezone-aware `datetime`.
- Spec **extension attributes** live at the top level (`traceparent`, `partitionkey`, ...). `CloudEvent` keeps them — read them via `msg.model_extra["traceparent"]`.
- Producing is the same model in reverse: `event.model_dump(mode="json", exclude_none=True)` yields the wire shape.

## Route namespaced, versioned types

`__message_type__` decouples the route key from the class name, which is what makes reverse-DNS CloudEvents types (and any other naming scheme) routable on pydantic routes. It works on plain `SQSEvent` models too:

```python
class OrderCreated(SQSEvent):
    __message_type__ = "com.acme.order.created.v1"

    order_id: str
```

The override is own-class only: a subclass without its own `__message_type__` falls back to snake_case of its class name, so reusing a model via inheritance can never silently collide on the parent's key. With `flexible_matching=True`, an override matches its exact value only — no case or format variants.

## Route a metadata + data envelope

The other common shape nests the routing type inside a metadata block. Point the discriminator at it with a dot-path — a `.` in the discriminator always means nested traversal:

```python
from fastsqs import FastSQS, SQSEvent

app = FastSQS(discriminator="metadata.eventType")


class EnvelopeMeta(SQSEvent):
    event_id: str
    event_type: str


class OrderData(SQSEvent):
    order_id: str


class OrderCreated(SQSEvent):
    metadata: EnvelopeMeta
    data: OrderData


@app.route(OrderCreated)
async def handle_order(msg: OrderCreated):
    print(msg.metadata.event_id, msg.data.order_id)
```

This message routes to `handle_order` (`metadata.eventType` resolves to `"order_created"`, the model's route key):

```json
{
  "metadata": {"eventId": "e-1", "eventType": "order_created", "occurredAt": "2026-07-17T14:32:00Z"},
  "data": {"orderId": "o-9"}
}
```

A payload where any path segment is missing — or is not an object — simply matches no route and falls to the default handler, if one is registered.

## Version your event types

Treat event schemas as public contracts: a breaking change is a **new type**, not a mutation of the old one. Encode the version in the type value (`com.acme.order.created.v2`, or `order_created_v2` via the class name `OrderCreatedV2`) and register both handlers while consumers migrate. The old route only goes away when the last producer of the old shape does.

## SNS → SQS: enable raw message delivery

When the queue subscribes to an SNS topic **without** raw message delivery, the record body is SNS's own envelope (`{"Type": "Notification", "Message": "<your JSON, stringified>"}`) — your discriminator will not resolve and every message falls to the default handler or fails as unrouted. Enable *raw message delivery* on the subscription so the body is your payload itself.
