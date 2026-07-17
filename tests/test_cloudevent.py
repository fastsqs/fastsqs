"""CloudEvent[T]: generic pydantic base for CloudEvents 1.0 structured JSON.

Consumer-side model of the CNCF spec attributes with typed ``data``, spec
extension attributes preserved via ``extra="allow"``, and routing by the
(reverse-DNS, versioned) ``type`` through ``__message_type__``.
"""

from datetime import datetime, timezone

from pydantic import BaseModel

from fastsqs import CloudEvent, FastSQS
from fastsqs.testing import SQSTestClient


class PaymentData(BaseModel):
    payment_id: str
    amount: int


class PaymentApproved(CloudEvent[PaymentData]):
    __message_type__ = "com.acme.payment.approved.v1"


def _payment_event(**overrides) -> dict:
    event = {
        "specversion": "1.0",
        "id": "b7f6f13c-0e2a-4f8e-9d1e-2a41c3b90f11",
        "source": "/payments/checkout",
        "type": "com.acme.payment.approved.v1",
        "time": "2026-07-17T14:32:00Z",
        "data": {"payment_id": "p-1", "amount": 15990},
    }
    event.update(overrides)
    return event


def _app_with_route(received: list) -> FastSQS:
    app = FastSQS()

    @app.route(PaymentApproved)
    async def handle(msg: PaymentApproved):
        received.append(msg)

    return app


def test_cloudevent_validates_and_routes_by_reverse_dns_type():
    received = []
    app = _app_with_route(received)

    result = SQSTestClient(app).send(_payment_event())

    assert result == {"batchItemFailures": []}
    assert len(received) == 1
    msg = received[0]
    assert msg.id == "b7f6f13c-0e2a-4f8e-9d1e-2a41c3b90f11"
    assert msg.source == "/payments/checkout"
    assert msg.type == "com.acme.payment.approved.v1"
    assert msg.data.payment_id == "p-1"
    assert msg.data.amount == 15990


def test_cloudevent_time_parsed_as_aware_datetime():
    received = []
    app = _app_with_route(received)

    SQSTestClient(app).send(_payment_event())

    assert received[0].time == datetime(2026, 7, 17, 14, 32, tzinfo=timezone.utc)


def test_cloudevent_time_is_optional():
    received = []
    app = _app_with_route(received)

    event = _payment_event()
    del event["time"]
    result = SQSTestClient(app).send(event)

    assert result == {"batchItemFailures": []}
    assert received[0].time is None


def test_cloudevent_specversion_defaults_to_1_0():
    received = []
    app = _app_with_route(received)

    event = _payment_event()
    del event["specversion"]
    result = SQSTestClient(app).send(event)

    assert result == {"batchItemFailures": []}
    assert received[0].specversion == "1.0"


def test_cloudevent_missing_data_fails_record():
    received = []
    app = _app_with_route(received)

    event = _payment_event()
    del event["data"]
    result = SQSTestClient(app).send(event, message_id="m-1")

    assert result == {"batchItemFailures": [{"itemIdentifier": "m-1"}]}
    assert received == []


def test_cloudevent_invalid_data_fails_record():
    received = []
    app = _app_with_route(received)

    result = SQSTestClient(app).send(
        _payment_event(data={"payment_id": "p-1", "amount": "not-a-number"}),
        message_id="m-2",
    )

    assert result == {"batchItemFailures": [{"itemIdentifier": "m-2"}]}
    assert received == []


def test_cloudevent_extension_attributes_are_preserved():
    # CloudEvents extension attributes (e.g. traceparent) live at the top level;
    # the model must keep them instead of dropping unknown keys.
    received = []
    app = _app_with_route(received)

    traceparent = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
    result = SQSTestClient(app).send(_payment_event(traceparent=traceparent))

    assert result == {"batchItemFailures": []}
    assert received[0].model_extra["traceparent"] == traceparent


def test_cloudevent_dump_produces_structured_json_shape():
    # Producer side: a constructed event dumps to the wire shape.
    event = PaymentApproved(
        id="e-1",
        source="/payments/checkout",
        type="com.acme.payment.approved.v1",
        data=PaymentData(payment_id="p-1", amount=100),
    )

    dumped = event.model_dump(mode="json", exclude_none=True)

    assert dumped == {
        "specversion": "1.0",
        "id": "e-1",
        "source": "/payments/checkout",
        "type": "com.acme.payment.approved.v1",
        "data": {"payment_id": "p-1", "amount": 100},
    }
