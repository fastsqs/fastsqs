"""Dot-path discriminators: a "." in the discriminator means nested traversal.

Covers the resolve_payload_path util and end-to-end routing of
metadata+data envelope payloads.
"""

from fastsqs import FastSQS, SQSEvent, SQSRouter
from fastsqs.testing import SQSTestClient
from fastsqs.utils import resolve_payload_path


# --- resolve_payload_path (pure helper) ---


def test_resolve_payload_path_top_level_key():
    assert resolve_payload_path({"type": "x"}, "type") == "x"


def test_resolve_payload_path_top_level_missing_returns_none():
    assert resolve_payload_path({}, "type") is None


def test_resolve_payload_path_nested():
    payload = {"metadata": {"eventType": "order_created"}}
    assert resolve_payload_path(payload, "metadata.eventType") == "order_created"


def test_resolve_payload_path_deeply_nested():
    payload = {"a": {"b": {"c": 7}}}
    assert resolve_payload_path(payload, "a.b.c") == 7


def test_resolve_payload_path_missing_intermediate_returns_none():
    assert resolve_payload_path({"data": {}}, "metadata.eventType") is None


def test_resolve_payload_path_non_dict_intermediate_returns_none():
    assert resolve_payload_path({"metadata": "flat"}, "metadata.eventType") is None


def test_resolve_payload_path_dot_always_means_traversal():
    # A literal flat key containing "." is NOT matched once the path has a dot.
    payload = {"metadata.eventType": "order_created"}
    assert resolve_payload_path(payload, "metadata.eventType") is None


# --- end-to-end routing ---


class EnvelopeMeta(SQSEvent):
    event_id: str
    event_type: str


class OrderData(SQSEvent):
    order_id: str


class OrderCreated(SQSEvent):
    metadata: EnvelopeMeta
    data: OrderData


def _envelope(event_type: str = "order_created") -> dict:
    return {
        "metadata": {"eventId": "e-1", "eventType": event_type},
        "data": {"orderId": "o-9"},
    }


def test_pydantic_route_matches_dot_path_discriminator():
    app = FastSQS(discriminator="metadata.eventType")
    received = []

    @app.route(OrderCreated)
    async def handle(msg: OrderCreated):
        received.append(msg)

    result = SQSTestClient(app).send(_envelope())

    assert result == {"batchItemFailures": []}
    assert len(received) == 1
    assert received[0].data.order_id == "o-9"
    assert received[0].metadata.event_id == "e-1"


def test_dot_path_missing_intermediate_falls_to_default():
    app = FastSQS(discriminator="metadata.eventType")
    calls = []

    @app.route(OrderCreated)
    async def handle(msg: OrderCreated):
        calls.append("typed")

    @app.default()
    async def fallback(payload):
        calls.append("default")

    result = SQSTestClient(app).send({"data": {"orderId": "o-9"}})

    assert result == {"batchItemFailures": []}
    assert calls == ["default"]


def test_dot_path_non_dict_intermediate_falls_to_default():
    app = FastSQS(discriminator="metadata.eventType")
    calls = []

    @app.route(OrderCreated)
    async def handle(msg: OrderCreated):
        calls.append("typed")

    @app.default()
    async def fallback(payload):
        calls.append("default")

    result = SQSTestClient(app).send({"metadata": "not-a-dict", "data": {}})

    assert result == {"batchItemFailures": []}
    assert calls == ["default"]


def test_dot_path_unmatched_without_default_fails_record():
    app = FastSQS(discriminator="metadata.eventType")

    @app.route(OrderCreated)
    async def handle(msg: OrderCreated):
        pass

    result = SQSTestClient(app).send(_envelope("unknown_event"), message_id="m-7")

    assert result == {"batchItemFailures": [{"itemIdentifier": "m-7"}]}


def test_key_value_subrouter_with_dot_path_discriminator():
    app = FastSQS()
    router = SQSRouter(discriminator="meta.kind")
    seen = []

    @router.route("audit")
    async def handle_audit(payload):
        seen.append(payload["meta"]["kind"])

    app.include_router(router)

    result = SQSTestClient(app).send({"meta": {"kind": "audit"}, "detail": {}})

    assert result == {"batchItemFailures": []}
    assert seen == ["audit"]
