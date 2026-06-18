"""Real-AWS e2e: the v1 feature surface (DI, typed Context, QueueType.AUTO
inference, multi-type routing) validated in the real Lambda runtime
(opt-in: ``pytest --run-aws``).

These can't be proven in-process: they assert that dependency injection,
typed-Context attribute access, and ARN-based FIFO/STANDARD inference all behave
correctly when fastsqs runs on a deployed python3.13 Lambda behind a real SQS
event-source mapping. The deployed handler (``_e2e_handler.py``) FAILS a check
message unless the feature worked, so a failure would land in the DLQ. Each test
pairs the feature message with a known-poison control: once the control reaches
the DLQ enough time has elapsed that a successful feature message would already
have been deleted, so its ABSENCE from the DLQ proves the feature worked.
Harness in conftest.py.
"""

import json

import pytest

pytestmark = pytest.mark.aws


def _dlq_ids(drain, dlq_url, min_count):
    """Drain the DLQ and return the set of task_id/order_id/type identifiers."""
    out = set()
    for body in drain(dlq_url, timeout=180, min_count=min_count):
        d = json.loads(body)
        out.add(d.get("task_id") or d.get("order_id") or d.get("type"))
    return out


def test_di_and_typed_standard_context_resolve_on_real_lambda(aws, pipeline, drain):
    """Depends() injection and typed Context (AUTO -> STANDARD) work on the real
    Lambda: di-check / ctx-std-check succeed; only the poison control redrives."""
    sqs = aws["sqs"]
    main_url, dlq_url = pipeline(fifo=False, max_receive_count=2)

    def send(task_id):
        sqs.send_message(
            QueueUrl=main_url,
            MessageBody=json.dumps({"type": "task", "task_id": task_id}),
        )

    send("di-check")        # fails unless Depends(get_marker) injected "INJECTED"
    send("ctx-std-check")   # fails unless AUTO inferred STANDARD and fifo_info is None
    send("boom-control")    # known poison -> MUST reach the DLQ

    ids = _dlq_ids(drain, dlq_url, min_count=1)
    assert "boom-control" in ids                                  # DLQ path works
    assert "di-check" not in ids and "ctx-std-check" not in ids   # DI + typed Context ok


def test_auto_fifo_inference_and_fifo_context_on_real_lambda(aws, pipeline, drain):
    """On a real .fifo queue, AUTO infers FIFO and ctx.fifo_info is populated:
    ctx-fifo-check (group G1) succeeds; the poison control (group G2) redrives
    without blocking G1."""
    sqs = aws["sqs"]
    main_url, dlq_url = pipeline(fifo=True, max_receive_count=2)

    def send(task_id, group):
        sqs.send_message(
            QueueUrl=main_url,
            MessageBody=json.dumps({"type": "task", "task_id": task_id}),
            MessageGroupId=group,
        )

    send("ctx-fifo-check", "G1")   # fails unless AUTO->FIFO and fifo_info.message_group_id set
    send("boom-control", "G2")     # poison in a different group -> reaches the DLQ

    ids = _dlq_ids(drain, dlq_url, min_count=1)
    assert "boom-control" in ids
    assert "ctx-fifo-check" not in ids   # AUTO FIFO inference + typed fifo_info ok


def test_multi_type_routing_partial_failure_on_real_lambda(aws, pipeline, drain):
    """A batch mixing two routed types plus an unroutable type: only the failing
    order and the unroutable message redrive; the good task and order succeed."""
    sqs = aws["sqs"]
    main_url, dlq_url = pipeline(fifo=False, max_receive_count=2)

    def send(body):
        sqs.send_message(QueueUrl=main_url, MessageBody=json.dumps(body))

    send({"type": "task", "task_id": "ok-1"})          # routed -> Task, succeeds
    send({"type": "order", "order_id": "ord-1"})       # routed -> Order, succeeds
    send({"type": "order", "order_id": "boom-ord"})    # routed -> Order, fails
    send({"type": "nonexistent", "x": 1})              # no route, no default -> fails

    ids = _dlq_ids(drain, dlq_url, min_count=2)
    assert "boom-ord" in ids        # failing order redrives
    assert "nonexistent" in ids     # unroutable type redrives (its body has only "type")
    assert "ok-1" not in ids and "ord-1" not in ids   # good task + order succeeded


def test_standard_full_batch_all_succeed_on_real_lambda(aws, pipeline, drain):
    """A full standard batch of 10 good records all succeed (none redrive); a
    lone poison control confirms the DLQ path and anchors the timing."""
    sqs = aws["sqs"]
    main_url, dlq_url = pipeline(fifo=False, max_receive_count=2)

    for i in range(10):
        sqs.send_message(
            QueueUrl=main_url,
            MessageBody=json.dumps({"type": "task", "task_id": f"ok-{i}"}),
        )
    sqs.send_message(
        QueueUrl=main_url,
        MessageBody=json.dumps({"type": "task", "task_id": "boom-z"}),
    )

    ids = _dlq_ids(drain, dlq_url, min_count=1)
    assert "boom-z" in ids                                    # control redrove
    assert not any(i and i.startswith("ok-") for i in ids)    # all 10 good records succeeded
