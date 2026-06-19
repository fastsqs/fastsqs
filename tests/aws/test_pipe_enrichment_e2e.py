"""Real-AWS e2e through an EventBridge Pipe: SQS -> SFN Express enrichment ->
Lambda target running fastsqs (opt-in: ``pytest --run-aws``).

This is the contract fastsqs 1.1.0 targets, validated end-to-end against a real
EventBridge Pipe (not a mock):

- the Lambda receives the batch as a bare JSON **list** (the Pipes target shape),
  not ``{"Records":[...]}`` — fastsqs must accept it;
- the SFN enrichment injects a routing field (``service``) into each record's body
  AND preserves the SQS envelope (``messageId``) via ``$merge``, so fastsqs's
  partial-batch-failure (keyed on ``messageId``) is honored THROUGH the pipe — only
  the poison record redrives to the DLQ, never its good siblings;
- a record whose handler depends on the injected field proves the enrichment ran.

Co-batching is forced deterministically: a FIFO single-group source behind a pipe
created ``DesiredState=STOPPED``. We enqueue one ``SendMessageBatch`` while the pipe
is OFF, then ``start()`` so the first poll grabs the whole group as one execution
(a standard source would split the records across pollers and defeat co-batching).
Counts stay tiny. Harness: ``pipe_pipeline`` in conftest.py.
"""

import json

import pytest

pytestmark = pytest.mark.aws


def _entries(records):
    """Build SendMessageBatch entries from (task_id, group) tuples."""
    out = []
    for i, rec in enumerate(records):
        task_id, group = rec if isinstance(rec, tuple) else (rec, None)
        entry = {"Id": f"m{i}", "MessageBody": json.dumps({"type": "task", "task_id": task_id})}
        if group is not None:
            entry["MessageGroupId"] = group
        out.append(entry)
    return out


def _dlq_task_ids(drain, dlq_url, min_count, timeout=300):
    """Drain the DLQ and return the set of task_id identifiers. The DLQ holds the
    ORIGINAL source message body, so task_id is still readable from the JSON body."""
    ids = set()
    for body in drain(dlq_url, timeout=timeout, min_count=min_count):
        ids.add(json.loads(body)["task_id"])
    return ids


def test_pipe_partial_failure_only_poison_redrives(aws, pipe_pipeline, drain):
    """ok-A, ok-B, boom-C co-batched through the pipe: ONLY boom-C dead-letters.

    Proves end-to-end that (1) fastsqs processed the Pipe-delivered list, (2) it
    reported boom-C by its source messageId, (3) the pipe honored that through the
    SFN enrichment, which preserved messageId via $merge. If the enrichment had
    stripped messageId, the whole batch would redrive (ok-A/ok-B would DLQ too).
    """
    sqs = aws["sqs"]
    src_url, dlq_url, start = pipe_pipeline(fifo=True, max_receive_count=1, start_stopped=True)

    # One atomic batch (single group G) while the pipe is OFF, so start() coalesces
    # all three into one pipe execution / one fastsqs invocation.
    sqs.send_message_batch(
        QueueUrl=src_url, Entries=_entries([("ok-A", "G"), ("ok-B", "G"), ("boom-C", "G")])
    )
    start()

    dlq_ids = _dlq_task_ids(drain, dlq_url, min_count=1)
    assert "boom-C" in dlq_ids
    assert "ok-A" not in dlq_ids and "ok-B" not in dlq_ids, (
        "only the poison should redrive; good siblings must not -> partial-batch-"
        "failure works THROUGH the pipe and the enrichment preserved messageId"
    )


def test_pipe_enrichment_injects_service_into_body(aws, pipe_pipeline, drain):
    """The SFN enrichment injects service='enriched' into each record body. The
    'svc-check' task FAILS unless that field is present at the target, so its
    ABSENCE from the DLQ proves the enrichment ran and the body merge reached
    fastsqs. A 'boom-anchor' poison anchors the timing (it MUST dead-letter, so by
    the time we see it the svc-check sibling has already been processed).
    """
    sqs = aws["sqs"]
    src_url, dlq_url, start = pipe_pipeline(fifo=True, max_receive_count=1, start_stopped=True)

    sqs.send_message_batch(
        QueueUrl=src_url, Entries=_entries([("svc-check", "G"), ("boom-anchor", "G")])
    )
    start()

    dlq_ids = _dlq_task_ids(drain, dlq_url, min_count=1)
    assert "boom-anchor" in dlq_ids, "the poison control must dead-letter (timing anchor)"
    assert "svc-check" not in dlq_ids, (
        "svc-check succeeds only if the enrichment injected service='enriched' into "
        "the body and it reached the fastsqs target"
    )
