"""Real-AWS: SQS FIFO content-based deduplication (opt-in: ``pytest --run-aws``).

With ContentBasedDeduplication, identical message bodies sent within the 5-minute
dedup window collapse to a single deliverable message — so a fastsqs handler sees
the message once. Uses the ``gabe`` profile; creates + deletes a throwaway queue.
"""

import json
import time
import uuid

import boto3
import pytest

pytestmark = pytest.mark.aws

REGION = "us-east-1"
PROFILE = "gabe"


@pytest.fixture(scope="module")
def sqs():
    return boto3.Session(profile_name=PROFILE, region_name=REGION).client("sqs")


@pytest.fixture
def fifo_queue(sqs):
    url = sqs.create_queue(
        QueueName=f"fastsqs-dedup-{uuid.uuid4().hex[:8]}.fifo",
        Attributes={"FifoQueue": "true", "ContentBasedDeduplication": "true"},
    )["QueueUrl"]
    try:
        yield url
    finally:
        sqs.delete_queue(QueueUrl=url)


def test_content_based_dedup_drops_duplicates(sqs, fifo_queue):
    body = json.dumps({"task_id": "dup-1"})
    for _ in range(3):  # 3 identical sends -> dedup collapses to 1
        sqs.send_message(QueueUrl=fifo_queue, MessageBody=body, MessageGroupId="g")

    received = []
    deadline = time.time() + 15
    while time.time() < deadline:
        r = sqs.receive_message(QueueUrl=fifo_queue, MaxNumberOfMessages=10, WaitTimeSeconds=2)
        for m in r.get("Messages", []):
            received.append(m)
            sqs.delete_message(QueueUrl=fifo_queue, ReceiptHandle=m["ReceiptHandle"])
        if received:
            break

    assert len(received) == 1  # deduplicated to a single message
    assert json.loads(received[0]["Body"])["task_id"] == "dup-1"
