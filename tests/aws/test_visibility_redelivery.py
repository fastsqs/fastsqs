"""Real-AWS: SQS visibility-timeout redelivery (opt-in: ``pytest --run-aws``).

A received-but-not-deleted message reappears after the visibility timeout, with
ApproximateReceiveCount incremented — the mechanism fastsqs relies on for retry.
Uses the ``gabe`` profile; creates + deletes a throwaway queue.
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
def queue(sqs):
    url = sqs.create_queue(
        QueueName=f"fastsqs-vis-{uuid.uuid4().hex[:8]}",
        Attributes={"VisibilityTimeout": "2"},
    )["QueueUrl"]
    try:
        yield url
    finally:
        sqs.delete_queue(QueueUrl=url)


def test_message_redelivered_after_visibility_timeout(sqs, queue):
    sqs.send_message(QueueUrl=queue, MessageBody=json.dumps({"task_id": "v1"}))

    counts = []
    for _ in range(2):
        r = sqs.receive_message(
            QueueUrl=queue,
            MaxNumberOfMessages=1,
            WaitTimeSeconds=1,
            AttributeNames=["ApproximateReceiveCount"],
        )
        msgs = r.get("Messages", [])
        assert msgs, "message should be (re)deliverable"
        counts.append(int(msgs[0]["Attributes"]["ApproximateReceiveCount"]))
        # do NOT delete -> let the visibility timeout expire so it redelivers
        time.sleep(3)

    assert counts == [1, 2]  # redelivered, receive count incremented
