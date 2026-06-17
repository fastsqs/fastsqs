"""Real-AWS: SQS messageAttributes round-trip (opt-in: ``pytest --run-aws``).

Custom message attributes survive the trip through SQS and are available on the
received record (where a handler would read tracing/correlation ids).
Uses the ``gabe`` profile; creates + deletes a throwaway queue.
"""

import json
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
    url = sqs.create_queue(QueueName=f"fastsqs-attrs-{uuid.uuid4().hex[:8]}")["QueueUrl"]
    try:
        yield url
    finally:
        sqs.delete_queue(QueueUrl=url)


def test_message_attributes_preserved(sqs, queue):
    sqs.send_message(
        QueueUrl=queue,
        MessageBody=json.dumps({"task_id": "a1"}),
        MessageAttributes={
            "RequestId": {"DataType": "String", "StringValue": "req-123"},
            "Priority": {"DataType": "String", "StringValue": "high"},
        },
    )

    r = sqs.receive_message(
        QueueUrl=queue,
        MaxNumberOfMessages=1,
        WaitTimeSeconds=3,
        MessageAttributeNames=["All"],
    )
    msgs = r.get("Messages", [])
    assert msgs, "message should be received"
    m = msgs[0]
    sqs.delete_message(QueueUrl=queue, ReceiptHandle=m["ReceiptHandle"])

    attrs = m["MessageAttributes"]
    assert attrs["RequestId"]["StringValue"] == "req-123"
    assert attrs["Priority"]["StringValue"] == "high"
