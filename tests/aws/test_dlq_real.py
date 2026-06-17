"""Real-AWS test: SQS native dead-letter redrive (opt-in: ``pytest --run-aws``).

fastsqs does NOT manage dead-letter queues — it reports partial batch failures
(``batchItemFailures``) and lets SQS redrive. This verifies the native AWS
mechanism that fastsqs relies on: a message that keeps failing (received past
``maxReceiveCount`` without being deleted) is moved to the DLQ by SQS itself,
with no application code involved.

Uses the ``gabe`` profile (us-east-1); creates + deletes throwaway queues.
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
def main_and_dlq(sqs):
    suffix = uuid.uuid4().hex[:8]
    dlq_url = sqs.create_queue(QueueName=f"fastsqs-dlq-{suffix}")["QueueUrl"]
    dlq_arn = sqs.get_queue_attributes(QueueUrl=dlq_url, AttributeNames=["QueueArn"])[
        "Attributes"
    ]["QueueArn"]
    main_url = sqs.create_queue(
        QueueName=f"fastsqs-main-{suffix}",
        Attributes={
            "VisibilityTimeout": "1",
            "RedrivePolicy": json.dumps(
                {"deadLetterTargetArn": dlq_arn, "maxReceiveCount": "1"}
            ),
        },
    )["QueueUrl"]
    try:
        yield main_url, dlq_url
    finally:
        sqs.delete_queue(QueueUrl=main_url)
        sqs.delete_queue(QueueUrl=dlq_url)


def test_sqs_redrives_failing_message_to_dlq(sqs, main_and_dlq):
    main_url, dlq_url = main_and_dlq
    sqs.send_message(
        QueueUrl=main_url,
        MessageBody=json.dumps({"type": "task", "task_id": "redrive-1"}),
    )

    # Simulate a consumer that keeps failing: receive without deleting, past
    # maxReceiveCount=1. SQS then moves the message to the DLQ on its own — no
    # fastsqs / application code involved.
    moved = []
    deadline = time.time() + 60
    while time.time() < deadline and not moved:
        sqs.receive_message(QueueUrl=main_url, MaxNumberOfMessages=1, WaitTimeSeconds=1)
        got = sqs.receive_message(
            QueueUrl=dlq_url, MaxNumberOfMessages=1, WaitTimeSeconds=1
        )
        for m in got.get("Messages", []):
            moved.append(json.loads(m["Body"]))
        time.sleep(1)  # let visibility expire so the next receive triggers redrive

    assert moved, "SQS did not redrive the failing message to the DLQ"
    assert moved[0]["task_id"] == "redrive-1"
