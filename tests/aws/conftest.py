"""Tier 2 real-AWS e2e harness (opt-in: ``pytest --run-aws``).

Session-scoped: builds ONE Lambda deployment zip (fastsqs + the e2e handler +
linux-x86_64 pydantic/fast-depends) and an IAM role, then deploys Lambda
functions on demand via ``lambda_factory`` — one per distinct env-var config (so
strict / halt_batch / corrupt / low-concurrency variants reuse the same zip). A
per-test ``pipeline`` factory wires a main SQS queue + DLQ (redrive policy) + an
event-source mapping with ReportBatchItemFailures, optionally a results queue for
the handler's echo side channel. ``drain`` returns raw bodies; ``drain_full``
returns full message dicts (system + message attributes). Everything is torn
down. Uses the ``gabe`` profile.
"""

import io
import json
import shutil
import subprocess
import sys
import time
import uuid
import zipfile
from pathlib import Path

import boto3
import pytest

REGION = "us-east-1"
PROFILE = "gabe"
REPO = Path(__file__).resolve().parents[2]


@pytest.fixture(scope="session")
def aws():
    s = boto3.Session(profile_name=PROFILE, region_name=REGION)
    return {"iam": s.client("iam"), "lambda": s.client("lambda"), "sqs": s.client("sqs")}


def _build_zip(tmp: Path) -> bytes:
    build = tmp / "build"
    build.mkdir(parents=True, exist_ok=True)
    subprocess.run(
        [
            sys.executable, "-m", "pip", "install", "--target", str(build),
            "--platform", "manylinux2014_x86_64", "--python-version", "3.13",
            "--only-binary=:all:", "pydantic>=2", "fast-depends>=3", "-q",
        ],
        check=True,
    )
    shutil.copytree(REPO / "fastsqs", build / "fastsqs")
    shutil.copy(REPO / "tests" / "aws" / "_e2e_handler.py", build / "lambda_function.py")
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
        for p in build.rglob("*"):
            if p.is_file() and "__pycache__" not in str(p):
                z.write(p, p.relative_to(build))
    return buf.getvalue()


@pytest.fixture(scope="session")
def _exec_role(aws):
    iam = aws["iam"]
    role_name = f"fastsqs-e2e-role-{uuid.uuid4().hex[:8]}"
    trust = {"Version": "2012-10-17", "Statement": [
        {"Effect": "Allow", "Principal": {"Service": "lambda.amazonaws.com"}, "Action": "sts:AssumeRole"}]}
    perms = {"Version": "2012-10-17", "Statement": [
        {"Effect": "Allow", "Action": ["logs:CreateLogGroup", "logs:CreateLogStream", "logs:PutLogEvents"], "Resource": "*"},
        {"Effect": "Allow", "Action": [
            "sqs:ReceiveMessage", "sqs:DeleteMessage", "sqs:GetQueueAttributes",
            "sqs:ChangeMessageVisibility", "sqs:SendMessage"], "Resource": "*"}]}
    role_arn = iam.create_role(RoleName=role_name, AssumeRolePolicyDocument=json.dumps(trust))["Role"]["Arn"]
    iam.put_role_policy(RoleName=role_name, PolicyName="perms", PolicyDocument=json.dumps(perms))
    yield role_arn
    try:
        iam.delete_role_policy(RoleName=role_name, PolicyName="perms")
        iam.delete_role(RoleName=role_name)
    except Exception:
        pass


@pytest.fixture(scope="session")
def _zip_bytes(tmp_path_factory):
    return _build_zip(tmp_path_factory.mktemp("lambda"))


@pytest.fixture(scope="session")
def lambda_factory(aws, _exec_role, _zip_bytes):
    """make_fn(env: dict | None) -> function name. Deploys (once, cached per env)
    a Lambda from the shared zip with the given env vars; tears all down."""
    lam = aws["lambda"]
    created: dict = {}

    def make_fn(env: dict | None = None):
        key = tuple(sorted((env or {}).items()))
        if key in created:
            return created[key]
        fn_name = f"fastsqs-e2e-{uuid.uuid4().hex[:8]}"
        for _ in range(15):  # IAM role propagation
            try:
                lam.create_function(
                    FunctionName=fn_name, Runtime="python3.13", Architectures=["x86_64"],
                    Handler="lambda_function.lambda_handler", Role=_exec_role,
                    Code={"ZipFile": _zip_bytes}, Timeout=10, MemorySize=256,
                    Environment={"Variables": dict(env or {})})
                break
            except lam.exceptions.InvalidParameterValueException:
                time.sleep(3)
        else:
            raise RuntimeError("Lambda did not accept the role in time")
        lam.get_waiter("function_active_v2").wait(FunctionName=fn_name)
        created[key] = fn_name
        return fn_name

    yield make_fn
    for fn in created.values():
        try:
            lam.delete_function(FunctionName=fn)
        except Exception:
            pass


@pytest.fixture(scope="session")
def deployed_lambda(lambda_factory):
    """The default function (partial_batch_failure=True, isolate_groups)."""
    return lambda_factory({})


@pytest.fixture
def pipeline(aws, deployed_lambda):
    """Factory: main queue + DLQ + ESM bound to a Lambda.

    make(fifo=False, max_receive_count=2, visibility=10, *, fn=None, results=False,
         content_dedup=True, batch_size=10, batching_window=0, scaling=None,
         high_throughput=False, start_disabled=False)
      -> (main_url, dlq_url[, results_url][, enable]).
    ``results_url`` is appended when results=True; ``enable`` (a 0-arg callback
    that enables the ESM and waits) is appended when start_disabled=True — the
    deterministic co-batch lever: enqueue messages, then call enable() so the
    first poll grabs them as one batch.
    """
    sqs, lam = aws["sqs"], aws["lambda"]
    queues: list = []
    esms: list = []

    def make(fifo=False, max_receive_count=2, visibility=10, *, fn=None, results=False,
             content_dedup=True, batch_size=10, batching_window=0, scaling=None,
             high_throughput=False, start_disabled=False, filter_criteria=None):
        fn = fn or deployed_lambda
        sfx = uuid.uuid4().hex[:8]
        ext = ".fifo" if fifo else ""
        fifo_attrs: dict = {}
        if fifo:
            fifo_attrs["FifoQueue"] = "true"
            if content_dedup:
                fifo_attrs["ContentBasedDeduplication"] = "true"
            if high_throughput:
                fifo_attrs["FifoThroughputLimit"] = "perMessageGroupId"
                fifo_attrs["DeduplicationScope"] = "messageGroup"
        dlq_url = sqs.create_queue(QueueName=f"fastsqs-e2e-dlq-{sfx}{ext}", Attributes=dict(fifo_attrs))["QueueUrl"]
        dlq_arn = sqs.get_queue_attributes(QueueUrl=dlq_url, AttributeNames=["QueueArn"])["Attributes"]["QueueArn"]
        main_url = sqs.create_queue(
            QueueName=f"fastsqs-e2e-main-{sfx}{ext}",
            Attributes={**fifo_attrs, "VisibilityTimeout": str(visibility),
                        "RedrivePolicy": json.dumps({"deadLetterTargetArn": dlq_arn, "maxReceiveCount": str(max_receive_count)})},
        )["QueueUrl"]
        main_arn = sqs.get_queue_attributes(QueueUrl=main_url, AttributeNames=["QueueArn"])["Attributes"]["QueueArn"]
        # Track queues for teardown BEFORE creating the ESM, so a failed
        # create_event_source_mapping cannot leak them.
        queues.extend([main_url, dlq_url])
        results_url = None
        if results:
            results_url = sqs.create_queue(QueueName=f"fastsqs-e2e-results-{sfx}")["QueueUrl"]
            queues.append(results_url)

        if fifo:
            batching_window = 0  # FIFO ESMs reject MaximumBatchingWindowInSeconds
        esm_kwargs = dict(
            EventSourceArn=main_arn, FunctionName=fn, Enabled=not start_disabled,
            BatchSize=batch_size, FunctionResponseTypes=["ReportBatchItemFailures"])
        if batching_window:
            esm_kwargs["MaximumBatchingWindowInSeconds"] = batching_window
        if scaling:
            esm_kwargs["ScalingConfig"] = {"MaximumConcurrency": scaling}
        if filter_criteria:
            esm_kwargs["FilterCriteria"] = filter_criteria
        esm = lam.create_event_source_mapping(**esm_kwargs)["UUID"]
        esms.append(esm)

        def _wait_enabled():
            for _ in range(30):
                if lam.get_event_source_mapping(UUID=esm)["State"] == "Enabled":
                    return
                time.sleep(2)

        def enable():
            """Enable the initially-disabled ESM; the first poll co-batches every
            message already enqueued (deterministic co-batching)."""
            lam.update_event_source_mapping(UUID=esm, Enabled=True)
            _wait_enabled()

        if not start_disabled:
            _wait_enabled()

        parts: list = [main_url, dlq_url]
        if results:
            parts.append(results_url)
        if start_disabled:
            parts.append(enable)
        return tuple(parts)

    yield make

    for esm in esms:
        try:
            lam.delete_event_source_mapping(UUID=esm)
        except Exception:
            pass
    time.sleep(5)  # ESM deletion is async; let it detach before deleting queues
    for url in queues:
        for _ in range(4):
            try:
                sqs.delete_queue(QueueUrl=url)
                break
            except Exception:
                time.sleep(3)


# Express enrichment: a Map over the batch array that injects service="enriched"
# into each record's body via $merge, PRESERVING the SQS envelope (messageId,
# attributes, eventSourceARN) — exactly what a faithful Pipes enrichment must do so
# partial-batch-failure (keyed on messageId) survives the enrichment stage.
_ENRICH_ASL = json.dumps(
    {
        "QueryLanguage": "JSONata",
        "Comment": "e2e: inject service into body via $merge, preserve the SQS envelope.",
        "StartAt": "EnrichBatch",
        "States": {
            "EnrichBatch": {
                "Type": "Map",
                "Items": "{% $states.input %}",
                "ItemProcessor": {
                    "ProcessorConfig": {"Mode": "INLINE"},
                    "StartAt": "Inject",
                    "States": {
                        "Inject": {
                            "Type": "Pass",
                            "Output": "{% $merge([$states.input, {'body': $string($merge([$parse($states.input.body), {'service': 'enriched'}]))}]) %}",
                            "End": True,
                        }
                    },
                },
                "End": True,
            }
        },
    }
)


@pytest.fixture
def pipe_pipeline(aws, deployed_lambda):
    """Factory: SQS source (+DLQ) -> SFN Express enrichment -> EventBridge Pipe -> Lambda.

    make(fifo=False, max_receive_count=2, visibility=120, *, fn=None, batch_size=10,
         batching_window=0, start_stopped=False) -> (source_url, dlq_url[, start]).

    The enrichment injects service="enriched" into each record body (preserving the
    envelope). When ``start_stopped=True`` the pipe is created STOPPED and a 0-arg
    ``start`` callback is appended — the deterministic co-batch lever: enqueue one
    SendMessageBatch, then start() so the first poll grabs the group as one execution.
    Tears down pipe -> state machine -> IAM roles -> queues.
    """
    session = boto3.Session(profile_name=PROFILE, region_name=REGION)
    sqs, iam = aws["sqs"], aws["iam"]
    lam = aws["lambda"]
    sfn = session.client("stepfunctions")
    pipes = session.client("pipes")

    queues: list = []
    roles: list = []  # (role_name, [policy_names])
    state_machines: list = []  # arns
    pipe_names: list = []

    def _role(name, principal, policy_doc=None):
        trust = {
            "Version": "2012-10-17",
            "Statement": [
                {"Effect": "Allow", "Principal": {"Service": principal}, "Action": "sts:AssumeRole"}
            ],
        }
        arn = iam.create_role(RoleName=name, AssumeRolePolicyDocument=json.dumps(trust))["Role"]["Arn"]
        pols = []
        if policy_doc:
            iam.put_role_policy(RoleName=name, PolicyName="perms", PolicyDocument=json.dumps(policy_doc))
            pols.append("perms")
        roles.append((name, pols))
        return arn

    def make(fifo=False, max_receive_count=2, visibility=120, *, fn=None,
             batch_size=10, batching_window=0, start_stopped=False):
        fn_name = fn or deployed_lambda
        fn_arn = lam.get_function(FunctionName=fn_name)["Configuration"]["FunctionArn"]
        sfx = uuid.uuid4().hex[:8]
        ext = ".fifo" if fifo else ""
        fifo_attrs = {"FifoQueue": "true", "ContentBasedDeduplication": "true"} if fifo else {}

        dlq_url = sqs.create_queue(
            QueueName=f"fastsqs-e2e-pdlq-{sfx}{ext}", Attributes=dict(fifo_attrs)
        )["QueueUrl"]
        dlq_arn = sqs.get_queue_attributes(QueueUrl=dlq_url, AttributeNames=["QueueArn"])["Attributes"]["QueueArn"]
        src_url = sqs.create_queue(
            QueueName=f"fastsqs-e2e-psrc-{sfx}{ext}",
            Attributes={
                **fifo_attrs,
                "VisibilityTimeout": str(visibility),
                "RedrivePolicy": json.dumps({"deadLetterTargetArn": dlq_arn, "maxReceiveCount": str(max_receive_count)}),
            },
        )["QueueUrl"]
        src_arn = sqs.get_queue_attributes(QueueUrl=src_url, AttributeNames=["QueueArn"])["Attributes"]["QueueArn"]
        # Track queues for teardown BEFORE the pipe is created (leak-proof).
        queues.extend([src_url, dlq_url])

        # Express enrichment SM (pure Pass/Map -> needs no service permissions).
        sm_role = _role(f"fastsqs-e2e-sfnrole-{sfx}", "states.amazonaws.com")
        sm_arn = None
        for _ in range(20):  # IAM role propagation
            try:
                sm_arn = sfn.create_state_machine(
                    name=f"fastsqs-e2e-enrich-{sfx}", definition=_ENRICH_ASL,
                    roleArn=sm_role, type="EXPRESS",
                )["stateMachineArn"]
                break
            except Exception:
                time.sleep(3)
        else:
            raise RuntimeError("SFN did not accept the role in time")
        state_machines.append(sm_arn)

        # Pipe role: consume source, start the enrichment (sync), invoke the target.
        pipe_role = _role(
            f"fastsqs-e2e-piperole-{sfx}", "pipes.amazonaws.com",
            {
                "Version": "2012-10-17",
                "Statement": [
                    {"Effect": "Allow", "Action": ["sqs:ReceiveMessage", "sqs:DeleteMessage", "sqs:GetQueueAttributes"], "Resource": src_arn},
                    {"Effect": "Allow", "Action": ["states:StartSyncExecution"], "Resource": sm_arn},
                    {"Effect": "Allow", "Action": ["lambda:InvokeFunction"], "Resource": fn_arn},
                ],
            },
        )

        sqs_params = {"BatchSize": batch_size}
        if batching_window and not fifo:
            sqs_params["MaximumBatchingWindowInSeconds"] = batching_window
        src_params = {"SqsQueueParameters": sqs_params}
        pipe_name = f"fastsqs-e2e-pipe-{sfx}"
        for _ in range(20):  # IAM role propagation
            try:
                pipes.create_pipe(
                    Name=pipe_name, RoleArn=pipe_role,
                    Source=src_arn, SourceParameters=src_params,
                    Enrichment=sm_arn,
                    Target=fn_arn,
                    TargetParameters={"LambdaFunctionParameters": {"InvocationType": "REQUEST_RESPONSE"}},
                    DesiredState="STOPPED" if start_stopped else "RUNNING",
                )
                break
            except Exception:
                time.sleep(3)
        else:
            raise RuntimeError("Pipe did not accept the roles in time")
        pipe_names.append(pipe_name)

        def _wait(target_state):
            cur = None
            for _ in range(50):
                cur = pipes.describe_pipe(Name=pipe_name)["CurrentState"]
                if cur == target_state:
                    return
                if cur.endswith("FAILED"):
                    reason = pipes.describe_pipe(Name=pipe_name).get("StateReason")
                    raise RuntimeError(f"pipe {pipe_name} -> {cur}: {reason}")
                time.sleep(3)
            raise RuntimeError(f"pipe {pipe_name} not {target_state} in time (last={cur})")

        def start():
            pipes.start_pipe(Name=pipe_name)
            _wait("RUNNING")

        _wait("STOPPED" if start_stopped else "RUNNING")

        parts = [src_url, dlq_url]
        if start_stopped:
            parts.append(start)
        return tuple(parts)

    yield make

    # Teardown order: pipe (references SM/queue/lambda) -> SM -> roles -> queues.
    for name in pipe_names:
        try:
            pipes.delete_pipe(Name=name)
        except Exception:
            pass
    for name in pipe_names:
        for _ in range(50):
            try:
                pipes.describe_pipe(Name=name)
                time.sleep(3)
            except Exception:
                break
    for arn in state_machines:
        try:
            sfn.delete_state_machine(stateMachineArn=arn)
        except Exception:
            pass
    for name, pols in roles:
        for p in pols:
            try:
                iam.delete_role_policy(RoleName=name, PolicyName=p)
            except Exception:
                pass
        try:
            iam.delete_role(RoleName=name)
        except Exception:
            pass
    time.sleep(3)
    for url in queues:
        for _ in range(4):
            try:
                sqs.delete_queue(QueueUrl=url)
                break
            except Exception:
                time.sleep(3)


def _drainer(sqs, with_attrs):
    def _drain(url, timeout=120, min_count=1, predicate=None):
        got = []
        deadline = time.time() + timeout
        kw = {"QueueUrl": url, "MaxNumberOfMessages": 10, "WaitTimeSeconds": 2}
        if with_attrs:
            kw["AttributeNames"] = ["All"]
            kw["MessageAttributeNames"] = ["All"]
        while time.time() < deadline and len(got) < min_count:
            r = sqs.receive_message(**kw)
            for m in r.get("Messages", []):
                if predicate is None or predicate(m):
                    got.append(m if with_attrs else m["Body"])
                sqs.delete_message(QueueUrl=url, ReceiptHandle=m["ReceiptHandle"])
            if len(got) < min_count:
                time.sleep(1)
        return got
    return _drain


@pytest.fixture
def drain(aws):
    """drain(url, timeout=120, min_count=1, predicate=None) -> [raw_body, ...]."""
    return _drainer(aws["sqs"], with_attrs=False)


@pytest.fixture
def drain_full(aws):
    """drain_full(url, timeout, min_count, predicate) -> [full message dict, ...]
    including system Attributes and MessageAttributes (delete-as-read)."""
    return _drainer(aws["sqs"], with_attrs=True)
