"""Handler deployed to a real Lambda by the Tier 2 e2e harness (conftest.py).

Exercises the v1 feature surface in the real Lambda runtime so the e2e tests can
observe it via the DLQ:

- ``Task`` events: "boom*" fails (-> DLQ), "sleep-<n>" sleeps (Lambda timeout).
- Dependency injection: a ``Depends`` marker is injected; "di-check" FAILS unless
  the dependency resolved.
- Typed ``Context`` + ``QueueType.AUTO`` ARN inference: "ctx-std-check" /
  "ctx-fifo-check" FAIL unless the resolved queue type and ``ctx.fifo_info`` match
  the real event-source ARN.
- ``Order`` events: a second routed type, "boom*" fails. Unknown types have no
  route and no default handler, so they fail (RouteNotFoundError).

A message that PASSES its check succeeds and is deleted; a message that FAILS is
redelivered and eventually dead-lettered. Tests pair each "should-succeed"
message with a known-poison control so success is observable as absence-from-DLQ
once the control arrives.
"""

import asyncio

from fastsqs import Context, Depends, FastSQS, QueueType, SQSEvent


class Task(SQSEvent):
    task_id: str


class Order(SQSEvent):
    order_id: str


def get_marker() -> str:
    return "INJECTED"


app = FastSQS()  # QueueType.AUTO infers FIFO from the .fifo event-source ARN


@app.route(Task)
async def handle(msg: Task, ctx: Context, marker: str = Depends(get_marker)):
    if msg.task_id.startswith("sleep-"):
        await asyncio.sleep(int(msg.task_id.split("-", 1)[1]))

    if msg.task_id == "di-check" and marker != "INJECTED":
        raise ValueError("DI did not resolve in the real runtime")

    if msg.task_id == "ctx-std-check":
        if ctx.queue_type != QueueType.STANDARD or ctx.fifo_info is not None:
            raise ValueError(f"expected STANDARD ctx, got {ctx.queue_type}/{ctx.fifo_info}")

    if msg.task_id == "ctx-fifo-check":
        if ctx.queue_type != QueueType.FIFO or not (ctx.fifo_info and ctx.fifo_info.message_group_id):
            raise ValueError(f"expected FIFO ctx, got {ctx.queue_type}/{ctx.fifo_info}")

    if msg.task_id.startswith("boom"):
        raise ValueError(f"boom {msg.task_id}")
    return {"ok": msg.task_id}


@app.route(Order)
async def handle_order(msg: Order):
    if msg.order_id.startswith("boom"):
        raise ValueError(f"boom order {msg.order_id}")
    return {"ok": msg.order_id}


def lambda_handler(event, context):
    return app.handler(event, context)
