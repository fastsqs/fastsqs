"""Typed Context (#6): ctx.message_id alongside ctx["messageId"], backward-compat."""

from fastsqs import FastSQS, SQSEvent, Context, Depends
from fastsqs.middleware import Middleware
from fastsqs.testing import SQSTestClient


class Task(SQSEvent):
    task_id: str


def test_typed_attribute_and_dict_access_agree():
    seen = {}
    app = FastSQS()

    @app.route(Task)
    async def handle(msg: Task, ctx: Context):
        seen["typed"] = ctx.message_id          # typed
        seen["dict"] = ctx["messageId"]         # dict — backward compatible
        seen["qtype"] = ctx.queue_type
        seen["mtype"] = ctx.message_type
        seen["route"] = ctx.route_path

    SQSTestClient(app).send({"type": "task", "task_id": "1"}, message_id="abc")
    assert seen["typed"] == "abc" == seen["dict"]
    assert seen["qtype"] == "standard"
    assert seen["mtype"] == "task"
    assert isinstance(seen["route"], list)   # typed list[str]


def test_middleware_dict_writes_still_work():
    captured = {}
    app = FastSQS()

    class MW(Middleware):
        async def before(self, payload, record, context, ctx):
            ctx["custom"] = "X"                 # dict write
            ctx.setdefault("acc", []).append(1)  # setdefault

        async def after(self, payload, record, context, ctx, error):
            captured["custom"] = ctx["custom"]
            captured["acc"] = ctx["acc"]

    app.add_middleware(MW())

    @app.route(Task)
    async def handle(msg: Task, ctx: Context):
        pass

    SQSTestClient(app).send({"type": "task", "task_id": "1"})
    assert captured == {"custom": "X", "acc": [1]}


def test_typed_context_with_di_together():
    def get_svc():
        return "SVC"

    out = {}
    app = FastSQS()

    @app.route(Task)
    async def handle(msg: Task, ctx: Context, svc=Depends(get_svc)):
        out["mid"] = ctx.message_id
        out["svc"] = svc

    SQSTestClient(app).send({"type": "task", "task_id": "1"}, message_id="m7")
    assert out == {"mid": "m7", "svc": "SVC"}


def test_context_is_a_dict():
    ctx = Context({"messageId": "z"})
    assert isinstance(ctx, dict)
    assert ctx.message_id == "z" == ctx["messageId"]
    assert ctx.handler_result is None        # missing key -> typed default
