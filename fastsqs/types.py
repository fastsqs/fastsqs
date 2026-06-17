"""Type definitions for FastSQS."""

from __future__ import annotations

from typing import Any, Awaitable, Callable, List, Optional, TypedDict, Union, TypeVar
from enum import Enum
from pydantic import BaseModel


class QueueType(Enum):
    """Enumeration for SQS queue types."""
    STANDARD = "standard"
    FIFO = "fifo"


Handler = Callable[..., Union[None, Awaitable[None], Any]]
"""Type alias for message handler functions."""

RouteValue = Union[str, int]
"""Type alias for route values."""

T = TypeVar('T', bound=BaseModel)
"""Type variable bound to Pydantic BaseModel."""


ProcessingContext = TypedDict(
    "ProcessingContext",
    {
        "messageId": str,
        "record": dict,
        "context": Any,
        "route_path": List[str],
        "queueType": str,
        "fifoInfo": dict,
        "message_type": str,
        "handler_result": Any,
        "error_history": List[Any],
        "dlq_start_time": float,
        "concurrency_stats": dict,
        "concurrency_wait_time": float,
        "visibility_timeout": float,
        "visibility_warning_time": float,
        "visibility_start_time": float,
        "visibility_warned": bool,
        "visibility_timeout_usage": float,
        "visibility_monitor_task": Any,
        "duration_ms": float,
        "processing_start_time": float,
        "processing_start_time_ns": int,
        "processing_duration_seconds": float,
        "processing_duration_ms": float,
        "processing_metrics": dict,
        "metrics_start_time": float,
        "_parallelization_middleware": Any,
    },
    total=False,
)
"""Per-message processing context shared across middleware + handlers.
All keys optional (total=False) — documents the contract, not enforced."""


class Context(dict):
    """Per-record processing context passed to handlers and middleware.

    It IS a ``dict`` — ``ctx["messageId"]`` reads, ``ctx.get(...)``,
    ``ctx.setdefault(...)`` and middleware writes all keep working unchanged —
    plus typed attribute access for the common fields, so handlers can write
    ``ctx.message_id`` (IDE-checkable, ``str``) instead of ``ctx["messageId"]``
    (``Any``, typo-prone). Dynamic / middleware-internal keys stay reachable via
    item access. Annotate a handler param ``ctx: Context`` to get the typing.
    """

    @property
    def message_id(self) -> str:
        return self.get("messageId", "")

    @property
    def message_type(self) -> Optional[str]:
        return self.get("message_type")

    @property
    def queue_type(self) -> str:
        return self.get("queueType", "")

    @property
    def record(self) -> dict:
        return self.get("record", {})

    @property
    def lambda_context(self) -> Any:
        return self.get("context")

    @property
    def route_path(self) -> List[str]:
        return self.get("route_path", [])

    @property
    def handler_result(self) -> Any:
        return self.get("handler_result")

    @property
    def fifo_info(self) -> Optional[dict]:
        return self.get("fifoInfo")

    @property
    def error_history(self) -> List[Any]:
        return self.get("error_history", [])
