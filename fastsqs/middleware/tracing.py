"""W3C Trace Context propagation for SQS records.

Producer instrumentation propagates ``traceparent``/``tracestate`` through SQS
as message attributes (transport-level) or as CloudEvents extension attributes
at the payload top level. This middleware parses either into a typed
:class:`TraceContext` at ``ctx.state.trace`` — hand it to whatever tracer you
use (e.g. OpenTelemetry's ``TraceContextTextMapPropagator``); fastsqs takes no
tracing dependency itself. Malformed values are ignored: tracing must never
fail message processing.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any, Optional

from .base import Middleware

_TRACEPARENT_RE = re.compile(
    r"^([0-9a-f]{2})-([0-9a-f]{32})-([0-9a-f]{16})-([0-9a-f]{2})$"
)


@dataclass(frozen=True)
class TraceContext:
    """Parsed W3C ``traceparent`` (plus the opaque ``tracestate``, verbatim)."""

    traceparent: str
    trace_id: str
    parent_id: str
    sampled: bool
    tracestate: Optional[str] = None


def parse_traceparent(
    value: Any, tracestate: Optional[str] = None
) -> Optional[TraceContext]:
    """Parse a W3C ``traceparent`` string; ``None`` for anything invalid.

    Per spec: version ``ff`` and all-zero trace/parent ids are invalid.
    """
    if not isinstance(value, str):
        return None
    match = _TRACEPARENT_RE.match(value)
    if match is None:
        return None
    version, trace_id, parent_id, flags = match.groups()
    if version == "ff" or set(trace_id) == {"0"} or set(parent_id) == {"0"}:
        return None
    return TraceContext(
        traceparent=value,
        trace_id=trace_id,
        parent_id=parent_id,
        sampled=bool(int(flags, 16) & 0x01),
        tracestate=tracestate,
    )


def _attribute_value(record: dict, name: str) -> Optional[str]:
    """Read an SQS message attribute's stringValue, case-insensitive on the
    attribute name (header names are case-insensitive in W3C Trace Context)."""
    attributes = record.get("messageAttributes")
    if not isinstance(attributes, dict):
        return None
    for key, attribute in attributes.items():
        if key.lower() == name and isinstance(attribute, dict):
            value = attribute.get("stringValue")
            if isinstance(value, str):
                return value
    return None


def _payload_value(payload: dict, name: str) -> Optional[str]:
    value = payload.get(name)
    return value if isinstance(value, str) else None


class TracingMiddleware(Middleware):
    """Expose the record's W3C trace context as ``ctx.state.trace``.

    Sources, in precedence order (message attributes are the transport-level
    channel, so they win): SQS message attributes, then top-level payload keys
    (the CloudEvents extension-attribute convention). When absent or invalid,
    ``ctx.state`` is left untouched (``ctx.state.get("trace")`` -> ``None``).
    """

    def __init__(self, state_key: str = "trace"):
        """Initialize tracing middleware.

        Args:
            state_key: ``ctx.state`` key to store the :class:`TraceContext` under.
        """
        self.state_key = state_key

    async def before(self, payload: dict, record: dict, context: Any, ctx) -> None:
        traceparent = _attribute_value(record, "traceparent") or _payload_value(
            payload, "traceparent"
        )
        tracestate = _attribute_value(record, "tracestate") or _payload_value(
            payload, "tracestate"
        )
        trace = parse_traceparent(traceparent, tracestate)
        if trace is not None:
            ctx.state[self.state_key] = trace
