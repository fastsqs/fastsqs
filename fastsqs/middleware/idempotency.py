"""Idempotency middleware: at-least-once dedup behind a structural Protocol port.

SQS standard queues deliver at-least-once, and producer retries can duplicate
messages even on FIFO. Consumers dedup on an APPLICATION-level key (an event
id in the payload — the transport ``messageId`` changes when a producer
retries), with three-state acquire semantics:

- ``ACQUIRED``    — first sighting: process it.
- ``COMPLETED``   — a finished duplicate: :class:`~fastsqs.SkipMessage` (ack,
  never redeliver).
- ``IN_PROGRESS`` — a concurrent duplicate: fail the record
  (:class:`~fastsqs.IdempotencyInProgressError`) so SQS redelivers AFTER the
  in-flight attempt settles — skipping here could lose the message if that
  attempt fails.

The IN_PROGRESS claim is a lease (``in_progress_ttl_seconds``), so a worker
that dies without cleanup (e.g. a Lambda timeout) never locks its key forever.
"""

from __future__ import annotations

import time
from enum import Enum
from typing import Any, Callable, Dict, Protocol, Tuple, runtime_checkable

from ..exceptions import (
    IdempotencyInProgressError,
    InvalidMessageError,
    SkipMessage,
)
from ..utils import resolve_payload_path
from .base import Middleware

_ACQUIRED_STATE_KEY = "_idempotency_acquired"


class AcquireResult(Enum):
    """Outcome of :meth:`IdempotencyStore.try_acquire`."""

    ACQUIRED = "acquired"
    IN_PROGRESS = "in_progress"
    COMPLETED = "completed"


@runtime_checkable
class IdempotencyStore(Protocol):
    """Structural port for idempotency state.

    Any object with these three async methods satisfies it — no fastsqs base
    class or inheritance required (DynamoDB conditional puts, Redis ``SET NX``,
    Postgres upserts...). ``try_acquire`` MUST be atomic in the backing store:
    two concurrent callers with the same key must never both get ``ACQUIRED``.
    """

    async def try_acquire(self, key: str, ttl_seconds: int) -> AcquireResult:
        """Atomically claim ``key`` for ``ttl_seconds`` (an IN_PROGRESS lease).

        Returns ``ACQUIRED`` on a fresh claim (including one whose previous
        lease/window expired), else the live entry's state."""
        ...

    async def mark_complete(self, key: str, ttl_seconds: int) -> None:
        """Mark ``key`` processed; duplicates skip for ``ttl_seconds`` (the
        dedup window)."""
        ...

    async def forget(self, key: str) -> None:
        """Release ``key`` after a failed attempt so a redelivery can retry."""
        ...


class InMemoryIdempotencyStore:
    """Reference :class:`IdempotencyStore` for tests and single-process dev.

    State lives in the process — it does NOT survive restarts and is not
    shared across Lambda sandboxes; use a DynamoDB/Redis-backed store in
    production. Methods contain no ``await`` between check and write, so they
    are atomic under a single event loop.

    ``clock`` is injectable (monotonic seconds) so tests can control expiry.
    """

    def __init__(self, *, clock: Callable[[], float] = time.monotonic) -> None:
        self._clock = clock
        self._entries: Dict[str, Tuple[AcquireResult, float]] = {}

    async def try_acquire(self, key: str, ttl_seconds: int) -> AcquireResult:
        now = self._clock()
        entry = self._entries.get(key)
        if entry is not None:
            state, expires_at = entry
            if expires_at > now:
                return state
        self._entries[key] = (AcquireResult.IN_PROGRESS, now + ttl_seconds)
        return AcquireResult.ACQUIRED

    async def mark_complete(self, key: str, ttl_seconds: int) -> None:
        self._entries[key] = (AcquireResult.COMPLETED, self._clock() + ttl_seconds)

    async def forget(self, key: str) -> None:
        self._entries.pop(key, None)


class IdempotencyMiddleware(Middleware):
    """Dedup middleware over an :class:`IdempotencyStore`.

    The key is resolved from the payload via ``key_path`` (dot-paths traverse
    nested dicts, e.g. ``"metadata.eventId"``; default ``"id"``, matching
    CloudEvents) and exposed to handlers as ``ctx.state.idempotency_key``.

    On success (or a handler-raised :class:`~fastsqs.SkipMessage`, which acks)
    the key is marked completed for ``completed_ttl_seconds``; on failure the
    claim is released so the SQS redelivery retries.
    """

    def __init__(
        self,
        store: IdempotencyStore,
        *,
        key_path: str = "id",
        in_progress_ttl_seconds: int = 300,
        completed_ttl_seconds: int = 86400,
        require_key: bool = True,
    ) -> None:
        """Initialize idempotency middleware.

        Args:
            store: Idempotency state backend (anything satisfying
                :class:`IdempotencyStore`).
            key_path: Payload path of the dedup key (dot-path for nested).
            in_progress_ttl_seconds: IN_PROGRESS lease length; must exceed the
                worst-case handler runtime (e.g. the Lambda timeout).
            completed_ttl_seconds: Dedup window after completion.
            require_key: When True (default), a payload without the key fails
                the record (contract violation); when False it processes
                without dedup.
        """
        if not isinstance(store, IdempotencyStore):
            raise TypeError(
                "store must satisfy the IdempotencyStore protocol (async "
                "try_acquire/mark_complete/forget); got "
                f"{type(store).__name__}"
            )
        self.store = store
        self.key_path = key_path
        self.in_progress_ttl_seconds = in_progress_ttl_seconds
        self.completed_ttl_seconds = completed_ttl_seconds
        self.require_key = require_key

    async def before(self, payload: dict, record: dict, context: Any, ctx) -> None:
        key_value = resolve_payload_path(payload, self.key_path)
        if key_value is None:
            if self.require_key:
                raise InvalidMessageError(
                    f"idempotency key missing at payload path '{self.key_path}'"
                )
            return

        key = str(key_value)
        ctx.state["idempotency_key"] = key

        result = await self.store.try_acquire(key, self.in_progress_ttl_seconds)
        if result is AcquireResult.COMPLETED:
            raise SkipMessage(f"duplicate of completed message (key='{key}')")
        if result is AcquireResult.IN_PROGRESS:
            raise IdempotencyInProgressError(
                f"message with key '{key}' is already in flight; failing so "
                "SQS redelivers after the in-flight attempt settles"
            )
        ctx.state[_ACQUIRED_STATE_KEY] = True

    async def after(
        self, payload: dict, record: dict, context: Any, ctx, error
    ) -> None:
        if not ctx.state.get(_ACQUIRED_STATE_KEY):
            return
        key = ctx.state["idempotency_key"]
        if error is None or isinstance(error, SkipMessage):
            await self.store.mark_complete(key, self.completed_ttl_seconds)
        else:
            await self.store.forget(key)
