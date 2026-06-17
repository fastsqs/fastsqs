"""Predefined middleware presets for common use cases."""

from __future__ import annotations

from typing import List

from .middleware import Middleware, LoggingMiddleware, TimingMsMiddleware


class MiddlewarePreset:
    """Factory for predefined middleware stacks.

    fastsqs ships only cross-cutting middleware that is genuinely the router's
    job (logging, timing). Concurrency is configured on ``FastSQS``
    (``max_concurrent_messages``); dead-letter handling is the SQS queue's job
    (redrive policy) — fastsqs reports partial batch failures and lets SQS
    redeliver/redrive.
    """

    @staticmethod
    def production() -> List[Middleware]:
        """Structured logging (with context) + timing."""
        return [
            LoggingMiddleware(verbose=True, include_context=True, include_record=False),
            TimingMsMiddleware(),
        ]

    @staticmethod
    def development() -> List[Middleware]:
        """Verbose logging (with record) + timing."""
        return [
            LoggingMiddleware(verbose=True, include_context=True, include_record=True),
            TimingMsMiddleware(),
        ]

    @staticmethod
    def minimal() -> List[Middleware]:
        """Logging + timing only."""
        return [
            LoggingMiddleware(),
            TimingMsMiddleware(),
        ]
