from __future__ import annotations

import inspect
from typing import Any, Awaitable, List, Optional


class Middleware:
    """Base class for FastSQS middleware.
    
    Middleware can hook into message processing before and after handler execution.
    """
    
    def __init__(self):
        """Initialize middleware."""
        self._app = None
    
    def _log(self, level: str, message: str, **data) -> None:
        """Log method that routes through the app's logging system.
        
        Args:
            level: Log level (info, debug, error, etc.)
            message: Log message
            **data: Additional log data
        """
        if self._app and hasattr(self._app, '_log'):
            self._app._log(level, message, **data)
    
    async def before(self, payload: dict, record: dict, context: Any, ctx: dict) -> None:
        """Hook called before handler execution.
        
        Args:
            payload: Message payload
            record: SQS record
            context: Lambda context
            ctx: Processing context
        """
        return None

    async def after(
        self, payload: dict, record: dict, context: Any, ctx: dict, error: Optional[Exception]
    ) -> None:
        """Hook called after handler execution.
        
        Args:
            payload: Message payload
            record: SQS record
            context: Lambda context
            ctx: Processing context
            error: Exception if handler failed, None otherwise
        """
        return None


def call_middleware_hook(mw: Middleware, hook: str, *args) -> Awaitable[None]:
    """Call a middleware hook method safely.
    
    Args:
        mw: Middleware instance
        hook: Hook method name ('before' or 'after')
        *args: Arguments to pass to hook
        
    Returns:
        Awaitable that resolves to None
    """
    fn = getattr(mw, hook, None)
    if fn is None:
        async def _noop():
            return None
        return _noop()
    res = fn(*args)
    if inspect.isawaitable(res):
        return res

    async def _wrap():
        return None

    return _wrap()


async def run_middlewares(
    mws: List[Middleware],
    when: str,
    payload: dict,
    record: dict,
    context: Any,
    ctx: dict,
    error: Optional[Exception] = None,
) -> None:
    """Run middleware chain for specified phase.
    
    Args:
        mws: List of middleware instances
        when: Phase ('before' or 'after')
        payload: Message payload
        record: SQS record
        context: Lambda context
        ctx: Processing context
        error: Exception if in 'after' phase with error
        
    Raises:
        ValueError: If 'when' parameter is invalid
    """
    if when == "before":
        for mw in mws:
            await call_middleware_hook(mw, "before", payload, record, context, ctx)
    elif when == "after":
        for mw in reversed(mws):
            try:
                await call_middleware_hook(mw, "after", payload, record, context, ctx, error)
            except Exception as hook_error:
                # An after-hook must never abort the other middlewares' cleanup
                # nor mask the original handler error (this chain runs inside
                # app.py's finally block).
                mw._log("error", "after middleware hook raised", error=str(hook_error))
    else:
        raise ValueError("when must be 'before' or 'after'")