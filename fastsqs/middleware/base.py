from __future__ import annotations

import inspect
from typing import Any, Awaitable, Callable, List, Optional


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


async def run_middleware_stack(
    mws: List[Middleware],
    payload: dict,
    record: dict,
    context: Any,
    ctx: dict,
    call_inner: Callable[[], Awaitable[Any]],
) -> Any:
    """Run the before -> inner -> after middleware stack with balanced cleanup.

    Only middlewares whose ``before`` completed are unwound (``after`` runs in
    reverse) — even if a later ``before`` or the inner call raises. This keeps
    enter/exit symmetric so resources acquired in ``before`` (e.g. a concurrency
    slot, a monitor task) are always released. After-hooks are isolated: one
    raising never aborts the others nor masks the original error, which is
    re-raised after cleanup.
    """
    entered: List[Middleware] = []
    err: Optional[Exception] = None
    try:
        for mw in mws:
            await call_middleware_hook(mw, "before", payload, record, context, ctx)
            entered.append(mw)
        return await call_inner()
    except Exception as e:
        err = e
        raise
    finally:
        for mw in reversed(entered):
            try:
                await call_middleware_hook(
                    mw, "after", payload, record, context, ctx, err
                )
            except Exception as hook_error:
                mw._log("error", "after middleware hook raised", error=str(hook_error))