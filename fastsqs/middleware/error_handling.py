"""Error handling middleware with retry logic and circuit breaker."""

from __future__ import annotations

import time
import asyncio
from typing import Any, Awaitable, Dict, List, Optional, Callable, Union
from .base import Middleware
from ..utils import maybe_await


class RetryConfig:
    """Configuration for retry behavior.
    
    Defines retry policies including backoff strategies,
    maximum attempts, and exception filtering.
    """
    
    def __init__(
        self,
        max_retries: int = 3,
        base_delay: float = 1.0,
        max_delay: float = 60.0,
        exponential_backoff: bool = True,
        jitter: bool = True,
        retry_exceptions: Optional[List[type]] = None
    ):
        """Initialize retry configuration.
        
        Args:
            max_retries: Maximum number of retry attempts
            base_delay: Base delay between retries in seconds
            max_delay: Maximum delay between retries in seconds
            exponential_backoff: Whether to use exponential backoff
            jitter: Whether to add random jitter to delays
            retry_exceptions: List of exception types to retry
        """
        self.max_retries = max_retries
        self.base_delay = base_delay
        self.max_delay = max_delay
        self.exponential_backoff = exponential_backoff
        self.jitter = jitter
        self.retry_exceptions = retry_exceptions or [Exception]
    
    def should_retry(self, exception: Exception, attempt: int) -> bool:
        """Determine if an exception should be retried.
        
        Args:
            exception: Exception that occurred
            attempt: Current attempt number
            
        Returns:
            True if should retry, False otherwise
        """
        if attempt >= self.max_retries:
            return False
        
        return any(isinstance(exception, exc_type) for exc_type in self.retry_exceptions)
    
    def get_delay(self, attempt: int) -> float:
        """Calculate delay for next retry attempt.
        
        Args:
            attempt: Current attempt number
            
        Returns:
            Delay in seconds
        """
        if self.exponential_backoff:
            delay = self.base_delay * (2 ** attempt)
        else:
            delay = self.base_delay
        
        delay = min(delay, self.max_delay)
        
        if self.jitter:
            import random
            delay *= (0.5 + random.random() * 0.5)
        
        return delay


class CircuitBreakerState:
    """Circuit breaker state constants."""
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


class CircuitBreaker:
    """Circuit breaker for preventing cascading failures.
    
    Tracks failures and opens circuit when threshold is exceeded,
    preventing further requests until recovery timeout.
    """
    
    def __init__(
        self,
        failure_threshold: int = 5,
        recovery_timeout: float = 60.0,
        expected_exception: type = Exception
    ):
        """Initialize circuit breaker.
        
        Args:
            failure_threshold: Number of failures before opening circuit
            recovery_timeout: Seconds to wait before attempting recovery
            expected_exception: Exception type that counts as failure
        """
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.expected_exception = expected_exception
        
        self.failure_count = 0
        self.last_failure_time: Optional[float] = None
        self.state = CircuitBreakerState.CLOSED
    
    def should_allow_request(self) -> bool:
        if self.state == CircuitBreakerState.CLOSED:
            return True
        
        if self.state == CircuitBreakerState.OPEN:
            if self.last_failure_time and time.time() - self.last_failure_time >= self.recovery_timeout:
                self.state = CircuitBreakerState.HALF_OPEN
                return True
            return False
        
        return True
    
    def record_success(self) -> None:
        self.failure_count = 0
        self.state = CircuitBreakerState.CLOSED
    
    def record_failure(self, exception: Exception) -> None:
        if isinstance(exception, self.expected_exception):
            self.failure_count += 1
            self.last_failure_time = time.time()
            
            if self.failure_count >= self.failure_threshold:
                self.state = CircuitBreakerState.OPEN


class ErrorHandlingMiddleware(Middleware):
    """Middleware for comprehensive error handling with retries and circuit breaking.
    
    Provides retry logic, circuit breaker protection, error classification,
    and dead letter queue handling for failed messages.
    """
    
    def __init__(
        self,
        retry_config: Optional[RetryConfig] = None,
        circuit_breaker: Optional[CircuitBreaker] = None,
        dead_letter_handler: Optional[Callable[..., Union[None, Awaitable[None]]]] = None,
        error_classifier: Optional[Callable[[Exception], str]] = None
    ):
        """Initialize error handling middleware.
        
        Args:
            retry_config: Retry configuration
            circuit_breaker: Circuit breaker instance
            dead_letter_handler: Handler for messages that exhaust retries
            error_classifier: Function to classify errors as permanent/temporary
        """
        super().__init__()
        self.retry_config = retry_config or RetryConfig()
        self.circuit_breaker = circuit_breaker
        self.dead_letter_handler = dead_letter_handler
        self.error_classifier = error_classifier or self._default_error_classifier
    
    def _default_error_classifier(self, exception: Exception) -> str:
        error_type = type(exception).__name__
        
        permanent_errors = {
            "ValidationError", "InvalidMessage", "TypeError", 
            "ValueError", "KeyError", "AttributeError"
        }
        
        if error_type in permanent_errors:
            return "permanent"
        
        temporary_errors = {
            "ConnectionError", "TimeoutError", "HTTPError",
            "ServiceUnavailableError", "ThrottlingError"
        }
        
        if error_type in temporary_errors:
            return "temporary"
        
        return "temporary"
    
    async def before(self, payload: dict, record: dict, context: Any, ctx: dict) -> None:
        msg_id = record.get("messageId", "UNKNOWN")
        
        # Enhanced logging for circuit breaker state
        if self.circuit_breaker:
            self._log("debug", f"Circuit breaker check", 
                     msg_id=msg_id, state=self.circuit_breaker.state,
                     failure_count=self.circuit_breaker.failure_count,
                     last_failure_time=self.circuit_breaker.last_failure_time)
            
            if not self.circuit_breaker.should_allow_request():
                self._log("warning", f"Circuit breaker OPEN, rejecting request", msg_id=msg_id)
                raise CircuitBreakerOpenError("Circuit breaker is open, rejecting request")
            else:
                self._log("debug", f"Circuit breaker allows request", msg_id=msg_id)
        
        ctx["retry_attempt"] = 0
        ctx["error_history"] = []
        
        self._log("debug", f"Initialized error handling context", msg_id=msg_id)
    
    async def after(self, payload: dict, record: dict, context: Any, ctx: dict, error: Optional[Exception]) -> None:
        msg_id = record.get("messageId", "UNKNOWN")
        
        if error is None:
            self._log("info", f"Processing succeeded", msg_id=msg_id)
            if self.circuit_breaker:
                self.circuit_breaker.record_success()
                self._log("debug", f"Circuit breaker success recorded", msg_id=msg_id)
        else:
            self._log("error", f"Processing failed with error", 
                     msg_id=msg_id, error_type=type(error).__name__, error=str(error))
            await self._handle_error(payload, record, context, ctx, error)
    
    async def _handle_error(self, payload: dict, record: dict, context: Any, ctx: dict, error: Exception) -> None:
        msg_id = record.get("messageId", "UNKNOWN")
        error_type = self.error_classifier(error)
        
        self._log("debug", f"Error classification", msg_id=msg_id, error_type=error_type)
        
        ctx["error_history"].append({
            "attempt": ctx["retry_attempt"],
            "error": str(error),
            "error_type": error_type,
            "timestamp": time.time()
        })
        
        self._log("debug", f"Error history updated", 
                 msg_id=msg_id, total_attempts=len(ctx['error_history']))
        
        if self.circuit_breaker:
            self.circuit_breaker.record_failure(error)
            self._log("debug", f"Circuit breaker failure recorded", 
                     msg_id=msg_id, new_failure_count=self.circuit_breaker.failure_count)
        
        if error_type == "temporary" and self.retry_config.should_retry(error, ctx["retry_attempt"]):
            ctx["should_retry"] = True
            ctx["retry_delay"] = self.retry_config.get_delay(ctx["retry_attempt"])
            self._log("info", f"Will retry", msg_id=msg_id, delay=ctx['retry_delay'])
        else:
            self._log("info", f"Will not retry", 
                     msg_id=msg_id, error_type=error_type,
                     max_retries=self.retry_config.max_retries, 
                     current_attempt=ctx['retry_attempt'])
            
            if self.dead_letter_handler:
                try:
                    self._log("info", f"Calling dead letter handler", msg_id=msg_id)
                    await maybe_await(self.dead_letter_handler(payload, record, error))
                    self._log("info", f"Dead letter handler completed", msg_id=msg_id)
                except Exception as dlq_error:
                    self._log("error", f"Dead letter handler failed", 
                             msg_id=msg_id, dlq_error=str(dlq_error))
            
            ctx["should_retry"] = False


class DeadLetterQueueMiddleware(Middleware):
    """Middleware for handling messages that cannot be processed.
    
    Routes failed messages to dead letter queue with timeout monitoring
    and context preservation for debugging.
    """
    
    def __init__(
        self,
        dlq_handler: Optional[Callable[..., Union[None, Awaitable[None]]]] = None,
        max_processing_time: Optional[float] = None,
        include_context: bool = True
    ):
        """Initialize dead letter queue middleware.
        
        Args:
            dlq_handler: Handler function for dead letter messages
            max_processing_time: Maximum processing time before timeout
            include_context: Whether to include processing context in DLQ records
        """
        super().__init__()
        self.dlq_handler = dlq_handler or self._default_dlq_handler
        self.max_processing_time = max_processing_time
        self.include_context = include_context
    
    async def _default_dlq_handler(self, payload: dict, record: dict, error: Exception, ctx: dict) -> None:
        import json
        
        msg_id = record.get("messageId", "UNKNOWN")
        self._log("info", f"Creating dead letter queue record", msg_id=msg_id)
        
        dlq_record = {
            "timestamp": int(time.time()),
            "message_id": msg_id,
            "original_payload": payload,
            "error": str(error),
            "error_type": type(error).__name__,
            "processing_attempts": ctx.get("retry_attempt", 0) + 1
        }
        
        if self.include_context:
            dlq_record["context"] = {
                "error_history": ctx.get("error_history", []),
                "processing_time": ctx.get("duration_ms"),
                "queue_type": ctx.get("queueType")
            }
        
        self._log("info", f"Message sent to dead letter queue", 
                 msg_id=msg_id, dlq_record=dlq_record)
    
    async def before(self, payload: dict, record: dict, context: Any, ctx: dict) -> None:
        msg_id = record.get("messageId", "UNKNOWN")
        self._log("debug", f"Starting DLQ monitoring", msg_id=msg_id)
        
        if self.max_processing_time:
            ctx["dlq_start_time"] = time.time()
            self._log("debug", f"Processing timeout set", 
                     msg_id=msg_id, timeout=self.max_processing_time)

    async def after(self, payload: dict, record: dict, context: Any, ctx: dict, error: Optional[Exception]) -> None:
        msg_id = record.get("messageId", "UNKNOWN")
        
        if self.max_processing_time:
            processing_time = time.time() - ctx.get("dlq_start_time", 0)
            self._log("debug", f"Processing time", 
                     msg_id=msg_id, processing_time=processing_time)
            
            if processing_time > self.max_processing_time:
                self._log("error", f"Processing timeout exceeded!", 
                         msg_id=msg_id, processing_time=processing_time, 
                         max_time=self.max_processing_time)
                timeout_error = ProcessingTimeoutError(f"Processing exceeded {self.max_processing_time}s")
                await maybe_await(self.dlq_handler(payload, record, timeout_error, ctx))
                return
        
        if error and not ctx.get("should_retry", False):
            self._log("info", f"Sending to DLQ due to error", 
                     msg_id=msg_id, error_type=type(error).__name__)
            await maybe_await(self.dlq_handler(payload, record, error, ctx))
        elif error:
            self._log("info", f"Error occurred but will retry", 
                     msg_id=msg_id, error_type=type(error).__name__)
        else:
            self._log("debug", f"Processing completed successfully", msg_id=msg_id)


class CircuitBreakerOpenError(Exception):
    """Exception raised when circuit breaker is open and rejecting requests."""
    pass


class ProcessingTimeoutError(Exception):
    """Exception raised when message processing exceeds timeout limit."""
    pass
