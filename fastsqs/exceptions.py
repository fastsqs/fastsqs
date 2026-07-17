"""Exception hierarchy for FastSQS.

All fastsqs exceptions derive from :class:`FastSQSError`, so callers can catch
any framework error with a single ``except FastSQSError``.
"""

from typing import List, Optional


class FastSQSError(Exception):
    """Base class for all FastSQS errors."""


class SkipMessage(Exception):
    """Control-flow signal: ack the current record as SUCCESS without (further)
    processing.

    Raise from a middleware ``before`` or from a handler to mark the record
    done on purpose — e.g. an idempotency middleware seeing an
    already-completed duplicate. The record is NOT reported as a batch item
    failure, so SQS deletes it instead of redelivering.

    Deliberately NOT a :class:`FastSQSError`: it is not an error, and a user's
    blanket ``except FastSQSError`` error handling must never swallow an ack.
    """


class RouteNotFoundError(FastSQSError):
    """Raised when no route handler matches a message and no default handler is set."""


class InvalidMessageError(FastSQSError):
    """Raised when a message body has an invalid format or content."""


class IdempotencyInProgressError(FastSQSError):
    """Raised for a duplicate whose first copy is still in flight.

    Failing the record (instead of skipping) makes SQS redeliver it after the
    visibility timeout — by then the in-flight attempt has completed (the
    redelivery skips) or failed/expired (the redelivery processes). Skipping
    would lose the message if the in-flight attempt fails.
    """


class BatchFailedError(FastSQSError):
    """Raised when ``partial_batch_failure`` is False and at least one record
    failed: the whole batch is failed (the Lambda invocation raises) so SQS
    redelivers every message, instead of silently reporting no failures.

    The failed item identifiers are available on :attr:`failures`.
    """

    def __init__(self, failures: List[str], message: Optional[str] = None) -> None:
        self.failures = failures
        super().__init__(
            message
            or f"{len(failures)} record(s) failed and partial_batch_failure is "
            "False; failing the whole batch"
        )
