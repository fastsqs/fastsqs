class RouteNotFound(Exception):
    """Exception raised when no route handler is found for a message."""
    pass


class InvalidMessage(Exception):
    """Exception raised when a message has invalid format or content."""
    pass


class BatchFailedError(Exception):
    """Raised when ``enable_partial_batch_failure`` is False and at least one
    record failed: the whole batch is failed (the Lambda invocation raises) so
    SQS redelivers every message, instead of silently reporting no failures."""
    pass