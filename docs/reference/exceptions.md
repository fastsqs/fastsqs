# Exceptions

The exception hierarchy. Every fastsqs *error* derives from `FastSQSError`;
`SkipMessage` deliberately does not — it is a control-flow ack signal, not an
error, so a blanket `except FastSQSError` can never swallow it.

::: fastsqs.FastSQSError

::: fastsqs.RouteNotFoundError

::: fastsqs.InvalidMessageError

::: fastsqs.BatchFailedError

::: fastsqs.IdempotencyInProgressError

::: fastsqs.SkipMessage
