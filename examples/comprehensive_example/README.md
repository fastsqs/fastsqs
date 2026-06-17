# Comprehensive FastSQS Example

This example demonstrates the middleware features of FastSQS.

## Features Demonstrated

### 1. Concurrency
Configured on the app itself: `FastSQS(max_concurrent_messages=5)`. Records in a
batch are processed concurrently up to that limit (asyncio).

### 2. Error Handling & Dead-Letter Routing
- `ErrorHandlingMiddleware` — classifies errors (permanent vs temporary).
- `DeadLetterQueueMiddleware` — routes terminal failures to a dead-letter
  handler; can flag messages exceeding `max_processing_time`.

> Retries are **not** performed in-process. SQS redelivers failed messages via
> the visibility timeout + `maxReceiveCount`, with its own native dead-letter
> queue — so the middleware fails fast and lets SQS redeliver.

### 3. Logging & Timing
- `LoggingMiddleware` — structured JSON logging.
- `TimingMsMiddleware` — per-message duration.

## Middleware Stack

1. **LoggingMiddleware** — structured logging
2. **TimingMsMiddleware** — per-message timing
3. **ErrorHandlingMiddleware** — error classification
4. **DeadLetterQueueMiddleware** — dead-letter routing

## Usage

### Local Testing
```bash
python lambda_function.py
```

### AWS Lambda Deployment
1. Package the code with dependencies
2. Set IAM permissions for SQS (and your DLQ)
3. Configure the SQS trigger with the desired queue

## Configuration Options

### Error Handling
```python
def my_dlq(payload, record, error):
    ...  # ship to your DLQ / alerting

app.add_middleware(ErrorHandlingMiddleware(dead_letter_handler=my_dlq))
app.add_middleware(DeadLetterQueueMiddleware(max_processing_time=300.0))
```

### Concurrency
```python
app = FastSQS(max_concurrent_messages=5)
```

## Message Routing

The example routes three event models by their `type` discriminator:

- **order_processing** — standard order processing
- **high_volume_message** — high-throughput processing
- **critical_message** — critical messages

## Production Considerations

1. **Monitoring**: integrate logging with CloudWatch or your system
2. **Error Handling**: configure an SQS DLQ (redrive policy) + alerting
3. **Concurrency**: tune `max_concurrent_messages` for your workload
4. **Timeouts**: set the queue visibility timeout based on processing time
