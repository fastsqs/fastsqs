# Deduplicate with idempotency

SQS standard queues deliver **at least once**: consumer crashes, visibility timeouts, and producer retries all create duplicates (a producer retry is a new `messageId`, so FIFO content deduplication does not fully cover it either). `IdempotencyMiddleware` dedups on an application-level key from the payload, so a handler runs once per logical event.

```python
from fastsqs import FastSQS, IdempotencyMiddleware, InMemoryIdempotencyStore, SQSEvent

app = FastSQS()
app.add_middleware(IdempotencyMiddleware(InMemoryIdempotencyStore()))


class OrderCreated(SQSEvent):
    order_id: str


@app.route(OrderCreated)
async def handle_order(msg: OrderCreated):
    ...


def handler(event, context):
    return app.handler(event, context)
```

`InMemoryIdempotencyStore` lives in the process: it does not survive restarts and is not shared across Lambda sandboxes. Use it for tests and local dev; production wants a shared store — see [Bring your own store](#bring-your-own-store).

## How acquire works

Before the handler runs, the middleware claims the key. The claim has three outcomes:

| Store answer | Meaning | What happens |
|---|---|---|
| `ACQUIRED` | first sighting | handler runs |
| `COMPLETED` | duplicate of a finished message | record **acks** (`SkipMessage`) — deleted, never redelivered |
| `IN_PROGRESS` | duplicate of an in-flight message | record **fails** (`IdempotencyInProgressError`) — SQS redelivers after the visibility timeout |

An in-flight duplicate fails on purpose: the first attempt might still crash, and skipping its duplicate would lose the message. By the time SQS redelivers, the first attempt has either completed (the redelivery skips) or failed (the redelivery processes).

After the handler, the claim settles: success (or a handler-raised `SkipMessage`) marks the key completed for `completed_ttl_seconds`; an exception releases the claim so the redelivery can retry. A worker that dies with no cleanup at all — a Lambda timeout is a SIGKILL — cannot lock its key forever: the IN_PROGRESS claim is a lease that expires after `in_progress_ttl_seconds`.

## Choose the key

The key is resolved from the payload via `key_path` — default `"id"`, which is exactly the CloudEvents `id` attribute. Dot-paths traverse nested envelopes:

```python
app.add_middleware(
    IdempotencyMiddleware(store, key_path="metadata.eventId")
)
```

Handlers and later middleware can read the resolved key at `ctx.state.idempotency_key`.

A payload without the key fails the record by default (if you installed idempotency, a keyless message is a contract violation). Pass `require_key=False` to process such messages without dedup instead.

## Tune the TTLs

- `in_progress_ttl_seconds` (default 300) — the lease. Must exceed your worst-case handler runtime (in practice: your Lambda timeout), or a slow first attempt loses its claim to a concurrent duplicate.
- `completed_ttl_seconds` (default 86400) — the dedup window. Duplicates arriving after it will process again; size it to how late your system can produce duplicates.

## Bring your own store

Storage is a **structural protocol** — any object with the three async methods satisfies it, no fastsqs base class required:

```python
import asyncio
import time

import boto3
from botocore.exceptions import ClientError

from fastsqs import AcquireResult


class DynamoDBIdempotencyStore:
    """Claims via conditional writes; ttl attribute expires stale entries."""

    def __init__(self, table_name: str) -> None:
        self._table = boto3.resource("dynamodb").Table(table_name)

    async def try_acquire(self, key: str, ttl_seconds: int) -> AcquireResult:
        now = int(time.time())
        try:
            await asyncio.to_thread(
                self._table.put_item,
                Item={"pk": key, "state": "in_progress", "expires_at": now + ttl_seconds},
                ConditionExpression="attribute_not_exists(pk) OR expires_at < :now",
                ExpressionAttributeValues={":now": now},
            )
            return AcquireResult.ACQUIRED
        except ClientError as error:
            if error.response["Error"]["Code"] != "ConditionalCheckFailedException":
                raise
            item = await asyncio.to_thread(
                lambda: self._table.get_item(Key={"pk": key}).get("Item", {})
            )
            if item.get("state") == "completed":
                return AcquireResult.COMPLETED
            return AcquireResult.IN_PROGRESS

    async def mark_complete(self, key: str, ttl_seconds: int) -> None:
        await asyncio.to_thread(
            self._table.put_item,
            Item={"pk": key, "state": "completed", "expires_at": int(time.time()) + ttl_seconds},
        )

    async def forget(self, key: str) -> None:
        await asyncio.to_thread(self._table.delete_item, Key={"pk": key})
```

The one hard requirement: `try_acquire` must be **atomic** in the backing store (DynamoDB conditional put, Redis `SET NX PX`, Postgres `INSERT ... ON CONFLICT`) — two concurrent callers with the same key must never both get `ACQUIRED`.

## Skip on purpose

`SkipMessage` is the ack signal the middleware uses for duplicates, and it is yours to use too — raise it from any handler or middleware `before` to finish a record successfully without processing:

```python
from fastsqs import SkipMessage


@app.route(OrderCreated)
async def handle_order(msg: OrderCreated):
    if await already_shipped(msg.order_id):
        raise SkipMessage("order already shipped")
    ...
```

A skipped record is a success: it never appears in `batchItemFailures`, does not halt its FIFO message group, and (under `IdempotencyMiddleware`) still marks its key completed. `TimingMiddleware` logs it as `status=SKIPPED`. `SkipMessage` is deliberately not a `FastSQSError`, so a blanket `except FastSQSError` in your code cannot swallow an ack by accident.
