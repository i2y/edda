# Transactional Outbox Pattern

Edda provides built-in support for the **Transactional Outbox Pattern**, ensuring that events are published reliably and atomically with workflow execution.

## What is the Transactional Outbox Pattern?

The Transactional Outbox Pattern solves the **dual-write problem** in distributed systems:

**Problem:**
```python
# ❌ This can fail!
await db.save_order(order)      # Step 1: Success
await broker.publish_event(...)  # Step 2: Fails! ← Event lost
```

If the broker is down or the network fails, the event is lost even though the order was saved.

**Solution:**
```python
# ✅ Atomic operation
async with transaction:
    await db.save_order(order)           # Step 1: Save order
    await db.save_to_outbox_table(event) # Step 2: Save event
# Later: Background process publishes events from outbox
```

Both operations succeed or fail together. A background **Outbox Relayer** publishes events asynchronously.

---

## Quick Start

### 1. Send Events Transactionally

Use `send_event_transactional()` inside activities:

```python
from edda import activity, WorkflowContext, send_event_transactional

@activity
async def create_order(ctx: WorkflowContext, order_id: str, amount: float):
    """Create an order and send event transactionally."""

    # Business logic
    print(f"Creating order {order_id}")

    # Send event transactionally
    # This writes to the outbox table in the same transaction
    await send_event_transactional(
        ctx,
        event_type="order.created",
        event_source="order-service",
        event_data={
            "order_id": order_id,
            "amount": amount,
            "status": "pending"
        }
    )

    return {"order_id": order_id, "status": "created"}
```

### 2. Enable Outbox Relayer

Configure `EddaApp` with `outbox_enabled=True`:

```python
from edda import EddaApp

app = EddaApp(
    service_name="order-service",
    db_url="sqlite:///orders.db",
    outbox_enabled=True,  # Enable outbox relayer
    broker_url="http://localhost:8080/events"  # CloudEvents endpoint
)

await app.initialize()
```

That's it! Events are now published reliably.

---

## Custom Database Operations (Advanced)

For **true atomic transactions** between your business logic and event publishing, you can access Edda's managed database session using `ctx.session`.

### Why Use ctx.session?

The basic approach (`send_event_transactional()` inside activities) uses **Edda's internal transaction** to write to the outbox table. This is sufficient for most use cases where your business logic is:

- Read-only operations
- External API calls
- Edda's own workflow history

However, if you need to **write to your own database** and guarantee atomicity with event publishing, use `ctx.session`:

### Example: Payment Processing with Custom Database Operations

```python
from sqlalchemy import Column, String, Float
from sqlalchemy.orm import declarative_base
from edda import WorkflowContext, activity
from edda.outbox.transactional import send_event_transactional

# Define your ORM models (must be in the same database as Edda)
Base = declarative_base()

class Payment(Base):
    __tablename__ = "payments"
    order_id = Column(String, primary_key=True)
    amount = Column(Float, nullable=False)
    status = Column(String, nullable=False)

@activity  # Edda automatically manages the transaction
async def process_payment(ctx: WorkflowContext, order_id: str, amount: float):
    """Process payment with custom database operations."""

    # Access Edda-managed session (same database as Edda)
    session = ctx.session

    # Your business logic (same transaction as Edda)
    payment = Payment(order_id=order_id, amount=amount, status="completed")
    session.add(payment)

    # Edda event (same transaction)
    await send_event_transactional(
        ctx,
        event_type="payment.processed",
        event_source="payment-service",
        event_data={"order_id": order_id, "amount": amount}
    )

    # Edda automatically commits (or rolls back on exception):
    # 1. Your payment record (same database)
    # 2. Edda's outbox event
    # 3. Edda's workflow history
    return {"order_id": order_id, "status": "processed"}
```

### ✅ Automatic Transaction Management

**Activities automatically manage transactions:**

```python
@activity  # Edda automatically manages the transaction
async def process_payment(ctx: WorkflowContext, order_id: str, amount: float):
    # Access Edda-managed session
    session = ctx.session

    # Your business logic
    payment = Payment(order_id=order_id, amount=amount)
    session.add(payment)

    # Events in same transaction
    await send_event_transactional(ctx, ...)

    # Edda automatically commits (or rolls back on exception)
    return {"order_id": order_id}
```

**How it works:**

When you access `ctx.session` inside an activity, Edda automatically:

1. **Provides managed session**: Returns the current transaction's session
2. **Single transaction**: All operations use the same session
3. **Commits or rolls back**: Commits on success, rolls back on exception

This results in:

1. **Single Transaction**: Your payment data + Edda's outbox + history commit together
2. **Atomic Guarantee**: Either all succeed or all fail (no data inconsistency)
3. **Rollback Safety**: If any operation fails, everything rolls back

### Key Points

- ✅ **Atomic Guarantee**: Your data + outbox event + history commit together
- ✅ **Single Transaction**: All operations use Edda's managed session
- ✅ **Automatic Management**: Edda handles begin/commit/rollback
- ✅ **Rollback Safety**: If any operation fails, everything rolls back
- ⚠️ **Database Requirement**: See [Database Requirements](#database-requirements-for-custom-database-operations) below

### When to Use ctx.session

| Use Case | Recommended Approach |
|----------|---------------------|
| **Writing to your own database + events** | `ctx.session` - Ensures atomicity |
| **Read-only operations** | Standard `@activity` - No custom DB access needed |
| **External API calls** | Standard `@activity` - No database transaction needed |

---

## Advanced Transaction Control

Activities are automatically transactional (see [Automatic Transaction Management](#automatic-transaction-management) above). However, in advanced scenarios, you may need finer control over transaction boundaries using `ctx.transaction()`.

### When to Use Advanced Transaction Control

- **Nested transactions (savepoints)** - Partial rollback within activities
- **Workflow-level transactions** - Manual control in workflow functions (not activities)

### Nested Transactions (Savepoints) in Activities

Within an activity, you can use `ctx.transaction()` for savepoints:

```python
from edda import WorkflowContext, activity
from edda.outbox.transactional import send_event_transactional

@activity
async def process_order_with_optional_payment(ctx: WorkflowContext, order_id: str):
    # Outer transaction (automatic, managed by @activity)
    session = ctx.session
    order = Order(order_id=order_id, status="pending")
    session.add(order)

    await send_event_transactional(
        ctx,
        event_type="order.created",
        event_source="order-service",
        event_data={"order_id": order_id}
    )

    try:
        async with ctx.transaction():  # Nested transaction (savepoint)
            # Risky payment operation
            await send_event_transactional(
                ctx,
                event_type="payment.attempted",
                event_source="payment-service",
                event_data={"order_id": order_id}
            )
            payment = Payment(order_id=order_id, amount=100.0)
            session.add(payment)
            # Simulate failure
            raise Exception("Payment gateway unavailable")
    except Exception:
        # Inner transaction rolled back to savepoint
        # Order creation still succeeds, but payment failed
        await send_event_transactional(
            ctx,
            event_type="payment.failed",
            event_source="payment-service",
            event_data={"order_id": order_id, "reason": "gateway_unavailable"}
        )

    # Outer transaction commits successfully
    return {"order_id": order_id, "status": "created"}
```

**Key points:**

- Outer transaction is **automatically managed** by `@activity`
- Inner `ctx.transaction()` creates a **savepoint**
- Inner transaction failure doesn't affect outer transaction
- Order and payment failure event are both committed

### Transaction Control in Workflows

In workflow functions (not activities), you can use `ctx.transaction()` directly:

```python
from edda import workflow, WorkflowContext
from edda.outbox.transactional import send_event_transactional

@workflow
async def custom_workflow(ctx: WorkflowContext, data: dict):
    # Manual transaction control (workflow functions don't auto-manage transactions)
    async with ctx.transaction():
        await ctx.storage.append_history(
            instance_id=ctx.instance_id,
            activity_id="custom_step:1",
            event_type="CustomEvent",
            event_data={"data": data}
        )
        await send_event_transactional(
            ctx,
            event_type="workflow.started",
            event_source="my-service",
            event_data={"data": data}
        )

    # Activities manage their own transactions
    result = await my_activity(ctx, data)
    return result
```

**Note:** This is rarely needed - prefer using activities for transactional operations.

---

## How It Works

The Transactional Outbox Pattern has two phases:

### Phase 1: Write to Outbox (Synchronous)

When you call `send_event_transactional()`:

```mermaid
sequenceDiagram
    participant W as Workflow
    participant A as Activity
    participant DB as Database

    W->>A: Execute activity
    A->>DB: BEGIN TRANSACTION
    A->>DB: Execute business logic
    A->>DB: INSERT INTO outbox_events
    A->>DB: INSERT INTO workflow_history
    A->>DB: COMMIT TRANSACTION
    A->>W: Return result
```

**Key Points:**

- Event is written to `outbox_events` table
- Same transaction as business logic
- If transaction fails, event is not stored
- If transaction succeeds, event will eventually be published

### Phase 2: Publish Events (Asynchronous)

The **Outbox Relayer** runs in the background:

```mermaid
sequenceDiagram
    participant R as Outbox Relayer
    participant DB as Database
    participant B as Message Broker

    loop Every 1 second (configurable)
        R->>DB: SELECT * FROM outbox_events WHERE status='pending'
        DB->>R: Return pending events

        loop For each event
            R->>B: POST CloudEvent
            alt Success
                B->>R: 200 OK
                R->>DB: UPDATE status='published'
            else Failure
                B->>R: 500 Error
                R->>DB: UPDATE retry_count++
            end
        end
    end
```

**Key Points:**

- Polls database every 1 second (default)
- Publishes events as CloudEvents (HTTP)
- Implements fixed-interval retry (retry on every poll until max_retries)
- At-least-once delivery guarantee

---

## Outbox Relayer Configuration

### Basic Configuration

```python
app = EddaApp(
    service_name="order-service",
    db_url="sqlite:///orders.db",
    outbox_enabled=True,
    broker_url="http://localhost:8080/events",
)
```

### Advanced Configuration

```python
from edda import EddaApp, OutboxRelayer

# Create custom relayer
relayer = OutboxRelayer(
    storage=app.storage,
    broker_url="http://localhost:8080/events",
    poll_interval=2.0,       # Poll every 2 seconds
    max_retries=5,           # Retry up to 5 times
    batch_size=20,           # Process 20 events per batch
    max_age_hours=24.0,      # Expire events older than 24 hours (optional)
)

# Start manually
await relayer.start()

# Stop gracefully
await relayer.stop()
```

**Configuration Options:**

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `storage` | `StorageProtocol` | Required | Storage backend for outbox events |
| `broker_url` | `str` | Required | Message Broker URL for publishing CloudEvents |
| `poll_interval` | `float` | `1.0` | Polling interval in seconds |
| `max_retries` | `int` | `3` | Maximum retry attempts for temporary failures |
| `batch_size` | `int` | `10` | Number of events to process per batch |
| `max_age_hours` | `float \| None` | `None` | Maximum event age in hours before expiration (disabled by default) |

**Max Age Timeout:**

Events older than `max_age_hours` are automatically marked as `'expired'` and will not be retried. This is useful for time-sensitive events that become meaningless after a certain period:

```python
# Example: Expire events older than 1 hour
relayer = OutboxRelayer(
    storage=app.storage,
    broker_url="http://localhost:8080/events",
    max_age_hours=1.0,  # Events older than 1 hour → 'expired'
)
```

**Use Cases for Max Age Timeout:**

- **Real-time notifications**: Push notifications that are only relevant for a short time
- **Time-sensitive updates**: Stock price updates, sensor readings
- **Temporary offers**: Flash sale notifications that expire quickly
- **Session-based events**: Events tied to user sessions that timeout

### Retry Logic and Error Handling

The Outbox Relayer implements **intelligent retry with error classification**:

**How it works:**

1. **Polling**: Every `poll_interval` seconds (default: 1s), the relayer checks for pending events
2. **Age Check** (Optional): If `max_age_hours` is set, events older than threshold → `'expired'` status
3. **Retry Check**: Before each send attempt, checks if `retry_count >= max_retries`
4. **Send Attempt**: If retry limit not exceeded, attempts to publish the event
5. **Success**: Event status → `'published'`
6. **Failure**: Error classification determines next action:
   - **4xx HTTP Error** (400, 401, 403, 404, 422, etc.) → `'invalid'` status (permanent failure, don't retry)
   - **5xx HTTP Error** (500, 502, 503, 504, etc.) → `'pending'` status, `retry_count` incremented (retry)
   - **Network Error** (connection timeout, DNS failure) → `'pending'` status, `retry_count` incremented (retry)
   - **Unknown Error** → `'pending'` status, `retry_count` incremented (retry, safety net)
7. **Max Retries Exceeded**: Event status → `'failed'` (permanently failed)

**Example with default settings** (`poll_interval=1s`, `max_retries=3`):

| Time | retry_count | Error Type | Action |
|------|-------------|-----------|--------|
| T+0s | 0 | First send attempt | |
| T+0s | 0 | → 400 Bad Request | **Stop**: Marked as `'invalid'` (client error) |
| T+0s | 0 | → 503 Service Unavailable | Continue: `retry_count` = 1 |
| T+1s | 1 | → Network timeout | Continue: `retry_count` = 2 |
| T+2s | 2 | → 500 Internal Server Error | Continue: `retry_count` = 3 |
| T+3s | 3 | N/A | **Stop**: `retry_count >= max_retries`, marked as `'failed'` |

**Important Notes:**

- **Fixed Interval**: Events are retried on every poll (no exponential backoff)
- **Retry Limit**: With `max_retries=3`, events are sent **3 times total** (at retry_count=0, 1, 2)
- **4xx Errors Don't Retry**: Client errors (malformed event, auth failure) are immediately marked as `'invalid'`
- **5xx Errors Retry**: Server errors (broker down, temporary failure) are retried up to max_retries
- **Network Errors Retry**: Connection timeouts, DNS failures are treated as temporary errors
- **Status Remains 'pending'**: During retries, event status stays `'pending'` (changes to `'published'` on success, `'failed'` after max retries, or `'invalid'` for 4xx errors)
- **Max Age Timeout**: Events older than `max_age_hours` are marked as `'expired'` and not retried

---

## With Pydantic Models

`send_event_transactional()` supports Pydantic models:

```python
from pydantic import BaseModel
from edda import activity, send_event_transactional

class OrderCreated(BaseModel):
    order_id: str
    customer_id: str
    amount: float
    items: list[str]

@activity
async def create_order(ctx: WorkflowContext, order_data: dict):
    # Create Pydantic model
    event = OrderCreated(
        order_id="ORD-123",
        customer_id="CUST-456",
        amount=99.99,
        items=["item1", "item2"]
    )

    # Send Pydantic model (auto-converted to JSON)
    await send_event_transactional(
        ctx,
        event_type="order.created",
        event_source="order-service",
        event_data=event  # ✅ Pydantic model supported
    )

    return {"status": "created"}
```

---

## Event Status Lifecycle

Events in the `outbox_events` table have five statuses:

```
pending → published  (Success)
        → failed     (After max_retries exceeded)
        → invalid    (4xx HTTP error, permanent failure)
        → expired    (Max age exceeded, too old to retry)
```

**Status Descriptions:**

| Status | Meaning | Retryable | How it happens |
|--------|---------|-----------|----------------|
| `pending` | Event waiting to be published | ✅ Yes | Initial status when event is created |
| `published` | Event successfully published | ❌ No | 2xx HTTP response from broker |
| `failed` | Event permanently failed after max retries | ❌ No | `retry_count >= max_retries` |
| `invalid` | Event is malformed or unauthorized | ❌ No | 4xx HTTP error (400, 401, 403, 404, 422) |
| `expired` | Event too old to be meaningful | ❌ No | `created_at` older than `max_age_hours` |

**Database Schema:**

```sql
-- SQLite / PostgreSQL / MySQL compatible schema
CREATE TABLE outbox_events (
    event_id VARCHAR(255) PRIMARY KEY,
    event_type VARCHAR(255) NOT NULL,
    event_source VARCHAR(255) NOT NULL,
    event_data TEXT NOT NULL,              -- JSON (unlimited size)
    content_type VARCHAR(100) NOT NULL DEFAULT 'application/json',
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    published_at TIMESTAMP,
    status VARCHAR(50) NOT NULL DEFAULT 'pending',
    retry_count INTEGER NOT NULL DEFAULT 0,
    last_error TEXT,

    -- Status constraint (enforced by database)
    CONSTRAINT valid_outbox_status CHECK (
        status IN ('pending', 'published', 'failed', 'invalid', 'expired')
    )
);

-- Index for efficient polling by Outbox Relayer
CREATE INDEX idx_outbox_status_created ON outbox_events(status, created_at);
```

**Notes:**
- **Column types**: Actual implementation uses precise types (VARCHAR with limits, TIMESTAMP with timezone support for PostgreSQL)
- **TIMESTAMP**: PostgreSQL/MySQL use timezone-aware TIMESTAMP; SQLite uses TEXT with ISO 8601 format
- **Constraint**: Database-level enforcement ensures status values are valid
- **Index**: Optimizes queries for pending events (used by Outbox Relayer)

**Query Outbox Events:**

```python
# Get pending events
events = await app.storage.get_pending_outbox_events(limit=10)

for event in events:
    print(f"Event: {event['event_type']}, Status: {event['status']}")
```

---

## Use Cases

### 1. Order Processing with Events

```python
@workflow
async def process_order(ctx: WorkflowContext, order_id: str, items: list[dict]):
    # Step 1: Reserve inventory
    inventory = await reserve_inventory(ctx, order_id, items)
    # → Sends "inventory.reserved" event

    # Step 2: Charge payment
    payment = await charge_payment(ctx, order_id, total_amount)
    # → Sends "payment.charged" event

    # Step 3: Ship order
    shipment = await ship_order(ctx, order_id)
    # → Sends "order.shipped" event

    # Final event
    await send_event_transactional(
        ctx,
        event_type="order.completed",
        event_source="order-service",
        event_data={"order_id": order_id, "status": "completed"}
    )

    return {"order_id": order_id, "status": "completed"}
```

### 2. Compensation with Events

Send events during compensation (rollback):

```python
@compensation
async def release_inventory(ctx: WorkflowContext, reservation_id: str):
    """Release inventory and publish event."""
    print(f"Releasing inventory: {reservation_id}")

    # Send compensation event
    await send_event_transactional(
        ctx,
        event_type="inventory.released",
        event_source="order-service",
        event_data={
            "reservation_id": reservation_id,
            "reason": "order_failed"
        }
    )
```

### 3. Saga Pattern Integration

```python
@activity
@on_failure(cancel_reservation)
async def reserve_hotel(ctx: WorkflowContext, booking_id: str):
    # Reserve hotel
    reservation_id = f"HOTEL-{booking_id}"

    # Send event transactionally
    await send_event_transactional(
        ctx,
        event_type="hotel.reserved",
        event_source="booking-service",
        event_data={"reservation_id": reservation_id}
    )

    return {"reservation_id": reservation_id}

@compensation
async def cancel_reservation(ctx: WorkflowContext, reservation_id: str):
    # Send cancellation event
    await send_event_transactional(
        ctx,
        event_type="hotel.cancelled",
        event_source="booking-service",
        event_data={"reservation_id": reservation_id}
    )
```

---

## Best Practices

### 1. Activity Transaction Control

Activities are **automatically transactional**. All operations within an activity (including `send_event_transactional()`) are executed within a single transaction.

```python
# ✅ Default: Automatic transaction (recommended)
@activity
async def create_order(ctx: WorkflowContext, order_id: str):
    # All operations are automatically transactional:
    # - Workflow history recording
    # - Event publishing to outbox
    await send_event_transactional(ctx, "order.created", ...)

# ✅ With external session sharing
@activity
async def create_order_with_db(ctx: WorkflowContext, order_id: str):
    # Access Edda-managed session
    session = ctx.session

    # Your database operations
    order = Order(order_id=order_id)
    session.add(order)

    # Events in same transaction
    await send_event_transactional(ctx, "order.created", ...)

# ❌ Bad: Outside activity (no transaction)
@workflow
async def order_workflow(ctx: WorkflowContext):
    await send_event_transactional(ctx, ...)  # ⚠️ Warning logged
```

**Default behavior:**

- ✅ Automatic transaction management
- ✅ Events are atomic with workflow history
- ✅ No manual begin/commit/rollback needed

### 2. Understanding Transaction Warnings

When you call `send_event_transactional()` **outside** of a transaction, Edda logs a warning but still sends the event. However, **atomicity is not guaranteed**.

**Warning Message:**
```
WARNING: send_event_transactional() called outside of a transaction.
Event will still be sent, but atomicity with other operations is not guaranteed.
Consider using @activity (with default transactional=True) or wrapping in ctx.transaction().
```

**How to fix:**

```python
# ❌ Problem: No transaction
@workflow
async def my_workflow(ctx: WorkflowContext):
    await send_event_transactional(ctx, ...)  # ⚠️ Warning!

# ✅ Solution: Use activity (automatic transaction)
@activity
async def my_activity(ctx: WorkflowContext):
    await send_event_transactional(ctx, ...)  # ✅ Atomic
```

**Detecting if you're in a transaction:**
```python
if ctx.in_transaction():
    # Safe to call send_event_transactional()
    pass
```

### 3. Use CloudEvents Standard

Edda publishes events as CloudEvents (HTTP), ensuring compatibility with:

- ✅ Knative Eventing
- ✅ CloudEvents SDK
- ✅ Any CloudEvents-compatible broker

### 4. Monitor Outbox Table

Monitor your outbox table regularly to detect issues:

```python
from sqlalchemy import text, select, func
from edda.storage.sqlalchemy_storage import OutboxEvent

# Get event status summary
async with AsyncSession(app.storage.engine) as session:
    result = await session.execute(
        select(
            OutboxEvent.status,
            func.count(OutboxEvent.event_id).label('count')
        ).group_by(OutboxEvent.status)
    )

    status_counts = {row.status: row.count for row in result}

    print(f"📊 Outbox Status Summary:")
    print(f"  - Pending:   {status_counts.get('pending', 0)}")
    print(f"  - Published: {status_counts.get('published', 0)}")
    print(f"  - Failed:    {status_counts.get('failed', 0)}")
    print(f"  - Invalid:   {status_counts.get('invalid', 0)}")  # 4xx errors
    print(f"  - Expired:   {status_counts.get('expired', 0)}")  # Too old

# Alert on invalid events (indicates malformed events or configuration issues)
if status_counts.get('invalid', 0) > 0:
    print(f"⚠️ {status_counts['invalid']} events marked as invalid (4xx errors)")
    print("   → Check event format, broker authentication, or permissions")

# Alert on expired events (might indicate broker outage)
if status_counts.get('expired', 0) > 0:
    print(f"⚠️ {status_counts['expired']} events expired due to age")
    print("   → Check broker availability or consider increasing max_age_hours")

# Alert on permanently failed events (max retries exceeded)
if status_counts.get('failed', 0) > 0:
    print(f"⚠️ {status_counts['failed']} events failed permanently")
    print("   → Check broker health or network connectivity")
```

**Monitoring Best Practices:**

- **Set up alerts** for `invalid` status (indicates configuration issues)
- **Monitor `expired` count** (indicates prolonged broker unavailability)
- **Track `failed` events** (indicates persistent problems)
- **Review `last_error` field** to diagnose root causes:

```python
# Get details of invalid events
async with AsyncSession(app.storage.engine) as session:
    invalid_events = await session.execute(
        select(OutboxEvent).where(OutboxEvent.status == 'invalid').limit(10)
    )

    for event in invalid_events.scalars():
        print(f"Invalid Event: {event.event_type}")
        print(f"  Error: {event.last_error}")
        print(f"  Created: {event.created_at}")
```

### 5. Handle Idempotency

Since the outbox pattern provides **at-least-once delivery**, consumers must handle duplicate events:

```python
# Consumer side (idempotency key)
@app.post("/events")
async def handle_event(event: CloudEvent):
    event_id = event["id"]

    # Check if already processed
    if await db.event_already_processed(event_id):
        return {"status": "duplicate"}

    # Process event
    await process_order_created(event.data)

    # Mark as processed
    await db.mark_event_processed(event_id)
```

---

## Complete Example: E-commerce Order Processing

This example demonstrates a real-world e-commerce order processing system using **custom database operations** with `ctx.session`.

### Scenario

1. Create order in your database tables (same database as Edda)
2. Send "order.created" event to Edda's outbox
3. Both operations are atomic (same transaction)

### Code

```python
from sqlalchemy import Column, String, Float, DateTime
from sqlalchemy.orm import declarative_base
from datetime import datetime
from edda import EddaApp, WorkflowContext, activity, workflow
from edda.outbox.transactional import send_event_transactional

# Your database models (same database as Edda)
Base = declarative_base()

class Order(Base):
    __tablename__ = "orders"

    order_id = Column(String(255), primary_key=True)
    customer_email = Column(String(255), nullable=False)
    amount = Column(Float, nullable=False)
    status = Column(String(50), nullable=False)
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)

# Edda app (all tables in same database)
edda_app = EddaApp(
    service_name="order-service",
    db_url="postgresql+asyncpg://user:password@localhost/ecommerce",
    outbox_enabled=True,
    broker_url="http://localhost:8080/events"
)

# Activity with custom database operations
@activity  # Edda automatically manages the transaction
async def create_order(
    ctx: WorkflowContext,
    order_id: str,
    customer_email: str,
    amount: float
):
    """Create order with atomic event publishing."""

    # Access Edda-managed session (same database)
    session = ctx.session

    # 1. Save order to YOUR table (same database as Edda)
    order = Order(
        order_id=order_id,
        customer_email=customer_email,
        amount=amount,
        status="pending"
    )
    session.add(order)

    # 2. Publish event to Edda's outbox (SAME transaction)
    await send_event_transactional(
        ctx,
        event_type="order.created",
        event_source="order-service",
        event_data={
            "order_id": order_id,
            "customer_email": customer_email,
            "amount": amount,
            "status": "pending"
        }
    )

    # 3. Edda automatically commits (or rolls back on exception)
    # If event publishing fails, order creation rolls back
    # If order creation fails, event publishing rolls back

    return {"order_id": order_id, "status": "created"}

# Workflow
@workflow
async def order_workflow(
    ctx: WorkflowContext,
    order_id: str,
    customer_email: str,
    amount: float
):
    """Order processing workflow."""

    # Create order (atomic with event)
    result = await create_order(ctx, order_id, customer_email, amount)

    return result

# Run
async def main():
    await edda_app.initialize()

    # Start workflow
    instance_id = await order_workflow.start(
        order_id="ORD-12345",
        customer_email="customer@example.com",
        amount=149.99
    )

    print(f"Order created: {instance_id}")

    # Event will be published by Outbox Relayer
    await asyncio.sleep(2)

    await edda_app.shutdown()

if __name__ == "__main__":
    import asyncio
    asyncio.run(main())
```

### Database Requirements for Custom Database Operations

When using `ctx.session` to access Edda's managed session for custom database operations, your application tables must be in the **same database** as Edda's tables:

**PostgreSQL**:

- **Requirement**: Your tables and Edda's tables must be in the **same PostgreSQL database** (not just the same server/instance)
- **Reason**: PostgreSQL connections are bound to a single database
- **Example**: If Edda uses `postgresql+asyncpg://user:pass@localhost/myapp`, your tables must also be in the `myapp` database

**MySQL**:

- **Requirement**: Your tables and Edda's tables must be in the **same MySQL database**
- **Reason**: While MySQL technically allows cross-database transactions, we recommend using the same database for simplicity
- **Example**: If Edda uses `mysql+aiomysql://user:pass@localhost/myapp`, your tables should also be in the `myapp` database

**SQLite**:

- **Requirement**: Your tables and Edda's tables must be in the **same SQLite file**
- **Reason**: SQLite is file-based; cross-file transactions are not possible
- **Example**: If Edda uses `sqlite+aiosqlite:///workflow.db`, your tables must also be in `workflow.db`

**Best Practice**: Use the same database for both Edda and your application tables. This simplifies deployment and ensures transactional consistency.

### Key Points

1. **Database Requirement**: See [Database Requirements](#database-requirements-for-custom-database-operations) above for PostgreSQL/MySQL/SQLite specifics
2. **Session Access**: `ctx.session` provides Edda's managed session
3. **Automatic Management**: Edda handles begin/commit/rollback automatically
4. **Atomic Guarantee**: Order creation + event publishing commit together
5. **Rollback Safety**: If any step fails, everything rolls back

### What Happens

```
1. Order saved to YOUR database (orders table)
2. Event saved to Edda's outbox (outbox_events table)
3. Both committed atomically (COMMIT)
4. Outbox Relayer publishes event (async, background)
5. Event delivered to broker (at-least-once guarantee)
```

---

## Complete Example

See the complete example at [examples/with_outbox.py](https://github.com/i2y/edda/blob/main/examples/with_outbox.py).

**Run the example:**

```bash
# Set broker URL (or use default)
export BROKER_URL="http://localhost:8080/events"

# Run the workflow
uv run python examples/with_outbox.py
```

**Expected Output:**

```
[Activity] Reserving inventory for order ORD-12345
[Activity] Event 'inventory.reserved' written to outbox
[Activity] Charging payment for order ORD-12345: $149.99
[Activity] Event 'payment.charged' written to outbox
[Activity] Shipping order ORD-12345
[Activity] Event 'order.shipped' written to outbox

[Outbox Relayer] Publishing 3 pending events...
[Outbox Relayer] ✅ Published: inventory.reserved
[Outbox Relayer] ✅ Published: payment.charged
[Outbox Relayer] ✅ Published: order.shipped
```

---

## What You Learned

- ✅ **Transactional Outbox Pattern**: Solves the dual-write problem
- ✅ **send_event_transactional()**: Writes events to outbox table
- ✅ **ctx.session**: Access Edda's managed session for custom database operations
- ✅ **Automatic Transaction Management**: Edda handles begin/commit/rollback
- ✅ **Rollback Safety**: All operations roll back on failure
- ✅ **Outbox Relayer**: Publishes events asynchronously with retry
- ✅ **At-Least-Once Delivery**: Guaranteed event delivery
- ✅ **CloudEvents Support**: Standard event format
- ✅ **Pydantic Integration**: Type-safe event data

## Next Steps

- **[Saga Pattern](../examples/saga.md)**: Combine with compensation
- **[CloudEvents HTTP Binding](events/cloudevents-http-binding.md)**: CloudEvents specification compliance
- **[Hooks](hooks.md)**: Monitor outbox events with lifecycle hooks
