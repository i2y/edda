# Event Waiting Example

This example demonstrates how workflows can wait for external events without blocking worker processes.

## What This Example Shows

- ✅ `wait_event()` for waiting for external events
- ✅ `sleep()` for time-based waiting
- ✅ Process-releasing behavior (workflow pauses, worker is freed)
- ✅ Event-driven workflow continuation

## The Problem

Traditional approaches keep the async task in memory while waiting:

```python
# ❌ Bad: Keeps coroutine in memory for 1 hour
await asyncio.sleep(3600)  # Workflow state held in RAM unnecessarily
```

Edda's `wait_event()` and `sleep()` **persist the workflow state to the database** and release the memory, allowing the async task to be garbage collected. The worker can then handle other workflows.

## Code Overview

### Wait for External Event

```python
from edda import workflow, activity, wait_event, WorkflowContext

@activity
async def start_payment_processing(ctx: WorkflowContext, order_id: str):
    """Initiate payment processing with external service."""
    print(f"🔄 Starting payment for order {order_id}")
    # Call external payment service API...
    return {"payment_id": f"PAY-{order_id}", "status": "pending"}

@workflow
async def payment_workflow(ctx: WorkflowContext, order_id: str):
    """
    Payment workflow that waits for external payment completion event.

    Note: Activity IDs are auto-generated for sequential execution.
    """
    # Step 1: Start payment processing (auto-generated ID: "start_payment_processing:1")
    payment = await start_payment_processing(ctx, order_id)
    print(f"Payment started: {payment['payment_id']}")

    # Step 2: Wait for payment completion event
    # Workflow pauses here, worker process is released
    print("⏸️  Waiting for payment.completed event...")
    event = await wait_event(
        ctx,
        event_type="payment.completed",
        timeout_seconds=300  # 5-minute timeout
    )

    # Step 3: Process payment result
    print(f"✅ Payment completed: {event.data}")
    return {"status": "completed", "payment_result": event.data}
```

### Wait for Timer

```python
from edda import sleep

@workflow
async def order_with_timeout(ctx: WorkflowContext, order_id: str):
    """
    Order workflow with payment timeout.

    Note: Activity IDs are auto-generated for sequential execution.
    """
    # Step 1: Create order (auto-generated ID: "create_order:1")
    await create_order(ctx, order_id)
    print(f"Order {order_id} created")

    # Step 2: Wait 60 seconds for payment
    print("⏱️  Waiting 60 seconds for payment...")
    await sleep(ctx, seconds=60)

    # Step 3: Check payment status (auto-generated ID: "check_payment_status:1")
    status = await check_payment_status(ctx, order_id)

    if status["paid"]:
        print("✅ Payment received!")
        return {"status": "completed"}
    else:
        print("❌ Payment timeout - cancelling order")
        # Step 4: Cancel order (auto-generated ID: "cancel_order:1")
        await cancel_order(ctx, order_id)
        return {"status": "cancelled", "reason": "payment_timeout"}
```

## How It Works

### Event Waiting Flow

```
1. Workflow executes: start_payment_processing()
2. Workflow hits: wait_event()
3. Workflow pauses (status="waiting_for_event")
4. Worker process is RELEASED (can handle other workflows)
5. External event arrives (e.g., CloudEvent)
6. Workflow RESUMES from wait_event()
7. Workflow continues: process payment result
```

### ReceivedEvent Structure

```python
from edda import ReceivedEvent

event = await wait_event(ctx, "payment.completed")

# event is a ReceivedEvent instance
print(event.type)    # "payment.completed"
print(event.source)  # "payment-service"
print(event.data)    # {"transaction_id": "...", "amount": 99.99}
```

### Type-Safe Events with Pydantic

Use Pydantic models for type-safe event data access:

```python
from pydantic import BaseModel

class PaymentCompleted(BaseModel):
    order_id: str
    transaction_id: str
    amount: float
    status: str

@workflow
async def payment_workflow_typed(ctx: WorkflowContext, order_id: str):
    """
    Payment workflow with type-safe event handling.
    """
    # Wait for event with Pydantic model
    event = await wait_event(
        ctx,
        event_type="payment.completed",
        model=PaymentCompleted  # Type-safe conversion
    )

    # Type-safe access with IDE completion
    amount = event.data.amount  # ✅ Type-safe (float)
    transaction_id = event.data.transaction_id  # ✅ Type-safe (str)
    order_id = event.data.order_id  # ✅ Type-safe (str)

    print(f"✅ Payment of ${amount} completed for order {order_id}")
    print(f"   Transaction ID: {transaction_id}")

    return {"status": "completed", "amount": amount}
```

**Benefits of Pydantic models:**
- ✅ **Type safety**: IDE autocomplete and mypy validation
- ✅ **Runtime validation**: Automatic data validation when event arrives
- ✅ **Clear contracts**: Explicit event structure definition
- ✅ **Error detection**: Invalid events fail fast with clear error messages

**Without Pydantic (dict access):**
```python
event = await wait_event(ctx, "payment.completed")
amount = event.data["amount"]  # ⚠️ No type checking, typo possible
```

**With Pydantic (model access):**
```python
event = await wait_event(ctx, "payment.completed", model=PaymentCompleted)
amount = event.data.amount  # ✅ Type-safe, IDE autocomplete
```

## Benefits

### 1. Resource Efficiency

```python
# ❌ Bad: Keeps workflow state in memory for 1 hour
@workflow
async def bad_workflow(ctx: WorkflowContext):
    await asyncio.sleep(3600)  # Task held in RAM!

# ✅ Good: Persists state and releases memory
@workflow
async def good_workflow(ctx: WorkflowContext):
    await sleep(ctx, seconds=3600)  # Memory freed!
```

**Impact:**

- Bad: 1 worker holds 1 workflow state in RAM (wasted memory)
- Good: 1 worker can handle 1000s of workflows (state persisted to DB)

### 2. Long-Running Workflows

Perfect for workflows that span hours or days:

```python
@workflow
async def loan_approval_workflow(ctx: WorkflowContext, application_id: str):
    # Submit for manual review
    await submit_for_review(ctx, application_id)

    # Wait up to 48 hours for approval
    event = await wait_event(
        ctx,
        event_type="loan.approved",
        timeout_seconds=48 * 3600
    )

    # Process approval
    await process_approval(ctx, event.data)
```

### 3. Event-Driven Architecture

Integrate with event-driven systems:

```python
@workflow
async def order_fulfillment(ctx: WorkflowContext, order_id: str):
    # Wait for warehouse to pack the order
    pack_event = await wait_event(ctx, "order.packed")

    # Wait for carrier to pick up
    pickup_event = await wait_event(ctx, "order.picked_up")

    # Wait for delivery confirmation
    delivery_event = await wait_event(ctx, "order.delivered")

    return {"status": "delivered"}
```

## Sending Events

To resume a waiting workflow, send a CloudEvent:

```bash
# Using curl
curl -X POST http://localhost:8001/events \
  -H "Content-Type: application/cloudevents+json" \
  -d '{
    "specversion": "1.0",
    "type": "payment.completed",
    "source": "payment-service",
    "id": "event-123",
    "datacontenttype": "application/json",
    "data": {
      "order_id": "ORD-123",
      "transaction_id": "TXN-456",
      "amount": 99.99,
      "status": "success"
    }
  }'
```

**Expected Response:**

```http
HTTP/1.1 202 Accepted
Content-Type: application/json

{
  "status": "accepted"
}
```

**Status Codes:**

- **202 Accepted**: Event accepted for processing ✅
- **400 Bad Request**: Invalid CloudEvent format (non-retryable) ❌
- **500 Internal Server Error**: Server error (retryable) ⚠️

See [CloudEvents HTTP Binding](../core-features/events/cloudevents-http-binding.md) for detailed error handling and retry logic.

Or programmatically:

```python
from edda import send_event

await send_event(
    event_type="payment.completed",
    source="payment-service",
    data={
        "order_id": "ORD-123",
        "transaction_id": "TXN-456",
        "amount": 99.99,
        "status": "success"
    }
)
```

## Running the Example

Create a file named `event_waiting_workflow.py` with the code shown above, then run:

```bash
# Install Edda if you haven't already
uv add edda-framework

# Run your workflow
uv run python event_waiting_workflow.py
```

## Complete Code

See the full implementation in [examples/event_waiting_workflow.py](https://github.com/i2y/edda/blob/main/examples/event_waiting_workflow.py).

## What You Learned

- ✅ **`wait_event()`**: Wait for external events
- ✅ **`sleep()`**: Wait for specific duration
- ✅ **Process Releasing**: Workers are freed during wait
- ✅ **ReceivedEvent**: Typed event data access
- ✅ **CloudEvents**: Standard event format support

## Next Steps

- **[CloudEvents HTTP Binding](../core-features/events/cloudevents-http-binding.md)**: Deep dive into CloudEvents integration
- **[Core Concepts](../getting-started/concepts.md)**: Learn about workflows, activities, and events
- **[Transactional Outbox](../core-features/transactional-outbox.md)**: Reliable event publishing
