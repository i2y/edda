# Event Waiting and Timers

Edda provides powerful event waiting capabilities that allow workflows to pause and wait for external events or timers without blocking worker processes. This enables efficient resource utilization and event-driven workflow orchestration.

## Overview

Event waiting in Edda allows workflows to:

- **Pause execution** and wait for external events
- **Release worker processes** while waiting (non-blocking)
- **Resume automatically** when events arrive
- **Handle timeouts** gracefully
- **Wait for specific durations** with timers

## Key Functions

### `wait_event()`

Wait for an external event with optional timeout:

```python
from edda import wait_event, WorkflowContext

async def my_workflow(ctx: WorkflowContext):
    # Wait for an event (with 5-minute timeout)
    event = await wait_event(
        ctx,
        event_type="payment.completed",
        timeout_seconds=300  # Optional timeout
    )

    # Access event data
    payment_id = event.data["payment_id"]
    amount = event.data["amount"]
```

### `sleep()`

Wait for a specific duration:

```python
from edda import sleep, WorkflowContext

async def my_workflow(ctx: WorkflowContext):
    # Wait for 60 seconds
    await sleep(ctx, seconds=60)

    # Continue execution after timer expires
    await process_timeout()
```

> **Note**: `wait_timer()` and `wait_until()` are backward-compatible aliases that also work:
>
> ```python
> from edda import sleep, sleep_until  # Primary names
> from edda import wait_timer, wait_until  # Aliases (backward-compatible)
> ```

## Detailed API Reference

### wait_event()

```python
async def wait_event(
    ctx: WorkflowContext,
    event_type: str,
    timeout_seconds: int | None = None,
    model: type[BaseModel] | None = None
) -> ReceivedEvent:
```

#### Parameters

- `ctx`: The workflow context
- `event_type`: CloudEvents type to wait for (e.g., "payment.completed")
- `timeout_seconds`: Optional timeout in seconds
- `model`: Optional Pydantic model for automatic data validation

#### Returns

`ReceivedEvent` object containing:

- `data`: Event data (dict or Pydantic model if specified)
- `type`: CloudEvents type
- `source`: Event source
- `subject`: Optional event subject
- `time`: Event timestamp
- `id`: Event ID

#### Example with Pydantic

```python
from pydantic import BaseModel
from edda import wait_event, WorkflowContext

class PaymentCompleted(BaseModel):
    payment_id: str
    amount: float
    status: str

@workflow
async def payment_workflow(ctx: WorkflowContext, order_id: str):
    # Wait with automatic validation
    event = await wait_event(
        ctx,
        event_type="payment.completed",
        timeout_seconds=300,
        model=PaymentCompleted  # Automatic validation
    )

    # event.data is now a PaymentCompleted instance
    if event.data.status == "success":
        return {"order_id": order_id, "payment_id": event.data.payment_id}
```

### sleep()

```python
async def sleep(
    ctx: WorkflowContext,
    seconds: int,
    timer_id: str | None = None
) -> None:
```

#### Parameters

- `ctx`: The workflow context
- `seconds`: Duration to wait in seconds
- `timer_id`: Optional custom timer ID (auto-generated if not provided)

#### Example

```python
from edda import sleep, workflow, WorkflowContext

@workflow
async def scheduled_task(ctx: WorkflowContext, task_id: str):
    # Execute immediately
    await perform_initial_task(ctx, task_id)

    # Wait 1 hour
    await sleep(ctx, seconds=3600)

    # Execute after delay
    await perform_delayed_task(ctx, task_id)
```

## Common Patterns

### Payment Processing with Timeout

```python
@workflow
async def payment_with_timeout(ctx: WorkflowContext, order_id: str):
    """Process payment with automatic cancellation on timeout."""

    # Initiate payment
    payment_id = await initiate_payment(ctx, order_id)

    try:
        # Wait for payment completion (5-minute timeout)
        event = await wait_event(
            ctx,
            event_type="payment.completed",
            timeout_seconds=300
        )

        if event.data["status"] == "success":
            await fulfill_order(ctx, order_id)
            return {"status": "completed"}
        else:
            await cancel_order(ctx, order_id)
            return {"status": "payment_failed"}

    except TimeoutError:
        # Timeout occurred
        await cancel_payment(ctx, payment_id)
        await cancel_order(ctx, order_id)
        return {"status": "timeout"}
```

### Approval Workflow

```python
@workflow
async def approval_workflow(ctx: WorkflowContext, request_id: str):
    """Wait for manager approval with escalation."""

    # Send approval request
    await send_approval_request(ctx, request_id, level="manager")

    # Wait for manager approval (1 day)
    try:
        event = await wait_event(
            ctx,
            event_type="approval.decision",
            timeout_seconds=86400  # 24 hours
        )

        return {"approved": event.data["approved"], "approver": event.data["approver"]}

    except TimeoutError:
        # Escalate to director
        await send_approval_request(ctx, request_id, level="director")

        event = await wait_event(
            ctx,
            event_type="approval.decision",
            timeout_seconds=86400
        )

        return {"approved": event.data["approved"], "approver": event.data["approver"], "escalated": True}
```

### Batch Processing with Delays

```python
@workflow
async def batch_processor(ctx: WorkflowContext, batch_id: str, items: list[dict]):
    """Process items in batches with delays to avoid rate limiting."""

    results = []
    batch_size = 10

    for i in range(0, len(items), batch_size):
        batch = items[i:i + batch_size]

        # Process batch
        batch_results = await process_batch(ctx, batch)
        results.extend(batch_results)

        # Wait between batches (except for last batch)
        if i + batch_size < len(items):
            await sleep(ctx, seconds=5)

    return {"batch_id": batch_id, "results": results}
```

### Orchestrating Multiple Services

```python
@workflow
async def multi_service_orchestration(ctx: WorkflowContext, request_id: str):
    """Orchestrate multiple async services."""

    # Start all services
    await trigger_service_a(ctx, request_id)
    await trigger_service_b(ctx, request_id)
    await trigger_service_c(ctx, request_id)

    # Wait for all services to complete
    results = {}

    # Wait for Service A
    event_a = await wait_event(ctx, "service.a.completed", timeout_seconds=600)
    results["service_a"] = event_a.data

    # Wait for Service B
    event_b = await wait_event(ctx, "service.b.completed", timeout_seconds=600)
    results["service_b"] = event_b.data

    # Wait for Service C
    event_c = await wait_event(ctx, "service.c.completed", timeout_seconds=600)
    results["service_c"] = event_c.data

    # Aggregate and return results
    return aggregate_results(results)
```

## Sending Events to Waiting Workflows

### Using CloudEvents

Send CloudEvents to resume waiting workflows:

```python
import httpx
from cloudevents.http import CloudEvent
from cloudevents.conversion import to_structured

# Create CloudEvent
event = CloudEvent({
    "type": "payment.completed",
    "source": "payment-service",
    "data": {
        "payment_id": "PAY-123",
        "amount": 99.99,
        "status": "success"
    }
})

# Send to Edda
headers, body = to_structured(event)
async with httpx.AsyncClient() as client:
    response = await client.post(
        "http://localhost:8001/",
        headers=headers,
        content=body
    )
```

### Using send_event()

From within workflows or activities:

```python
from edda import send_event

@activity
async def process_payment(ctx: WorkflowContext, payment_data: dict):
    # Process payment...

    # Send event to resume waiting workflows
    await send_event(
        event_type="payment.completed",
        event_source="payment-processor",
        event_data={
            "payment_id": payment_data["id"],
            "amount": payment_data["amount"],
            "status": "success"
        }
    )
```

## Best Practices

### 1. Always Set Timeouts

Prevent workflows from waiting indefinitely:

```python
# Good: Has timeout
event = await wait_event(ctx, "payment.completed", timeout_seconds=300)

# Bad: No timeout (could wait forever)
event = await wait_event(ctx, "payment.completed")
```

### 2. Handle Timeout Gracefully

```python
try:
    event = await wait_event(ctx, "approval.decision", timeout_seconds=3600)
    # Process approval
except TimeoutError:
    # Handle timeout scenario
    await handle_timeout_scenario(ctx)
```

### 3. Use Unique Event Types

Avoid event type collisions:

```python
# Good: Specific event types
await wait_event(ctx, "order.payment.completed")
await wait_event(ctx, "subscription.payment.completed")

# Bad: Generic event type
await wait_event(ctx, "completed")  # Too generic
```

### 4. Include Correlation Data

For matching specific events:

```python
@workflow
async def order_workflow(ctx: WorkflowContext, order_id: str):
    # Send order_id as correlation data
    await initiate_payment(ctx, order_id)

    # Wait for event with matching order_id
    event = await wait_event(
        ctx,
        event_type=f"payment.completed.{order_id}",
        timeout_seconds=300
    )
```

### 5. Consider Idempotency

Ensure events can be safely redelivered:

```python
@workflow
async def idempotent_workflow(ctx: WorkflowContext, request_id: str):
    # Check if already processed
    if await is_already_processed(ctx, request_id):
        return await get_previous_result(ctx, request_id)

    # Wait for event
    event = await wait_event(ctx, f"process.{request_id}")

    # Process idempotently
    result = await process_idempotently(ctx, event.data)
    return result
```

## Advanced Topics

### Multiple Event Types

Wait for any of multiple event types:

```python
@workflow
async def multi_event_workflow(ctx: WorkflowContext, order_id: str):
    # Currently, wait for specific event type
    # For multiple types, use separate wait_event calls or
    # implement pattern matching in event type

    # Option 1: Sequential checks
    try:
        event = await wait_event(ctx, "payment.completed", timeout_seconds=60)
    except TimeoutError:
        event = await wait_event(ctx, "payment.failed", timeout_seconds=60)

    # Process based on event type
    if event.type == "payment.completed":
        return {"status": "success"}
    else:
        return {"status": "failed"}
```

### Event Filtering

Filter events based on data:

```python
@workflow
async def filtered_event_workflow(ctx: WorkflowContext, user_id: str):
    # Wait for event specific to this user
    event = await wait_event(
        ctx,
        event_type=f"user.action.{user_id}",
        timeout_seconds=300
    )

    # Additional filtering can be done after receiving
    if event.data.get("action") == "purchase":
        await process_purchase(ctx, event.data)
```

### Cancellation

Workflows waiting for events can be cancelled:

```python
# Via API
POST /cancel/{instance_id}

# Or programmatically
from edda.replay import ReplayEngine

engine = ReplayEngine(storage)
await engine.cancel_workflow(instance_id)
```

## Monitoring and Debugging

### Viewer UI

The Edda Viewer shows waiting workflows:

- Status: `waiting_for_event` or `waiting_for_timer`
- Event type being waited for
- Timeout information
- Time elapsed

### Logging

Add logging for debugging:

```python
import logging

logger = logging.getLogger(__name__)

@workflow
async def logged_workflow(ctx: WorkflowContext):
    logger.info(f"Waiting for payment event at step {ctx.current_step}")

    event = await wait_event(ctx, "payment.completed", timeout_seconds=300)

    logger.info(f"Received event: {event.id} with data: {event.data}")
```

## Performance Considerations

### Worker Release

- Waiting workflows don't consume worker resources
- Workers can handle other workflows while waiting
- Scales to thousands of waiting workflows

### Event Matching

- Event type matching is exact (string comparison)
- Consider using hierarchical event types for flexibility
- Index on event_type for performance

### Timer Precision

- Timers checked every 10 seconds by default
- Maximum 10-second delay in timer expiration
- Configure check interval for different precision needs

## Related Topics

- [CloudEvents HTTP Binding](cloudevents-http-binding.md) - CloudEvents integration
- [Workflows and Activities](../workflows-activities.md) - Basic workflow concepts
- [Saga Pattern](../saga-compensation.md) - Compensation and rollback
- [Examples: Event Waiting](../../examples/events.md) - Practical examples
