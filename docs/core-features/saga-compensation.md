# Saga Pattern and Compensation

The Saga pattern is a core feature of Edda that enables distributed transaction management across multiple services and activities. It provides automatic compensation (rollback) when workflows fail, ensuring data consistency without requiring traditional database transactions.

## What is the Saga Pattern?

The Saga pattern is a design pattern for managing distributed transactions by breaking them into a series of local transactions. Each local transaction updates data within a single service and publishes an event or message. If a local transaction fails, a series of compensating transactions are executed to undo the changes made by previous transactions.

## Key Concepts

### Compensation Functions

Compensation functions are special activities that undo the effects of previously executed activities. They run automatically when a workflow fails, executing in reverse order of the original activities.

### The `@on_failure` Decorator

The `@on_failure` decorator links a compensation function to its corresponding activity:

```python
from edda import activity, on_failure, compensation, WorkflowContext

@compensation
async def cancel_inventory_reservation(ctx: WorkflowContext, order_id: str, item_id: str):
    """Compensation: Release reserved inventory."""
    # Cancel reservation logic
    return {"cancelled": True}

@activity
@on_failure(cancel_inventory_reservation)
async def reserve_inventory(ctx: WorkflowContext, order_id: str, item_id: str):
    """Reserve inventory for an item."""
    # Reserve inventory logic
    return {"reservation_id": f"RES-{item_id}"}
```

### The `@compensation` Decorator

The `@compensation` decorator marks a function as a compensation activity. This is required for all compensation functions:

```python
@compensation
async def refund_payment(ctx: WorkflowContext, order_id: str, amount: float):
    """Refund a payment."""
    # Refund logic
    return {"refunded": True}

@activity
@on_failure(refund_payment)
async def charge_payment(ctx: WorkflowContext, order_id: str, amount: float):
    """Charge customer payment."""
    # Payment logic
    return {"transaction_id": f"TXN-{order_id}"}
```

## How Compensation Works

### Automatic Triggering

When a workflow fails (raises an exception), Edda automatically:

1. Stops workflow execution
2. Identifies all successfully completed activities
3. Executes their compensation functions in reverse order
4. Marks the workflow as failed with compensation completed

### Execution Order

Compensation functions always execute in **reverse order** of activity execution:

```python
@workflow
async def order_saga(ctx: WorkflowContext, order_id: str):
    await reserve_inventory(ctx, order_id, "ITEM-123")  # Step 1
    await charge_payment(ctx, order_id, 99.99)          # Step 2
    await ship_order(ctx, order_id)                     # Step 3 (fails)

    # On failure, compensations run as:
    # 1. Compensation for Step 2 (refund payment)
    # 2. Compensation for Step 1 (cancel reservation)
    # Step 3 has no compensation as it failed
```

### Partial Completion Handling

Only **successfully completed** activities are compensated:

- ✅ Completed activities → Compensation executed
- ❌ Failed activities → No compensation needed
- ⏭️ Not-yet-executed activities → No compensation needed

## Implementation Example

### Complete Order Processing Saga

```python
from edda import activity, workflow, on_failure, compensation, WorkflowContext
from edda.app import EddaApp

# Define compensation functions

@compensation
async def cancel_inventory_reservation(ctx: WorkflowContext, order_id: str, items: list[dict]):
    """Release reserved inventory."""
    print(f"Cancelled inventory reservations for order {order_id}")
    return {"cancelled": True}

@compensation
async def refund_payment(ctx: WorkflowContext, order_id: str, amount: float, card_token: str):
    """Refund payment to customer."""
    print(f"Refunded ${amount} for order {order_id}")
    return {"refund_id": f"REF-{order_id}"}

# Define activities with compensation links

@activity
@on_failure(cancel_inventory_reservation)
async def reserve_inventory(ctx: WorkflowContext, order_id: str, items: list[dict]):
    """Reserve inventory for order items."""
    print(f"Reserved inventory for order {order_id}")
    return {"reservation_ids": [f"RES-{item['id']}" for item in items]}

@activity
@on_failure(refund_payment)
async def charge_payment(ctx: WorkflowContext, order_id: str, amount: float, card_token: str):
    """Charge customer payment."""
    print(f"Charged ${amount} for order {order_id}")
    return {"transaction_id": f"TXN-{order_id}", "amount": amount}

@activity
async def create_shipment(ctx: WorkflowContext, order_id: str, address: dict):
    """Create shipment for order."""
    print(f"Creating shipment for order {order_id}")
    # This might fail if shipping service is unavailable
    if "invalid" in address.get("street", ""):
        raise ValueError("Invalid shipping address")
    return {"shipment_id": f"SHIP-{order_id}"}

# Define the saga workflow

@workflow
async def order_processing_saga(
    ctx: WorkflowContext,
    order_id: str,
    items: list[dict],
    amount: float,
    card_token: str,
    shipping_address: dict
):
    """
    Complete order processing workflow with automatic compensation.

    If any step fails, all completed steps are automatically
    compensated in reverse order.
    """

    # Reserve inventory
    reservation = await reserve_inventory(ctx, order_id, items)

    # Charge payment
    payment = await charge_payment(ctx, order_id, amount, card_token)

    # Create shipment (might fail)
    shipment = await create_shipment(ctx, order_id, shipping_address)

    return {
        "order_id": order_id,
        "reservation": reservation,
        "payment": payment,
        "shipment": shipment
    }

# Application setup

app = EddaApp()
app.initialize()

if __name__ == "__main__":
    import asyncio
    import uvloop

    async def test_saga():
        # This will succeed
        success_result = await order_processing_saga.start(
            order_id="ORD-001",
            items=[{"id": "ITEM-1", "qty": 2}],
            amount=99.99,
            card_token="tok_valid",
            shipping_address={"street": "123 Main St", "city": "Springfield"}
        )
        print(f"Success: {success_result}")

        # This will fail and trigger compensation
        try:
            failed_result = await order_processing_saga.start(
                order_id="ORD-002",
                items=[{"id": "ITEM-2", "qty": 1}],
                amount=49.99,
                card_token="tok_valid",
                shipping_address={"street": "invalid address", "city": "Unknown"}
            )
        except Exception as e:
            print(f"Failed with compensation: {e}")

    asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
    asyncio.run(test_saga())
```

## Best Practices

### 1. Idempotent Compensations

Make compensation functions idempotent - they should handle being called multiple times safely:

```python
@compensation
async def refund_payment(ctx: WorkflowContext, order_id: str, amount: float):
    # Check if already refunded
    if await is_already_refunded(order_id):
        return {"status": "already_refunded"}

    # Perform refund
    refund_id = await process_refund(order_id, amount)
    return {"refund_id": refund_id}

@activity
@on_failure(refund_payment)
async def charge_payment(ctx: WorkflowContext, order_id: str, amount: float):
    # Charge customer
    transaction_id = await process_payment(order_id, amount)
    return {"transaction_id": transaction_id}
```

### 2. Store Compensation Data

Activities should return data needed for compensation:

```python
@activity
async def reserve_inventory(ctx: WorkflowContext, order_id: str, items: list):
    reservation_ids = []
    for item in items:
        res_id = await reserve_item(item["id"], item["quantity"])
        reservation_ids.append(res_id)

    # Return data needed for compensation
    return {"reservation_ids": reservation_ids, "items": items}
```

### 3. Handle Partial State

Consider partial completion within activities:

```python
@activity
async def reserve_multiple_items(ctx: WorkflowContext, items: list):
    reserved = []
    try:
        for item in items:
            res_id = await reserve_item(item)
            reserved.append(res_id)
    except Exception as e:
        # Manually compensate partial reservations
        for res_id in reserved:
            await cancel_reservation(res_id)
        raise

    return {"all_reserved": reserved}
```

### 4. Timeout Handling

Set appropriate timeouts for compensation functions:

```python
@compensation
async def compensate_long_running(ctx: WorkflowContext, data: dict):
    # Add timeout to prevent compensation from hanging
    try:
        async with asyncio.timeout(30):  # 30 second timeout
            await perform_compensation(data)
    except asyncio.TimeoutError:
        # Log and handle timeout
        await log_compensation_failure(data)
        raise

@activity
@on_failure(compensate_long_running)
async def long_running_activity(ctx: WorkflowContext, data: dict):
    # Perform long-running operation
    result = await perform_long_operation(data)
    return result
```

## Advanced Features

### Conditional Compensation

You can conditionally execute compensation based on activity results:

```python
@compensation
async def conditional_compensation(ctx: WorkflowContext, should_compensate: bool):
    if not should_compensate:
        return {"skipped": True}

    await perform_cleanup()
    return {"compensated": True}

@activity
@on_failure(conditional_compensation)
async def conditional_activity(ctx: WorkflowContext, should_compensate: bool):
    result = await perform_action()
    result["needs_compensation"] = should_compensate
    return result
```

### Nested Sagas

Sagas can call other sagas, with compensation cascading through the hierarchy:

```python
@workflow
async def parent_saga(ctx: WorkflowContext, order_data: dict):
    # If child saga fails, its compensations run first
    await child_saga(ctx, order_data["child_data"])

    # Then parent activities
    await parent_activity(ctx, order_data["parent_data"])
```

### Manual Compensation Trigger

While Edda handles automatic compensation, you can also manually trigger compensation:

```python
@workflow
async def manual_compensation_saga(ctx: WorkflowContext, data: dict):
    try:
        result = await risky_activity(ctx, data)

        if result.get("needs_rollback"):
            # Manually trigger compensation
            raise Exception("Manual rollback triggered")

    except Exception as e:
        # Compensation will run automatically
        raise
```

## Common Use Cases

### E-commerce Order Processing
- Reserve inventory → Charge payment → Create shipment → Send confirmation
- On failure: Cancel shipment → Refund payment → Release inventory

### Travel Booking
- Book flight → Reserve hotel → Rent car → Process payment
- On failure: Cancel car → Cancel hotel → Cancel flight → Refund payment

### Financial Transactions
- Lock source account → Lock target account → Transfer funds → Update ledgers
- On failure: Reverse ledgers → Unlock accounts → Restore balances

### Microservices Orchestration
- Call Service A → Call Service B → Call Service C → Aggregate results
- On failure: Compensate C → Compensate B → Compensate A

## Monitoring and Debugging

### View Compensation in Viewer UI

The Edda Viewer UI shows compensation execution:

1. Failed workflows display compensation status
2. Compensation activities are marked with special indicators
3. Execution order and timing are visualized

### Logging Compensation

Add detailed logging to compensation functions:

```python
@compensation
async def compensate_critical(ctx: WorkflowContext, data: dict):
    import logging
    logger = logging.getLogger(__name__)

    logger.info(f"Starting compensation for {data['id']}")
    try:
        result = await perform_compensation(data)
        logger.info(f"Compensation successful: {result}")
        return result
    except Exception as e:
        logger.error(f"Compensation failed: {e}")
        raise

@activity
@on_failure(compensate_critical)
async def critical_activity(ctx: WorkflowContext, data: dict):
    # Perform critical operation
    result = await perform_critical_operation(data)
    return result
```

## Related Topics

- [Workflows and Activities](workflows-activities.md) - Learn about basic workflow concepts
- [Durable Execution](durable-execution/replay.md) - Understand replay and recovery
- [Transactional Outbox](transactional-outbox.md) - Ensure message delivery consistency
- [Examples: Saga Pattern](../examples/saga.md) - See practical examples
