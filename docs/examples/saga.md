# Saga Pattern with Compensation

This example demonstrates automatic compensation (rollback) when a workflow fails.

## What This Example Shows

- ✅ `@on_failure` decorator for compensation functions
- ✅ Automatic reverse-order compensation
- ✅ Saga pattern for distributed transactions
- ✅ Rollback on workflow failure

## The Problem

In distributed systems, you can't use traditional database transactions. The **Saga pattern** solves this with **compensation functions** that undo completed steps.

## Code Overview

### Define Activities with Compensation

```python
from edda import activity, on_failure, compensation, WorkflowContext

# Define compensation functions
@compensation
async def cancel_inventory_reservation(
    ctx: WorkflowContext,
    order_id: str,
    item_id: str
):
    """Compensation: Release reserved inventory."""
    print(f"❌ Cancelled reservation for {item_id}")
    return {"cancelled": True}

@compensation
async def refund_payment(ctx: WorkflowContext, order_id: str, amount: float):
    """Compensation: Refund payment."""
    print(f"❌ Refunded ${amount}")
    return {"refunded": True}

# Define activities with compensation links
@activity
@on_failure(cancel_inventory_reservation)
async def reserve_inventory(ctx: WorkflowContext, order_id: str, item_id: str):
    """Reserve inventory for an item."""
    print(f"✅ Reserved {item_id} for order {order_id}")
    return {"reservation_id": f"RES-{item_id}", "item_id": item_id}

@activity
@on_failure(refund_payment)
async def charge_payment(ctx: WorkflowContext, order_id: str, amount: float):
    """Charge customer payment."""
    print(f"✅ Charged ${amount} for order {order_id}")
    return {"transaction_id": f"TXN-{order_id}", "amount": amount}

@activity
async def ship_order(ctx: WorkflowContext, order_id: str):
    """Ship the order (this will fail in our example)."""
    print(f"🚚 Attempting to ship order {order_id}")
    raise Exception("Shipping service unavailable!")
```

### Define Saga Workflow

```python
from edda import workflow

@workflow
async def order_saga(ctx: WorkflowContext, order_id: str):
    """
    Order processing workflow with automatic compensation.

    If any step fails, Edda automatically calls compensation functions
    for all completed steps in reverse order.

    Note: Activity IDs are auto-generated for sequential execution.
    """

    # Step 1: Reserve inventory (auto-generated ID: "reserve_inventory:1")
    await reserve_inventory(ctx, order_id, "ITEM-123")

    # Step 2: Charge payment (auto-generated ID: "charge_payment:1")
    await charge_payment(ctx, order_id, 99.99)

    # Step 3: Ship order (will fail!) (auto-generated ID: "ship_order:1")
    await ship_order(ctx, order_id)

    return {"status": "completed"}
```

## Expected Output

```
✅ Reserved ITEM-123 for order ORD-001
✅ Charged $99.99 for order ORD-001
🚚 Attempting to ship order ORD-001
💥 Exception: Shipping service unavailable!

Automatic compensation (reverse order):
❌ Refunded $99.99
❌ Cancelled reservation for ITEM-123

Workflow failed with compensation completed.
```

## How It Works

1. **Step 1 completes**: Inventory reserved ✅
2. **Step 2 completes**: Payment charged ✅
3. **Step 3 fails**: Shipping fails ❌
4. **Automatic compensation** (reverse order):
   - First: Refund payment (Step 2 compensation)
   - Then: Cancel reservation (Step 1 compensation)

## Key Rules

### 1. Reverse Order Execution

Compensation functions run in **reverse order** of activity execution:

```
Activities:      reserve → charge → ship (fails)
Compensations:   cancel ← refund
```

### 2. Only Completed Activities

Only **successfully completed** activities are compensated:

```python
await reserve_inventory(ctx, ...)  # ✅ Completed → Will be compensated
await charge_payment(ctx, ...)     # ✅ Completed → Will be compensated
await ship_order(ctx, ...)         # ❌ Failed → No compensation needed
```

### 3. Automatic Trigger

No manual compensation trigger required - Edda handles it automatically on workflow failure.

## Real-World Use Cases

- **E-commerce**: Reserve inventory → Charge payment → Ship order
- **Hotel Booking**: Reserve room → Charge deposit → Send confirmation
- **Travel**: Book flight → Book hotel → Rent car
- **Financial**: Transfer funds → Update ledger → Send receipt

## Running the Example

Create a file named `compensation_workflow.py` with the activities and workflow shown above, then run:

```bash
# Install Edda if you haven't already
uv add edda-framework

# Run your workflow
uv run python compensation_workflow.py
```

## Complete Code

See a reference implementation in [examples/compensation_workflow.py](https://github.com/i2y/edda/blob/main/examples/compensation_workflow.py) in the Edda repository.

## What You Learned

- ✅ **`@on_failure` Decorator**: Links compensation to activities
- ✅ **Automatic Execution**: Edda handles compensation automatically
- ✅ **Reverse Order**: Compensations run in reverse order
- ✅ **Saga Pattern**: Distributed transaction management without 2PC

## Next Steps

- **[Event Waiting](events.md)**: Wait for external events
- **[Core Concepts](../getting-started/concepts.md)**: Deep dive into Saga pattern
