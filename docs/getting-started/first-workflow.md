# Your First Workflow

In this tutorial, you'll build a complete **order processing workflow** with compensation (Saga pattern). This workflow demonstrates:

- ✅ Activities with Pydantic models
- ✅ Automatic compensation on failure
- ✅ Durable execution with crash recovery
- ✅ Event publishing with transactional outbox

## Prerequisites

Before starting, make sure you have Edda installed:

```bash
# Install Edda from PyPI
uv add edda-framework
```

If you haven't installed uv yet, see the [Installation Guide](installation.md).

## What We're Building

An e-commerce order processing system that:

1. **Reserves inventory** for ordered items
2. **Processes payment** for the order
3. **Ships the order** to the customer
4. **Publishes events** at each step
5. **Automatically rolls back** if any step fails

## Step 1: Define Data Models

Create `order_workflow.py` and start with Pydantic models:

```python
from pydantic import BaseModel, Field

class OrderItem(BaseModel):
    """A single item in an order"""
    product_id: str
    quantity: int = Field(..., ge=1)  # At least 1
    unit_price: float = Field(..., gt=0)  # Positive price

class ShippingAddress(BaseModel):
    """Customer shipping address"""
    street: str
    city: str
    postal_code: str
    country: str

class OrderInput(BaseModel):
    """Input for order processing workflow"""
    order_id: str = Field(..., pattern=r"^ORD-\d+$")  # e.g., ORD-123
    customer_email: str = Field(..., pattern=r"^[^@]+@[^@]+\.[^@]+$")
    items: list[OrderItem]
    shipping_address: ShippingAddress

class OrderResult(BaseModel):
    """Result of order processing"""
    order_id: str
    status: str
    total_amount: float
    confirmation_number: str
```

## Step 2: Create Activities

Add the three main activities with compensation:

```python
from edda import activity, on_failure, compensation, WorkflowContext
from edda.outbox.transactional import send_event_transactional

# Compensation functions
@compensation
async def cancel_inventory_reservation(
    ctx: WorkflowContext,
    order_id: str,
    items: list[OrderItem]
) -> dict:
    """Compensation: Release reserved inventory"""
    print(f"❌ Cancelling inventory reservation for {order_id}")

    await send_event_transactional(
        ctx,
        event_type="inventory.cancelled",
        event_source="order-service",
        event_data={"order_id": order_id}
    )

    return {"cancelled": True}

@compensation
async def refund_payment(
    ctx: WorkflowContext,
    order_id: str,
    amount: float,
    customer_email: str
) -> dict:
    """Compensation: Refund payment"""
    print(f"❌ Refunding payment for {order_id}: ${amount:.2f}")

    await send_event_transactional(
        ctx,
        event_type="payment.refunded",
        event_source="order-service",
        event_data={
            "order_id": order_id,
            "amount": amount
        }
    )

    return {"refunded": True}

# Activities with compensation links
@activity
@on_failure(cancel_inventory_reservation)
async def reserve_inventory(
    ctx: WorkflowContext,
    order_id: str,
    items: list[OrderItem]
) -> dict:
    """Reserve inventory for all items"""
    total = sum(item.quantity * item.unit_price for item in items)

    print(f"📦 Reserving inventory for {order_id}: ${total:.2f}")

    # Publish event
    await send_event_transactional(
        ctx,
        event_type="inventory.reserved",
        event_source="order-service",
        event_data={
            "order_id": order_id,
            "total_amount": total,
            "item_count": len(items)
        }
    )

    return {
        "reservation_id": f"RES-{order_id}",
        "total_amount": total
    }

@activity
@on_failure(refund_payment)
async def process_payment(
    ctx: WorkflowContext,
    order_id: str,
    amount: float,
    customer_email: str
) -> dict:
    """Process customer payment"""
    print(f"💳 Processing payment for {order_id}: ${amount:.2f}")

    # Publish event
    await send_event_transactional(
        ctx,
        event_type="payment.processed",
        event_source="order-service",
        event_data={
            "order_id": order_id,
            "amount": amount,
            "customer_email": customer_email
        }
    )

    return {
        "transaction_id": f"TXN-{order_id}",
        "amount": amount,
        "status": "completed"
    }

# Activity 3: Ship Order
@activity
async def ship_order(
    ctx: WorkflowContext,
    order_id: str,
    address: ShippingAddress
) -> dict:
    """Ship order to customer"""
    print(f"🚚 Shipping {order_id} to {address.city}, {address.country}")

    # Publish event
    await send_event_transactional(
        ctx,
        event_type="order.shipped",
        event_source="order-service",
        event_data={
            "order_id": order_id,
            "destination": f"{address.city}, {address.country}"
        }
    )

    return {
        "tracking_number": f"TRACK-{order_id}",
        "status": "shipped"
    }
```

## Step 3: Create the Workflow

Now orchestrate the activities:

```python
from edda import workflow

@workflow
async def order_processing_workflow(
    ctx: WorkflowContext,
    input: OrderInput
) -> OrderResult:
    """
    Complete order processing workflow with Saga pattern.

    Steps:
    1. Reserve inventory (with cancellation compensation)
    2. Process payment (with refund compensation)
    3. Ship order

    If any step fails, all previous steps are automatically compensated
    in reverse order.
    """

    # Step 1: Reserve inventory
    reservation = await reserve_inventory(
        ctx,
        input.order_id,
        input.items
    )

    # Step 2: Process payment
    payment = await process_payment(
        ctx,
        input.order_id,
        reservation["total_amount"],
        input.customer_email
    )

    # Step 3: Ship order
    shipment = await ship_order(
        ctx,
        input.order_id,
        input.shipping_address
    )

    # Success! Return result
    return OrderResult(
        order_id=input.order_id,
        status="completed",
        total_amount=payment["amount"],
        confirmation_number=shipment["tracking_number"]
    )
```

## Step 4: Run the Workflow

Add the main function:

```python
import asyncio
from edda import EddaApp

async def main():
    # Create Edda app
    app = EddaApp(
        db_url="sqlite:///orders.db",
        service_name="order-service",
        outbox_enabled=True  # Enable transactional outbox
    )

    # Initialize the app (required before starting workflows)
    await app.initialize()

    try:
        # Create order input
        order = OrderInput(
            order_id="ORD-12345",
            customer_email="customer@example.com",
            items=[
                OrderItem(product_id="PROD-1", quantity=2, unit_price=29.99),
                OrderItem(product_id="PROD-2", quantity=1, unit_price=49.99),
            ],
            shipping_address=ShippingAddress(
                street="1-2-3 Dogenzaka",
                city="Shibuya",
                postal_code="150-0001",
                country="Japan"
            )
        )

        # Start workflow
        print("Starting order processing workflow...")
        instance_id = await order_processing_workflow.start(input=order)

        print(f"\n✅ Workflow started: {instance_id}")

        # Get result
        instance = await app.storage.get_instance(instance_id)
        if instance["status"] == "completed":
            result = instance["output_data"]
            print(f"📊 Order completed:")
            print(f"   - Order ID: {result['order_id']}")
            print(f"   - Total: ${result['total_amount']:.2f}")
            print(f"   - Tracking: {result['confirmation_number']}")

    finally:
        await app.shutdown()

if __name__ == "__main__":
    asyncio.run(main())
```

## Step 5: Test Happy Path

Run the workflow:

```bash
uv run python order_workflow.py
```

**Expected output:**

```
Starting order processing workflow...

📦 Reserving inventory for ORD-12345: $109.97
💳 Processing payment for ORD-12345: $109.97
🚚 Shipping ORD-12345 to Shibuya, Japan

✅ Workflow started: <instance_id>
📊 Order completed:
   - Order ID: ORD-12345
   - Total: $109.97
   - Tracking: TRACK-ORD-12345
```

## Step 6: Test Failure & Compensation

Let's simulate a shipping failure to see compensation in action.

Modify `ship_order` to fail:

```python
@activity
async def ship_order(
    ctx: WorkflowContext,
    order_id: str,
    address: ShippingAddress
) -> dict:
    """Ship order to customer"""
    print(f"🚚 Shipping {order_id} to {address.city}, {address.country}")

    # Simulate shipping failure
    raise Exception("Shipping service unavailable!")

    # ... rest of the function
```

Run again:

```bash
uv run python order_workflow.py
```

**Expected output:**

```
📦 Reserving inventory for ORD-12345: $109.97
💳 Processing payment for ORD-12345: $109.97
🚚 Shipping ORD-12345 to Shibuya, Japan
💥 Exception: Shipping service unavailable!

❌ Refunding payment for ORD-12345: $109.97
❌ Cancelling inventory reservation for ORD-12345

Traceback (most recent call last):
  ...
Exception: Shipping service unavailable!
```

**What happened:**

1. Inventory reserved ✅
2. Payment processed ✅
3. Shipping failed ❌
4. **Automatic compensation in reverse order:**
   - Refund payment ✅
   - Cancel inventory reservation ✅

This is the **Saga pattern** - distributed rollback through compensation functions.

## Step 7: Understanding Crash Recovery

Edda's durable execution ensures workflows survive crashes through **deterministic replay**. When a workflow crashes mid-execution:

1. ✅ **Activity results are saved** to the database before execution continues
2. ✅ **Workflow state is preserved** (current step, history, locks)
3. ✅ **Automatic recovery** detects and resumes stale workflows

### How Automatic Recovery Works

In production environments with long-running EddaApp instances (e.g., FastAPI/uvicorn servers):

- **Crash detection**: Edda's background task checks for stale locks every 60 seconds
- **Auto-resume**: Crashed workflows are automatically resumed when their lock timeout expires
  - Both normal execution and rollback execution are automatically resumed
  - Default timeout: 5 minutes (300 seconds)
  - Customizable at 3 levels: runtime (`start(lock_timeout_seconds=X)`), decorator (`@workflow(lock_timeout_seconds=Y)`), or global default
  - See [Lock Timeout Customization](../core-features/workflows-activities.md#lock-timeout-customization) for details
  - Workflows resume from their last checkpoint using deterministic replay
- **Deterministic replay**: Previously executed activities return cached results from history
- **Resume from checkpoint**: Only remaining activities execute fresh

### Workflows Waiting for Events or Timers

Workflows in special waiting states are handled differently:

- **Waiting for Events**: Resumed immediately when the awaited event arrives (not on a fixed schedule)
- **Waiting for Timers**: Checked every 10 seconds and resumed when the timer expires
- These workflows are **not** included in the 60-second crash recovery cycle

### Crash Recovery in Action

**Production scenario**:

```python
# Server starts and runs continuously
app = EddaApp(service_name="order-service", db_url="sqlite:///orders.db")
await app.initialize()

# Workflow starts executing
instance_id = await order_processing_workflow.start(input=order)

# Server crashes after payment step
# → inventory reservation: ✅ saved
# → payment: ✅ saved
# → shipping: ❌ not executed

# Server restarts (automatic or manual)
# → Edda's background task detects stale workflow (lock > 5 minutes)
# → Automatically resumes workflow from last checkpoint
# → inventory reservation: ⚡ replayed from history (instant)
# → payment: ⚡ replayed from history (instant)
# → shipping: 🚚 executes fresh
```

### Why Activities Execute Exactly Once

Edda's replay mechanism ensures idempotency:

1. **Before execution**: Check if result exists in history for current step
2. **If found**: Return cached result (replay)
3. **If not found**: Execute activity and save result to history
4. **Side effects**: External API calls, payments, etc. happen exactly once

**Example**:

```python
@activity
async def process_payment(ctx: WorkflowContext, order_id: str, amount: float):
    # This code executes ONCE per workflow instance
    # On crash recovery, cached result is returned
    print(f"💳 Processing payment for {order_id}: ${amount:.2f}")

    payment_result = await external_payment_api.charge(amount)

    return {
        "transaction_id": payment_result.id,
        "amount": amount,
        "status": "completed"
    }
```

**On first execution**:

- Code executes
- External payment API is called
- Result saved to database
- Output: `💳 Processing payment for ORD-12345: $109.97`

**On crash recovery (replay)**:

- Code does NOT execute
- Result loaded from database
- External payment API is NOT called again
- No output (instant return)

### Testing Crash Recovery

For a full demonstration, you would need:

1. Long-running EddaApp instance (e.g., uvicorn server)
2. Workflow that crashes mid-execution
3. Wait 5+ minutes for automatic recovery
4. Observe workflow resume from last checkpoint

**Note**: Running the same script twice creates **separate workflow instances** with different UUIDs. To test replay on the **same instance**, you need a persistent server and workflow resumption logic.

## What You've Learned

- ✅ **Pydantic Models**: Type-safe inputs and outputs
- ✅ **Activities**: Business logic units with automatic history recording
- ✅ **Compensation**: Automatic rollback with `@on_failure`
- ✅ **Saga Pattern**: Distributed transaction management
- ✅ **Durable Execution**: Workflows survive crashes
- ✅ **Transactional Outbox**: Reliable event publishing
- ✅ **Deterministic Replay**: Activities execute exactly once

## Next Steps

- **[Saga Pattern](../core-features/saga-compensation.md)**: Deep dive into compensation
- **[Event Handling](../core-features/events/wait-event.md)**: Wait for external events
- **[Transactional Outbox](../core-features/transactional-outbox.md)**: Reliable event publishing
- **[Examples](../examples/ecommerce.md)**: More real-world examples
- **[Viewer UI](../viewer-ui/setup.md)**: Visualize your workflows
