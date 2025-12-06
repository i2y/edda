"""
Retry + Compensation Example

Demonstrates:
- Activities with automatic retry
- Compensation functions for rollback
- Retry exhaustion triggering compensation
- TerminalError vs retryable errors in Saga workflows

Scenario: E-commerce order processing workflow
1. Reserve inventory (retries on failure, compensates on rollback)
2. Process payment (retries on failure, compensates on rollback)
3. Ship order (may fail after retries, triggering compensation)

Run:
    python examples/retry_with_compensation.py
"""

import asyncio

from edda import (
    EddaApp,
    RetryExhaustedError,
    RetryPolicy,
    TerminalError,
    WorkflowContext,
    activity,
    compensation,
    on_failure,
    workflow,
)

# ============================================================================
# Simulated External Services (with failure simulation)
# ============================================================================

inventory_service = {"SKU-123": 10, "SKU-456": 5}  # Available stock
reserved_items = {}  # Track reservations
payment_records = {}  # Track payments
shipment_records = {}  # Track shipments

# Counters for demo purposes
reserve_attempts = 0
payment_attempts = 0
ship_attempts = 0


# ============================================================================
# Activities with Retry + Compensation
# ============================================================================


@compensation
async def cancel_inventory_reservation(
    _ctx: WorkflowContext, order_id: str, sku: str, quantity: int
):
    """Compensation: Cancel inventory reservation."""
    print(f"🔙 [COMPENSATION] Cancelling reservation for order {order_id}")
    print(f"   Returning {quantity}x {sku} to inventory")

    # Return items to inventory
    inventory_service[sku] += quantity
    if order_id in reserved_items:
        del reserved_items[order_id]

    return {"cancelled": True, "order_id": order_id}


@activity(retry_policy=RetryPolicy(max_attempts=3, initial_interval=0.5))
@on_failure(cancel_inventory_reservation)
async def reserve_inventory(
    _ctx: WorkflowContext, order_id: str, sku: str, quantity: int
) -> dict:
    """
    Reserve inventory for an order.

    Retry policy: 3 attempts with 0.5s initial delay
    Compensation: cancel_inventory_reservation
    """
    global reserve_attempts
    reserve_attempts += 1

    print(f"📦 [ACTIVITY] reserve_inventory - Attempt #{reserve_attempts}")
    print(f"   Order: {order_id}, SKU: {sku}, Quantity: {quantity}")

    # Simulate transient failure (succeeds on 2nd attempt)
    if reserve_attempts == 1:
        print("   ❌ Database connection timeout (retrying...)")
        raise ConnectionError("Database connection timeout")

    # Check inventory
    available = inventory_service.get(sku, 0)
    if available < quantity:
        # Permanent error - don't retry
        print(f"   ❌ Insufficient inventory (available: {available})")
        raise TerminalError(f"Insufficient inventory for {sku} (need {quantity}, have {available})")

    # Reserve inventory
    inventory_service[sku] -= quantity
    reserved_items[order_id] = {"sku": sku, "quantity": quantity}

    print(f"   ✅ Reserved {quantity}x {sku} for order {order_id}")
    print(f"   Remaining inventory: {inventory_service[sku]}")

    return {"order_id": order_id, "sku": sku, "quantity": quantity, "reserved": True}


@compensation
async def refund_payment(_ctx: WorkflowContext, order_id: str, amount: float):
    """Compensation: Refund payment."""
    print(f"🔙 [COMPENSATION] Refunding payment for order {order_id}")
    print(f"   Amount: ${amount:.2f}")

    # Mark payment as refunded
    if order_id in payment_records:
        payment_records[order_id]["status"] = "refunded"

    return {"refunded": True, "order_id": order_id, "amount": amount}


@activity(retry_policy=RetryPolicy(max_attempts=5, initial_interval=1.0))
@on_failure(refund_payment)
async def process_payment(_ctx: WorkflowContext, order_id: str, amount: float) -> dict:
    """
    Process payment for an order.

    Retry policy: 5 attempts with 1s initial delay (more critical)
    Compensation: refund_payment
    """
    global payment_attempts
    payment_attempts += 1

    print(f"💳 [ACTIVITY] process_payment - Attempt #{payment_attempts}")
    print(f"   Order: {order_id}, Amount: ${amount:.2f}")

    # Simulate transient failure (succeeds on 3rd attempt)
    if payment_attempts < 3:
        print("   ❌ Payment gateway timeout (retrying...)")
        raise ConnectionError("Payment gateway timeout")

    # Process payment
    transaction_id = f"TXN-{order_id}-{payment_attempts}"
    payment_records[order_id] = {
        "transaction_id": transaction_id,
        "amount": amount,
        "status": "completed",
    }

    print("   ✅ Payment processed successfully")
    print(f"   Transaction ID: {transaction_id}")

    return {"order_id": order_id, "transaction_id": transaction_id, "amount": amount}


@compensation
async def cancel_shipment(_ctx: WorkflowContext, order_id: str, tracking_number: str):
    """Compensation: Cancel shipment."""
    print(f"🔙 [COMPENSATION] Cancelling shipment for order {order_id}")
    print(f"   Tracking: {tracking_number}")

    # Cancel shipment
    if order_id in shipment_records:
        shipment_records[order_id]["status"] = "cancelled"

    return {"cancelled": True, "order_id": order_id}


@activity(retry_policy=RetryPolicy(max_attempts=3, initial_interval=1.0))
@on_failure(cancel_shipment)
async def ship_order(_ctx: WorkflowContext, order_id: str) -> dict:
    """
    Ship an order.

    Retry policy: 3 attempts with 1s initial delay
    Compensation: cancel_shipment
    """
    global ship_attempts
    ship_attempts += 1

    print(f"🚚 [ACTIVITY] ship_order - Attempt #{ship_attempts}")
    print(f"   Order: {order_id}")

    # Simulate persistent failure (always fails - for demo)
    print("   ❌ Shipping service unavailable (retrying...)")
    raise ConnectionError("Shipping service unavailable")

    # This code never executes (demo purposes)
    tracking_number = f"TRACK-{order_id}"
    shipment_records[order_id] = {"tracking_number": tracking_number, "status": "shipped"}
    print("   ✅ Order shipped successfully")
    print(f"   Tracking: {tracking_number}")
    return {"order_id": order_id, "tracking_number": tracking_number}


# ============================================================================
# Workflows
# ============================================================================


@workflow
async def successful_order_workflow(ctx: WorkflowContext, order_id: str) -> dict:
    """
    Successful order workflow - all steps succeed.

    Demonstrates:
    - Activities retry and succeed
    - No compensation needed
    - Retry metadata in history
    """
    print("\n" + "=" * 70)
    print(f"🛒 Starting SUCCESSFUL order workflow: {order_id}")
    print("=" * 70)

    # Step 1: Reserve inventory (retries once, then succeeds)
    inventory = await reserve_inventory(ctx, order_id, sku="SKU-123", quantity=2)

    # Step 2: Process payment (retries twice, then succeeds)
    payment = await process_payment(ctx, order_id, amount=99.99)

    # Note: We skip ship_order in successful workflow to avoid failure

    print("\n✅ Order completed successfully!")
    print(f"   Inventory reserved: {inventory}")
    print(f"   Payment: {payment}")

    return {
        "status": "completed",
        "order_id": order_id,
        "inventory": inventory,
        "payment": payment,
    }


@workflow
async def failed_order_workflow(ctx: WorkflowContext, order_id: str) -> dict:
    """
    Failed order workflow - final step exhausts retries.

    Demonstrates:
    - Activities retry and fail
    - Compensation runs in reverse order
    - RetryExhaustedError handling
    """
    print("\n" + "=" * 70)
    print(f"🛒 Starting FAILED order workflow: {order_id}")
    print("   (will fail at shipping step and trigger compensation)")
    print("=" * 70)

    try:
        # Step 1: Reserve inventory (retries once, then succeeds)
        inventory = await reserve_inventory(ctx, order_id, sku="SKU-456", quantity=1)

        # Step 2: Process payment (retries twice, then succeeds)
        payment = await process_payment(ctx, order_id, amount=49.99)

        # Step 3: Ship order (retries 3 times, then fails)
        # This will exhaust retries and raise RetryExhaustedError
        shipment = await ship_order(ctx, order_id)

        # This code never executes
        return {
            "status": "completed",
            "order_id": order_id,
            "inventory": inventory,
            "payment": payment,
            "shipment": shipment,
        }

    except RetryExhaustedError as e:
        print(f"\n⚠️ RetryExhaustedError: {e}")
        print(f"   Original error: {e.__cause__}")
        print("\n🔄 Triggering compensation (Saga rollback)...")
        print("   Expected order:")
        print("   1. Cancel shipment (step 3 compensation)")
        print("   2. Refund payment (step 2 compensation)")
        print("   3. Cancel inventory reservation (step 1 compensation)\n")

        # Re-raise to trigger compensation
        raise


@workflow
async def terminal_error_workflow(ctx: WorkflowContext, order_id: str) -> dict:
    """
    Workflow with TerminalError - no retry, compensation runs.

    Demonstrates:
    - TerminalError doesn't retry
    - Compensation still runs
    - Faster failure (no wasted retry attempts)
    """
    print("\n" + "=" * 70)
    print(f"🛒 Starting TERMINAL ERROR workflow: {order_id}")
    print("   (will hit terminal error - no retry)")
    print("=" * 70)

    try:
        # Step 1: Reserve inventory for non-existent SKU
        # This will raise TerminalError immediately (no retry)
        _ = await reserve_inventory(ctx, order_id, sku="INVALID-SKU", quantity=100)

        # This code never executes
        _ = await process_payment(ctx, order_id, amount=199.99)

        return {"status": "completed", "order_id": order_id}

    except TerminalError as e:
        print(f"\n⚠️ TerminalError: {e}")
        print("   No retry attempted (permanent error)")
        print("\n🔄 Triggering compensation...")

        # Re-raise to trigger compensation
        raise


# ============================================================================
# Main Application
# ============================================================================


async def main():
    """Run all retry + compensation examples."""
    print("=" * 70)
    print("🔄 Edda Retry + Compensation Examples")
    print("=" * 70)

    # Initialize Edda
    app = EddaApp(
        service_name="retry-compensation-demo", db_url="sqlite:///retry_compensation.db"
    )
    await app.initialize()

    try:
        # ====================================================================
        # Example 1: Successful Workflow (retries succeed, no compensation)
        # ====================================================================
        global reserve_attempts, payment_attempts, ship_attempts
        reserve_attempts = 0
        payment_attempts = 0
        ship_attempts = 0

        _ = await successful_order_workflow.start(order_id="ORDER-SUCCESS-001")
        await asyncio.sleep(2)  # Wait for workflow to complete

        # ====================================================================
        # Example 2: Failed Workflow (retries exhausted, compensation runs)
        # ====================================================================
        reserve_attempts = 0
        payment_attempts = 0
        ship_attempts = 0

        try:
            _ = await failed_order_workflow.start(order_id="ORDER-FAIL-001")
            await asyncio.sleep(5)  # Wait for retries and compensation
        except Exception as e:
            print(f"\n✅ Workflow failed as expected: {type(e).__name__}")

        # ====================================================================
        # Example 3: Terminal Error (no retry, compensation runs)
        # ====================================================================
        reserve_attempts = 0
        payment_attempts = 0
        ship_attempts = 0

        try:
            _ = await terminal_error_workflow.start(order_id="ORDER-TERMINAL-001")
            await asyncio.sleep(1)  # Wait for workflow to complete
        except Exception as e:
            print(f"\n✅ Workflow failed as expected: {type(e).__name__}")

        print("\n" + "=" * 70)
        print("✅ All examples completed!")
        print("=" * 70)
        print(
            "\n💡 Key Takeaways:"
            "\n   1. Activities retry automatically (default: 5 attempts)"
            "\n   2. Custom retry policies can be set per activity"
            "\n   3. Compensation runs in reverse order on failure"
            "\n   4. TerminalError skips retry (for permanent errors)"
            "\n   5. RetryExhaustedError triggers compensation"
            "\n   6. Retry metadata is recorded in workflow history"
        )
        print(
            "\n💡 View workflow history in Viewer UI:"
            "\n   python viewer_app.py"
            "\n   http://localhost:8080"
        )

        # Show final state
        print("\n📊 Final State:")
        print(f"   Inventory: {inventory_service}")
        print(f"   Reserved Items: {reserved_items}")
        print(f"   Payment Records: {payment_records}")
        print(f"   Shipment Records: {shipment_records}")

    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback

        traceback.print_exc()


if __name__ == "__main__":
    asyncio.run(main())
