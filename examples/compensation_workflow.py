"""
Example workflow demonstrating compensation (Saga pattern).

This example shows how to use register_compensation to implement
automatic rollback when a workflow fails.
"""

import asyncio

from edda import (
    EddaApp,
    WorkflowContext,
    activity,
    compensation,
    register_compensation,
    workflow,
)


# Define activities with compensation
@activity
async def reserve_inventory(_ctx: WorkflowContext, order_id: str, items: list[str]) -> dict:
    """Reserve inventory for an order."""
    print(f"[Activity] Reserving inventory for order: {order_id}")
    print(f"[Activity] Items: {items}")

    reservation_id = f"reservation-{order_id}"

    # Simulate reservation
    reserved_items = {item: f"reserved-{item}" for item in items}

    return {
        "reservation_id": reservation_id,
        "reserved_items": reserved_items,
    }


@compensation
async def release_inventory(_ctx: WorkflowContext, reservation_id: str) -> None:
    """Release inventory reservation (compensation)."""
    print(f"[Compensation] Releasing inventory reservation: {reservation_id}")
    # In a real system, this would cancel the reservation


@activity
async def charge_payment(_ctx: WorkflowContext, order_id: str, amount: float) -> dict:
    """Charge payment for an order."""
    print(f"[Activity] Charging payment for order: {order_id}")
    print(f"[Activity] Amount: ${amount}")

    payment_id = f"payment-{order_id}"

    return {
        "payment_id": payment_id,
        "amount": amount,
        "status": "charged",
    }


@compensation
async def refund_payment(_ctx: WorkflowContext, payment_id: str, amount: float) -> None:
    """Refund payment (compensation)."""
    print(f"[Compensation] Refunding payment: {payment_id}, amount: ${amount}")
    # In a real system, this would process the refund


@activity
async def ship_order(_ctx: WorkflowContext, order_id: str) -> dict:
    """Ship the order - this will fail to trigger compensation."""
    print(f"[Activity] Shipping order: {order_id}")

    # Simulate a failure during shipping
    raise RuntimeError(f"Shipping service unavailable for order {order_id}")


# Define saga workflow with compensation
@workflow
async def order_workflow_with_compensation(
    ctx: WorkflowContext,
    order_id: str,
    items: list[str],
    amount: float,
) -> dict:
    """
    Order workflow with automatic compensation on failure.

    Note: Edda automatically generates activity IDs for sequential execution.

    This workflow demonstrates:
    1. Reserving inventory (with compensation to release)
    2. Charging payment (with compensation to refund)
    3. Failing during shipping
    4. Automatic rollback of all completed steps
    """
    print(f"\n[Workflow] Starting order workflow: {order_id}")

    # Step 1: Reserve inventory (Activity ID auto-generated: "reserve_inventory:1")
    reservation_result = await reserve_inventory(ctx, order_id, items)
    print(f"[Workflow] Inventory reserved: {reservation_result}")

    # Register compensation to release inventory if workflow fails
    await register_compensation(
        ctx,
        release_inventory,
        reservation_id=reservation_result["reservation_id"],
    )

    # Step 2: Charge payment (Activity ID auto-generated: "charge_payment:1")
    payment_result = await charge_payment(ctx, order_id, amount)
    print(f"[Workflow] Payment charged: {payment_result}")

    # Register compensation to refund payment if workflow fails
    await register_compensation(
        ctx,
        refund_payment,
        payment_id=payment_result["payment_id"],
        amount=amount,
    )

    # Step 3: Ship order (this will fail) (Activity ID auto-generated: "ship_order:1")
    try:
        shipping_result = await ship_order(ctx, order_id)
        print(f"[Workflow] Order shipped: {shipping_result}")
    except RuntimeError as e:
        print(f"\n[Workflow] Shipping failed: {e}")
        print("[Workflow] Triggering automatic compensation (rollback)...")
        raise  # Re-raise to trigger compensation

    return {
        "order_id": order_id,
        "status": "completed",
        "reservation": reservation_result,
        "payment": payment_result,
    }


async def main():
    """Run the compensation workflow example."""
    # Initialize Kairo app
    app = EddaApp(
        service_name="order-service",
        db_url="sqlite:///demo.db",
    )

    await app.initialize()

    print("=" * 60)
    print("Compensation Workflow Example")
    print("=" * 60)

    # Start the workflow (it will fail and trigger compensation)
    try:
        instance_id = await order_workflow_with_compensation.start(
            order_id="ORDER-67890",
            items=["item-1", "item-2", "item-3"],
            amount=149.99,
        )
        print(f"\n[Main] Workflow completed: {instance_id}")
    except RuntimeError as e:
        print(f"\n[Main] Workflow failed as expected: {e}")
        print("[Main] Compensations were executed automatically")
        print("\n[Main] The workflow performed rollback in LIFO order:")
        print("  1. Refunded payment (last compensation registered)")
        print("  2. Released inventory (first compensation registered)")

    print("\n[Main] Check the database to see the workflow status = 'failed'")
    print("[Main] and the compensation history")

    await app.shutdown()


if __name__ == "__main__":
    asyncio.run(main())
