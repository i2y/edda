"""
Example: Transactional Outbox Pattern

This example demonstrates how to use the transactional outbox pattern
for reliable event publishing in distributed systems.

The outbox pattern ensures that events are published atomically with
workflow execution - if the workflow fails, events are not published.

Key features:
- Transactional event sending
- Automatic retry with exponential backoff
- At-least-once delivery guarantee
- Integration with Knative Eventing

Run:
    python examples/with_outbox.py
"""

import asyncio
import os

from edda import (
    EddaApp,
    WorkflowContext,
    activity,
    register_compensation,
    send_event_transactional,
    workflow,
)

# ============================================================================
# Activities with Transactional Event Sending
# ============================================================================


@activity
async def reserve_inventory(ctx: WorkflowContext, order_id: str, items: list[dict]) -> dict:
    """
    Reserve inventory for an order.

    This activity demonstrates transactional event sending:
    - Business logic (inventory reservation) happens first
    - Event is written to outbox table in same transaction
    - If workflow fails, event is NOT published
    - If workflow succeeds, outbox relayer will publish the event
    """
    print(f"[Activity] Reserving inventory for order {order_id}")

    # Simulate inventory reservation
    reservation_id = f"RES-{order_id}"
    reserved_items = [{"sku": item["sku"], "quantity": item["qty"]} for item in items]

    print(f"[Activity] Reserved: {reserved_items}")

    # Send event transactionally
    # This writes to the outbox table instead of sending immediately
    await send_event_transactional(
        ctx,
        event_type="inventory.reserved",
        event_source="order-service",
        event_data={
            "order_id": order_id,
            "reservation_id": reservation_id,
            "items": reserved_items,
        },
    )

    print("[Activity] Event 'inventory.reserved' written to outbox")

    return {
        "reservation_id": reservation_id,
        "items": reserved_items,
    }


@activity
async def charge_payment(ctx: WorkflowContext, order_id: str, amount: float) -> dict:
    """
    Charge payment for an order with transactional event.
    """
    print(f"[Activity] Charging payment for order {order_id}: ${amount}")

    payment_id = f"PAY-{order_id}"

    # Send payment charged event transactionally
    await send_event_transactional(
        ctx,
        event_type="payment.charged",
        event_source="order-service",
        event_data={
            "order_id": order_id,
            "payment_id": payment_id,
            "amount": amount,
            "currency": "USD",
        },
    )

    print("[Activity] Event 'payment.charged' written to outbox")

    return {
        "payment_id": payment_id,
        "amount": amount,
    }


@activity
async def ship_order(ctx: WorkflowContext, order_id: str) -> dict:
    """
    Ship an order with transactional event.
    """
    print(f"[Activity] Shipping order {order_id}")

    shipment_id = f"SHIP-{order_id}"

    # Send order shipped event transactionally
    await send_event_transactional(
        ctx,
        event_type="order.shipped",
        event_source="order-service",
        event_data={
            "order_id": order_id,
            "shipment_id": shipment_id,
            "carrier": "UPS",
            "tracking_number": "1Z999AA10123456784",
        },
    )

    print("[Activity] Event 'order.shipped' written to outbox")

    return {
        "shipment_id": shipment_id,
    }


# Compensation activities
@activity
async def release_inventory(ctx: WorkflowContext, reservation_id: str) -> None:
    """Release inventory reservation (compensation)."""
    print(f"[Compensation] Releasing inventory reservation: {reservation_id}")

    # Send compensation event transactionally
    await send_event_transactional(
        ctx,
        event_type="inventory.released",
        event_source="order-service",
        event_data={
            "reservation_id": reservation_id,
            "reason": "order_failed",
        },
    )


@activity
async def refund_payment(ctx: WorkflowContext, payment_id: str, amount: float) -> None:
    """Refund payment (compensation)."""
    print(f"[Compensation] Refunding payment: {payment_id} (${amount})")

    # Send refund event transactionally
    await send_event_transactional(
        ctx,
        event_type="payment.refunded",
        event_source="order-service",
        event_data={
            "payment_id": payment_id,
            "amount": amount,
            "reason": "order_failed",
        },
    )


# ============================================================================
# Saga Workflow with Transactional Outbox
# ============================================================================


@workflow
async def order_fulfillment_saga(
    ctx: WorkflowContext,
    order_id: str,
    items: list[dict],
    amount: float,
) -> dict:
    """
    Order fulfillment workflow using transactional outbox pattern.

    Note: Edda automatically generates activity IDs for sequential execution.

    This workflow demonstrates:
    - Multiple activities sending events transactionally
    - Automatic compensation on failure
    - All events published via outbox relayer
    """
    print(f"\n[Workflow] Starting order fulfillment: {order_id}")

    # Step 1: Reserve inventory (Activity ID auto-generated: "reserve_inventory:1")
    inventory_result = await reserve_inventory(ctx, order_id, items)
    await register_compensation(
        ctx,
        release_inventory,
        reservation_id=inventory_result["reservation_id"],
    )

    # Step 2: Charge payment (Activity ID auto-generated: "charge_payment:1")
    payment_result = await charge_payment(ctx, order_id, amount)
    await register_compensation(
        ctx,
        refund_payment,
        payment_id=payment_result["payment_id"],
        amount=amount,
    )

    # Step 3: Ship order (Activity ID auto-generated: "ship_order:1")
    shipment_result = await ship_order(ctx, order_id)

    print(f"[Workflow] Order {order_id} completed successfully!")
    print(f"  - Reservation: {inventory_result['reservation_id']}")
    print(f"  - Payment: {payment_result['payment_id']}")
    print(f"  - Shipment: {shipment_result['shipment_id']}")

    # Send final order completed event
    await send_event_transactional(
        ctx,
        event_type="order.completed",
        event_source="order-service",
        event_data={
            "order_id": order_id,
            "reservation_id": inventory_result["reservation_id"],
            "payment_id": payment_result["payment_id"],
            "shipment_id": shipment_result["shipment_id"],
        },
    )

    return {
        "status": "completed",
        "order_id": order_id,
        "reservation_id": inventory_result["reservation_id"],
        "payment_id": payment_result["payment_id"],
        "shipment_id": shipment_result["shipment_id"],
    }


# ============================================================================
# Main Application
# ============================================================================


async def main():
    """
    Main application demonstrating transactional outbox pattern.
    """
    print("=" * 70)
    print("Kairo Framework - Transactional Outbox Pattern Example")
    print("=" * 70)

    # Initialize Kairo app with outbox enabled
    app = EddaApp(
        service_name="order-service",
        db_url="sqlite:///demo.db",
        outbox_enabled=True,  # Enable outbox relayer
        broker_url=os.environ.get(
            "BROKER_URL",
            "http://broker-ingress.knative-eventing.svc.cluster.local/default/default",
        ),
    )

    # Initialize the app (starts outbox relayer)
    await app.initialize()

    print("\n[Main] Outbox relayer started - events will be published asynchronously")
    print("[Main] Starting order fulfillment workflow...\n")

    try:
        # Start the saga
        instance_id = await order_fulfillment_saga.start(
            order_id="ORDER-12345",
            items=[
                {"sku": "WIDGET-A", "qty": 2},
                {"sku": "GADGET-B", "qty": 1},
            ],
            amount=199.99,
        )

        print(f"\n[Main] Workflow instance started: {instance_id}")
        print("[Main] Workflow completed successfully")

        # Wait a bit for outbox relayer to publish events
        print("\n[Main] Waiting for outbox relayer to publish events...")
        await asyncio.sleep(3)

        # Check outbox status
        pending_events = await app.storage.get_pending_outbox_events(limit=100)
        print(f"\n[Main] Pending outbox events: {len(pending_events)}")

        if len(pending_events) == 0:
            print("[Main] ✅ All events have been published!")
        else:
            print(f"[Main] ⏳ {len(pending_events)} events still pending...")

    except Exception as e:
        print(f"\n[Main] ❌ Workflow failed: {e}")

    finally:
        # Shutdown the app (stops outbox relayer)
        print("\n[Main] Shutting down...")
        await app.shutdown()

    print("\n" + "=" * 70)
    print("Example completed!")
    print("=" * 70)
    print("\nKey Takeaways:")
    print("1. Events are written to outbox table during workflow execution")
    print("2. Outbox relayer publishes events asynchronously in background")
    print("3. If workflow fails, events are NOT published (transactional)")
    print("4. Retry logic ensures at-least-once delivery")
    print("5. Check outbox_example.db to see the outbox_events table")


if __name__ == "__main__":
    asyncio.run(main())
