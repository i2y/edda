"""
Cancellable Workflow Example for Kairo Framework.

This example demonstrates how to create workflows that can be cancelled,
including compensation execution on cancellation.

Usage:
    # Start the demo app
    uv run tsuno demo_app:application --bind 127.0.0.1:8001

    # In another terminal, start the viewer
    uv run python viewer_app.py

    # Run this example
    uv run python examples/cancellable_workflow.py

    # Open the viewer at http://localhost:8080
    # Click on the workflow instance to see details
    # Click "Cancel Workflow" button to cancel it
"""

import asyncio

from edda import EddaApp
from edda.activity import activity
from edda.compensation import register_compensation
from edda.context import WorkflowContext
from edda.workflow import workflow

# Simulated external service calls
external_reservations: dict[str, dict[str, str]] = {}


@activity
async def reserve_hotel(ctx: WorkflowContext, booking_id: str) -> dict[str, str]:
    """
    Reserve a hotel room.

    This activity registers a compensation to cancel the reservation
    if the workflow fails or is cancelled.
    """
    print(f"[Hotel] Reserving hotel for booking {booking_id}")

    # Simulate reservation
    hotel_reservation_id = f"HOTEL-{booking_id}"
    external_reservations[hotel_reservation_id] = {"type": "hotel", "booking_id": booking_id}

    # Register compensation to cancel the reservation
    await register_compensation(ctx, cancel_hotel_reservation, reservation_id=hotel_reservation_id)

    print(f"[Hotel] Reserved hotel: {hotel_reservation_id}")
    return {"hotel_reservation_id": hotel_reservation_id}


@activity
async def cancel_hotel_reservation(_ctx: WorkflowContext, reservation_id: str) -> None:
    """Compensation: Cancel hotel reservation."""
    print(f"[Hotel Compensation] Cancelling hotel reservation: {reservation_id}")

    if reservation_id in external_reservations:
        del external_reservations[reservation_id]
        print(f"[Hotel Compensation] Hotel reservation {reservation_id} cancelled")
    else:
        print(f"[Hotel Compensation] Hotel reservation {reservation_id} not found")


@activity
async def reserve_flight(ctx: WorkflowContext, booking_id: str) -> dict[str, str]:
    """
    Reserve a flight.

    This activity registers a compensation to cancel the reservation
    if the workflow fails or is cancelled.
    """
    print(f"[Flight] Reserving flight for booking {booking_id}")

    # Simulate reservation (with some delay)
    await asyncio.sleep(2)

    flight_reservation_id = f"FLIGHT-{booking_id}"
    external_reservations[flight_reservation_id] = {"type": "flight", "booking_id": booking_id}

    # Register compensation
    await register_compensation(
        ctx, cancel_flight_reservation, reservation_id=flight_reservation_id
    )

    print(f"[Flight] Reserved flight: {flight_reservation_id}")
    return {"flight_reservation_id": flight_reservation_id}


@activity
async def cancel_flight_reservation(_ctx: WorkflowContext, reservation_id: str) -> None:
    """Compensation: Cancel flight reservation."""
    print(f"[Flight Compensation] Cancelling flight reservation: {reservation_id}")

    if reservation_id in external_reservations:
        del external_reservations[reservation_id]
        print(f"[Flight Compensation] Flight reservation {reservation_id} cancelled")
    else:
        print(f"[Flight Compensation] Flight reservation {reservation_id} not found")


@activity
async def charge_payment(_ctx: WorkflowContext, booking_id: str, amount: float) -> dict[str, str]:
    """
    Charge customer payment.

    This is the final step. It has a long delay to give time for cancellation.
    """
    print(f"[Payment] Processing payment for booking {booking_id}: ${amount}")

    # Simulate slow payment processing (10 seconds)
    # This gives you time to cancel via the Viewer
    for i in range(10):
        await asyncio.sleep(1)
        print(f"[Payment] Processing... {i+1}/10")

    payment_id = f"PAY-{booking_id}"
    print(f"[Payment] Payment completed: {payment_id}")
    return {"payment_id": payment_id}


@workflow
async def cancellable_travel_booking(
    ctx: WorkflowContext, booking_id: str, destination: str, amount: float
) -> dict[str, str]:
    """
    A travel booking saga that can be cancelled.

    Note: Edda automatically generates activity IDs for sequential execution.

    This workflow:
    1. Reserves a hotel (with compensation)
    2. Reserves a flight (with compensation, includes delay)
    3. Charges payment (slow, gives time to cancel)

    If cancelled, compensations run in reverse order:
    1. Cancel flight reservation
    2. Cancel hotel reservation
    """
    print(f"\n{'='*60}")
    print(f"Starting travel booking saga: {booking_id}")
    print(f"Destination: {destination}")
    print(f"Amount: ${amount}")
    print(f"{'='*60}\n")

    # Step 1: Reserve hotel (Activity ID auto-generated: "reserve_hotel:1")
    hotel_result = await reserve_hotel(ctx, booking_id)
    print(f"✓ Hotel reserved: {hotel_result}")

    # Step 2: Reserve flight (with delay) (Activity ID auto-generated: "reserve_flight:1")
    flight_result = await reserve_flight(ctx, booking_id)
    print(f"✓ Flight reserved: {flight_result}")

    # Step 3: Charge payment (slow, gives time to cancel) (Activity ID auto-generated: "charge_payment:1")
    print(
        "\n⏰ Payment processing will take 10 seconds..."
    )
    print("💡 You can cancel this workflow from the Viewer now!")
    print("   1. Open http://localhost:8080")
    print(f"   2. Click on '{ctx.instance_id}'")
    print("   3. Click 'Cancel Workflow' button\n")

    payment_result = await charge_payment(ctx, booking_id, amount)
    print(f"✓ Payment charged: {payment_result}")

    print(f"\n{'='*60}")
    print("✅ Travel booking completed successfully!")
    print(f"{'='*60}\n")

    return {
        "booking_id": booking_id,
        "hotel": hotel_result["hotel_reservation_id"],
        "flight": flight_result["flight_reservation_id"],
        "payment": payment_result["payment_id"],
        "status": "completed",
    }


async def main() -> None:
    """Run the cancellable workflow example."""
    # Initialize Kairo app
    # Use the same database as demo_app and viewer for visibility
    app = EddaApp(
        service_name="travel-booking-service", db_url="sqlite:///demo.db"
    )

    await app.initialize()

    try:
        # Start the cancellable saga
        instance_id = await cancellable_travel_booking.start(
            booking_id="BOOK-001", destination="Tokyo", amount=1500.00
        )

        print(f"\n🎯 Workflow started with instance_id: {instance_id}")
        print(f"📊 View in Viewer: http://localhost:8080/workflow/{instance_id}")
        print("\n" + "=" * 60)
        print("INSTRUCTIONS:")
        print("=" * 60)
        print("1. Open the Viewer at http://localhost:8080")
        print(f"2. Click on the workflow instance: {instance_id}")
        print("3. Click the 'Cancel Workflow' button during payment processing")
        print("4. Observe the compensations being executed")
        print("=" * 60 + "\n")

        # Wait a bit to allow viewing
        await asyncio.sleep(15)

        # Check final status
        instance = await app.storage.get_instance(instance_id)
        if instance:
            final_status = instance["status"]
            print(f"\n📋 Final workflow status: {final_status}")

            if final_status == "cancelled":
                print("✅ Workflow was cancelled successfully!")
                print("🔄 Compensations were executed:")
                print("   - Flight reservation cancelled")
                print("   - Hotel reservation cancelled")
                print(f"\n📊 Remaining reservations: {len(external_reservations)}")
                print(f"   {list(external_reservations.keys())}")
            elif final_status == "completed":
                print("✅ Workflow completed successfully!")
                print(f"📊 Active reservations: {len(external_reservations)}")
                print(f"   {list(external_reservations.keys())}")

    finally:
        await app.shutdown()


if __name__ == "__main__":
    print("\n" + "🚀 " * 30)
    print("Cancellable Workflow Example")
    print("🚀 " * 30 + "\n")
    asyncio.run(main())
