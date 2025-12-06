"""
Complete Event Waiting Workflow Example.

This example demonstrates the full cycle:
1. Start a workflow that waits for an event
2. Send the CloudEvent to resume the workflow
3. Complete the workflow

Run with:
    uv run python -m examples.event_waiting_workflow_complete
"""

import asyncio
from uuid import uuid4

from edda import EddaApp, WorkflowContext, activity, wait_event, workflow


# Define activities
@activity
async def start_payment_processing(_ctx: WorkflowContext, order_id: str) -> dict:
    """Initiate payment processing."""
    print(f"[Activity] Starting payment processing for order: {order_id}")
    payment_id = f"payment-{order_id}"
    return {"payment_id": payment_id, "status": "pending"}


@activity
async def complete_order(_ctx: WorkflowContext, order_id: str, payment_data: dict) -> dict:
    """Complete the order after payment is confirmed."""
    print(f"[Activity] Completing order: {order_id}")
    print(f"[Activity] Payment data: {payment_data}")
    return {
        "order_id": order_id,
        "status": "completed",
        "payment_confirmed": True,
    }


# Define saga workflow with event waiting
@workflow
async def payment_workflow(ctx: WorkflowContext, order_id: str, amount: float) -> dict:  # noqa: ARG001
    """
    Order workflow that waits for payment confirmation.

    Note: Edda automatically generates activity IDs for sequential execution.

    This workflow demonstrates:
    1. Starting an async process (payment)
    2. Waiting for an external event (payment.completed)
    3. Continuing workflow after event arrives
    """
    print(f"\n[Workflow] Starting payment workflow for order: {order_id}")

    # Step 1: Initiate payment processing (Activity ID auto-generated: "start_payment_processing:1")
    payment_result = await start_payment_processing(ctx, order_id)
    print(f"[Workflow] Payment initiated: {payment_result}")

    # Step 2: Wait for payment confirmation event
    print("[Workflow] Waiting for payment confirmation event...")
    print(f"[Workflow] Instance ID: {ctx.instance_id}")

    payment_event = await wait_event(
        ctx,
        event_type="payment.completed",
        timeout_seconds=300,
    )
    print(f"[Workflow] Payment event received from {payment_event.source}: {payment_event.data}")
    print(f"[Workflow] Event ID: {payment_event.id}, Time: {payment_event.time}")

    # Step 3: Complete the order (Activity ID auto-generated: "complete_order:1")
    final_result = await complete_order(ctx, order_id, payment_event.data)
    print(f"[Workflow] Order completed: {final_result}")

    return final_result


async def send_payment_event(app: EddaApp, _instance_id: str, order_id: str) -> None:
    """Send a payment.completed CloudEvent to resume the workflow."""
    print("\n" + "=" * 60)
    print("Sending payment.completed event to resume workflow...")
    print("=" * 60)

    # Create CloudEvent
    event_data = {
        "order_id": order_id,
        "payment_id": f"payment-{order_id}",
        "status": "success",
        "amount": 99.99,
        "transaction_id": str(uuid4()),
    }

    # Send event to the workflow instance
    from cloudevents.http import CloudEvent

    event = CloudEvent(
        {
            "type": "payment.completed",
            "source": "payment-service",
            "id": str(uuid4()),
            "specversion": "1.0",
        },
        event_data,
    )

    # Deliver the event to the waiting workflow
    await app.handle_cloudevent(event)

    print(f"[Event] Sent payment.completed event: {event['type']}")


async def main():
    """Run the complete event waiting workflow example."""
    print("=" * 60)
    print("Complete Event Waiting Workflow Example")
    print("=" * 60)

    # Initialize Kairo app
    app = EddaApp(
        service_name="order-service",
        db_url="sqlite:///demo.db",
    )

    await app.initialize()

    try:
        order_id = "ORDER-99999"

        # Step 1: Start the workflow
        print(f"\n>>> Step 1: Starting workflow for {order_id}...")
        instance_id = await payment_workflow.start(
            order_id=order_id,
            amount=99.99,
        )

        print(f"\n[Main] Workflow started: {instance_id}")
        print("[Main] Workflow is now in 'waiting_for_event' status")

        # Check workflow status
        instance = await app.storage.get_instance(instance_id)
        if instance:
            print(f"[Main] Current status: {instance['status']}")

        # Step 2: Wait a moment for the workflow to settle
        print("\n>>> Step 2: Waiting 2 seconds before sending event...")
        await asyncio.sleep(2)

        # Step 3: Send the payment.completed event
        print("\n>>> Step 3: Sending payment.completed event...")
        await send_payment_event(app, instance_id, order_id)

        # Step 4: Wait for workflow to complete
        print("\n>>> Step 4: Waiting for workflow to complete...")
        await asyncio.sleep(2)

        # Check final status
        instance = await app.storage.get_instance(instance_id)
        if instance:
            final_status = instance["status"]
            print(f"\n{'=' * 60}")
            print(f"Final workflow status: {final_status}")
            print("=" * 60)

            if final_status == "completed":
                print("✅ Workflow completed successfully!")
                print("\nWorkflow execution flow:")
                print("  1. ✅ Started payment processing")
                print("  2. ⏸️  Waited for payment.completed event")
                print("  3. 📨 Received payment.completed event")
                print("  4. ✅ Completed the order")
            elif final_status == "waiting_for_event":
                print("⏸️  Workflow is still waiting for event")
                print("   (This might happen if event delivery is async)")
            else:
                print(f"ℹ️  Workflow status: {final_status}")

        print(f"\n💡 View in Viewer: http://localhost:8080/workflow/{instance_id}")

    finally:
        await app.shutdown()
        print("\n" + "=" * 60)
        print("Example completed!")
        print("=" * 60)


if __name__ == "__main__":
    asyncio.run(main())
