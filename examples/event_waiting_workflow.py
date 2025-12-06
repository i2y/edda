"""
Example workflow demonstrating event waiting.

This example shows how to use wait_event to pause a workflow until a
CloudEvent arrives, implementing async communication patterns.
"""

import asyncio

from edda import EddaApp, WorkflowContext, activity, wait_event, workflow


# Define activities
@activity
async def start_payment_processing(_ctx: WorkflowContext, order_id: str) -> dict:
    """Initiate payment processing."""
    print(f"[Activity] Starting payment processing for order: {order_id}")
    payment_id = f"payment-{order_id}"
    # In a real system, this would call a payment service
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
    # The workflow will be paused here until the event arrives
    print("[Workflow] Waiting for payment confirmation event...")

    try:
        payment_event = await wait_event(
            ctx,
            event_type="payment.completed",
            timeout_seconds=300,  # 5 minute timeout
        )
        print(f"[Workflow] Payment event received from {payment_event.source}: {payment_event.data}")
        print(f"[Workflow] Event ID: {payment_event.id}, Time: {payment_event.time}")
    except Exception as e:
        print(f"[Workflow] Error waiting for event: {e}")
        raise

    # Step 3: Complete the order (Activity ID auto-generated: "complete_order:1")
    final_result = await complete_order(ctx, order_id, payment_event.data)
    print(f"[Workflow] Order completed: {final_result}")

    return final_result


async def main():
    """Run the event waiting workflow example."""
    # Initialize Kairo app
    app = EddaApp(
        service_name="order-service",
        db_url="sqlite:///demo.db",
    )

    await app.initialize()

    print("=" * 60)
    print("Event Waiting Workflow Example")
    print("=" * 60)

    # Start the workflow
    instance_id = await payment_workflow.start(
        order_id="ORDER-12345",
        amount=99.99,
    )

    print(f"\n[Main] Workflow started: {instance_id}")
    print("[Main] Workflow is now waiting for 'payment.completed' event")
    print("\n[Main] To resume the workflow, you would:")
    print("  1. Receive a CloudEvent of type 'payment.completed'")
    print("  2. Call app.handle_event() with the event")
    print("  3. The workflow will resume and complete the order")

    # In a real system, this would be triggered by an incoming CloudEvent:
    #
    # event = {
    #     "type": "payment.completed",
    #     "source": "payment-service",
    #     "data": {
    #         "order_id": "ORDER-12345",
    #         "payment_id": "payment-ORDER-12345",
    #         "status": "success",
    #         "amount": 99.99
    #     }
    # }
    # await app.handle_event(event, instance_id)

    print("\n[Main] For this example, the workflow will remain paused.")
    print("[Main] Check the database to see the workflow in 'waiting_for_event' status")

    await app.shutdown()


if __name__ == "__main__":
    asyncio.run(main())
