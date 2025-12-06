"""
Event Waiting Workflow as ASGI Application.

This application demonstrates wait_event() functionality with a payment workflow.

Usage:
    # Start the app
    uv run tsuno event_waiting_app:application --bind 127.0.0.1:8002

    # In another terminal, trigger the workflow
    curl -X POST http://localhost:8002/events \
      -H "Content-Type: application/json" \
      -H "CE-Type: payment_workflow" \
      -H "CE-Source: example" \
      -H "CE-ID: $(uuidgen)" \
      -H "CE-SpecVersion: 1.0" \
      -d '{"order_id": "ORDER-12345", "amount": 99.99}'

    # Wait a moment, then send the payment.completed event
    curl -X POST http://localhost:8002/events \
      -H "Content-Type: application/json" \
      -H "CE-Type: payment.completed" \
      -H "CE-Source: payment-service" \
      -H "CE-ID: $(uuidgen)" \
      -H "CE-SpecVersion: 1.0" \
      -d '{"order_id": "ORDER-12345", "payment_id": "PAY-123", "status": "success", "amount": 99.99}'

    # View in Viewer: http://localhost:8080
"""

import asyncio
import sys

import uvloop

from edda import EddaApp, WorkflowContext, activity, wait_event, workflow

# Python 3.12+ uses asyncio.set_event_loop_policy() instead of uvloop.install()
if sys.version_info >= (3, 12):
    asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
else:
    uvloop.install()

# Create Kairo app
app = EddaApp(
    service_name="payment-service",
    db_url="sqlite:///demo.db",
)


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
async def payment_workflow(ctx: WorkflowContext, order_id: str, amount: float) -> dict:
    """
    Order workflow that waits for payment confirmation.

    Note: Edda automatically generates activity IDs for sequential execution.

    CloudEvent type: "payment_workflow"
    Data: {"order_id": "ORDER-12345", "amount": 99.99}

    This workflow demonstrates:
    1. Starting an async process (payment)
    2. Waiting for an external event (payment.completed)
    3. Continuing workflow after event arrives
    """
    print(f"\n{'='*60}")
    print(f"[Workflow] Starting payment workflow for order: {order_id}")
    print(f"[Workflow] Amount: ${amount}")
    print(f"[Workflow] Instance ID: {ctx.instance_id}")
    print(f"{'='*60}\n")

    # Step 1: Initiate payment processing (Activity ID auto-generated: "start_payment_processing:1")
    payment_result = await start_payment_processing(ctx, order_id)
    print(f"[Workflow] Payment initiated: {payment_result}")

    # Step 2: Wait for payment confirmation event
    print("\n[Workflow] ⏸️  Waiting for payment confirmation event...")
    print("[Workflow] Expected event type: 'payment.completed'")
    print(f"[Workflow] Expected data: order_id = '{order_id}'")
    print("\n💡 To send the event, run:")
    print("curl -X POST http://localhost:8002/events \\")
    print('  -H "Content-Type: application/json" \\')
    print('  -H "CE-Type: payment.completed" \\')
    print('  -H "CE-Source: payment-service" \\')
    print('  -H "CE-ID: $(uuidgen)" \\')
    print('  -H "CE-SpecVersion: 1.0" \\')
    print(f"  -d '{{\"order_id\": \"{order_id}\", \"payment_id\": \"PAY-123\", \"status\": \"success\", \"amount\": {amount}}}'\n")

    payment_event = await wait_event(
        ctx,
        event_type="payment.completed",
        timeout_seconds=300,  # 5 minute timeout
    )
    print(f"\n[Workflow] ✅ Payment event received from {payment_event.source}: {payment_event.data}")
    print(f"[Workflow] Event ID: {payment_event.id}, Time: {payment_event.time}")

    # Step 3: Complete the order (Activity ID auto-generated: "complete_order:1")
    final_result = await complete_order(ctx, order_id, payment_event.data)
    print(f"[Workflow] ✅ Order completed: {final_result}")

    print(f"\n{'='*60}")
    print("[Workflow] Payment workflow completed successfully!")
    print(f"{'='*60}\n")

    return final_result


# Export ASGI application
application = app

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(application, host="127.0.0.1", port=8002)
