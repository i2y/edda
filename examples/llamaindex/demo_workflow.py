"""Demo script for LlamaIndex Workflow with Edda durability."""

import asyncio

from llama_index.core.workflow import Event, StartEvent, StopEvent, Workflow, step

from edda import EddaApp, WorkflowContext, workflow
from edda.integrations.llamaindex import DurableSleepEvent, DurableWorkflowRunner


# =============================================================================
# Event Definitions
# =============================================================================


class OrderReceivedEvent(Event):
    """Event when order is received."""

    order_id: str
    amount: float


class ProcessingCompleteEvent(Event):
    """Event when processing is complete."""

    order_id: str
    status: str


# =============================================================================
# LlamaIndex Workflow Definition
# =============================================================================


class OrderWorkflow(Workflow):
    """Simple order processing workflow."""

    @step
    async def receive_order(self, ctx, ev: StartEvent) -> OrderReceivedEvent:
        """Receive and validate the order."""
        print(f"  [receive_order] Received order: {ev.order_id}")
        return OrderReceivedEvent(order_id=ev.order_id, amount=ev.amount)

    @step
    async def process_order(self, ctx, ev: OrderReceivedEvent) -> ProcessingCompleteEvent:
        """Process the order (simulate with delay)."""
        print(f"  [process_order] Processing order {ev.order_id}...")
        # Simulate processing
        await asyncio.sleep(0.1)
        print(f"  [process_order] Order {ev.order_id} processed!")
        return ProcessingCompleteEvent(order_id=ev.order_id, status="processed")

    @step
    async def complete_order(self, ctx, ev: ProcessingCompleteEvent) -> StopEvent:
        """Complete the order."""
        print(f"  [complete_order] Completing order {ev.order_id}")
        return StopEvent(
            result={
                "order_id": ev.order_id,
                "status": ev.status,
                "message": "Order completed successfully",
            }
        )


# =============================================================================
# Durable Runner Setup
# =============================================================================

runner = DurableWorkflowRunner(OrderWorkflow)


# =============================================================================
# Edda Workflow
# =============================================================================


@workflow
async def order_workflow(ctx: WorkflowContext, order_id: str, amount: float) -> dict:
    """Process an order using durable LlamaIndex workflow."""
    print(f"\n=== Starting order workflow for {order_id} ===")

    result = await runner.run(ctx, order_id=order_id, amount=amount)

    print(f"=== Order workflow completed: {result} ===\n")
    return result


# =============================================================================
# Main
# =============================================================================


async def main():
    """Run the demo."""
    print("=" * 60)
    print("LlamaIndex Workflow + Edda Durability Demo")
    print("=" * 60)

    # Create and initialize the app
    app = EddaApp(
        service_name="order-service",
        db_url="sqlite+aiosqlite:///:memory:",
    )
    await app.initialize()

    try:
        # Start the order workflow
        print("\n[Starting order workflow...]")

        instance_id = await order_workflow.start(
            order_id="ORD-001",
            amount=99.99,
        )
        print(f"Workflow instance: {instance_id}")

        # Wait for completion
        await asyncio.sleep(1)

        # Check final status
        instance = await app.storage.get_instance(instance_id)
        print(f"\n[Final Result]")
        print(f"Status: {instance['status']}")
        print(f"Result: {instance['output_data']}")

        # Show execution history
        print("\n[Execution History]")
        history = await app.storage.get_history(instance_id)
        for event in history:
            if event["event_type"] == "ActivityCompleted":
                activity_name = event["event_data"].get("activity_name", "unknown")
                print(f"  - {activity_name}")

    finally:
        await app.shutdown()

    print("\n" + "=" * 60)
    print("Demo completed successfully!")
    print("=" * 60)


if __name__ == "__main__":
    asyncio.run(main())
