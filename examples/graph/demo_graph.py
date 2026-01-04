"""Demo script for durable graph integration."""

import asyncio
from dataclasses import dataclass

from pydantic_graph import BaseNode, End, Graph

from edda import EddaApp, WorkflowContext, workflow
from edda.integrations.graph import DurableGraph, DurableGraphContext


# =============================================================================
# State Definition
# =============================================================================


@dataclass
class OrderState:
    """State for order processing graph."""

    order_id: str | None = None
    items: list[str] | None = None
    total: float = 0.0
    status: str = "pending"


# =============================================================================
# Node Definitions
# =============================================================================


@dataclass
class ValidateOrderNode(BaseNode[OrderState, None, dict]):
    """Validate the order."""

    order_id: str
    items: list[str]

    async def run(self, ctx: DurableGraphContext) -> "CalculateTotalNode":
        print(f"  [ValidateOrderNode] Validating order {self.order_id}...")
        ctx.state.order_id = self.order_id
        ctx.state.items = self.items
        ctx.state.status = "validated"
        print(f"  [ValidateOrderNode] Order validated: {len(self.items)} items")
        return CalculateTotalNode()


@dataclass
class CalculateTotalNode(BaseNode[OrderState, None, dict]):
    """Calculate order total."""

    async def run(self, ctx: DurableGraphContext) -> "ProcessPaymentNode":
        print(f"  [CalculateTotalNode] Calculating total...")
        # Simulate price calculation
        ctx.state.total = len(ctx.state.items or []) * 10.0
        print(f"  [CalculateTotalNode] Total: ${ctx.state.total}")
        return ProcessPaymentNode()


@dataclass
class ProcessPaymentNode(BaseNode[OrderState, None, dict]):
    """Process payment."""

    async def run(self, ctx: DurableGraphContext) -> "ShipOrderNode | End[dict]":
        print(f"  [ProcessPaymentNode] Processing payment of ${ctx.state.total}...")
        # Simulate payment processing
        await asyncio.sleep(0.1)

        if ctx.state.total > 100:
            print("  [ProcessPaymentNode] Payment failed: amount too high")
            ctx.state.status = "payment_failed"
            return End({
                "order_id": ctx.state.order_id,
                "status": "payment_failed",
                "reason": "Amount exceeds limit",
            })

        ctx.state.status = "paid"
        print("  [ProcessPaymentNode] Payment successful!")
        return ShipOrderNode()


@dataclass
class ShipOrderNode(BaseNode[OrderState, None, dict]):
    """Ship the order."""

    async def run(self, ctx: DurableGraphContext) -> End[dict]:
        print(f"  [ShipOrderNode] Shipping order {ctx.state.order_id}...")
        await asyncio.sleep(0.1)
        ctx.state.status = "shipped"
        print("  [ShipOrderNode] Order shipped!")
        return End({
            "order_id": ctx.state.order_id,
            "status": "shipped",
            "total": ctx.state.total,
            "items": ctx.state.items,
        })


# =============================================================================
# Graph Setup
# =============================================================================

order_graph = Graph(nodes=[ValidateOrderNode, CalculateTotalNode, ProcessPaymentNode, ShipOrderNode])
durable_order_graph = DurableGraph(order_graph)


# =============================================================================
# Workflow Definition
# =============================================================================


@workflow
async def process_order_workflow(
    ctx: WorkflowContext,
    order_id: str,
    items: list[str],
) -> dict:
    """Process an order using the durable graph."""
    print(f"\n=== Starting order workflow for {order_id} ===")

    result = await durable_order_graph.run(
        ctx,
        start_node=ValidateOrderNode(order_id=order_id, items=items),
        state=OrderState(),
    )

    print(f"=== Order workflow completed: {result} ===\n")
    return result


# =============================================================================
# Main
# =============================================================================


async def main():
    """Run the demo."""
    print("=" * 60)
    print("Durable Graph Integration Demo")
    print("=" * 60)

    # Create and initialize the app
    app = EddaApp(
        service_name="order-service",
        db_url="sqlite+aiosqlite:///:memory:",
    )
    await app.initialize()

    try:
        # Test 1: Successful order
        print("\n[Test 1] Processing a small order (should succeed)...")
        instance_id = await process_order_workflow.start(
            order_id="ORD-001",
            items=["item1", "item2", "item3"],
        )
        print(f"Workflow instance: {instance_id}")

        # Wait for completion
        await asyncio.sleep(0.5)

        # Check result
        instance = await app.storage.get_instance(instance_id)
        print(f"Status: {instance['status']}")
        print(f"Result: {instance['output_data']}")

        # Test 2: Failed order (too many items)
        print("\n[Test 2] Processing a large order (should fail at payment)...")
        instance_id_2 = await process_order_workflow.start(
            order_id="ORD-002",
            items=[f"item{i}" for i in range(15)],  # 15 items = $150 > $100 limit
        )
        print(f"Workflow instance: {instance_id_2}")

        await asyncio.sleep(0.5)

        instance_2 = await app.storage.get_instance(instance_id_2)
        print(f"Status: {instance_2['status']}")
        print(f"Result: {instance_2['output_data']}")

        # Show execution history
        print("\n[Execution History for Test 1]")
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
