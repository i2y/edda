"""
Edda + pydantic-rpc Integration Example.

This example demonstrates how to integrate Edda (Durable Execution Framework)
with pydantic-rpc (gRPC/ConnectRPC library).

Integration Patterns:
1. Trigger Edda workflows from RPC services
2. Call external RPC services as Edda activities
3. Expose workflow status via RPC
4. Combined ASGI application (RPC + CloudEvents)

Requirements:
    pip install pydantic-rpc
    # or
    uv add pydantic-rpc

Note: This is a conceptual example. To run it, you need both
pydantic-rpc and Edda installed, along with their dependencies.

Run with:
    # gRPC server mode
    uv run python -m examples.pydantic_rpc_integration

    # Or with uvicorn for ASGI mode
    uvicorn examples.pydantic_rpc_integration:combined_app --port 8000
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from typing import TYPE_CHECKING, Any

from pydantic import BaseModel, Field

from edda import EddaApp, WorkflowContext, activity, workflow

# pydantic-rpc imports (install with: pip install pydantic-rpc)
# Uncomment these when pydantic-rpc is installed:
# from pydantic_rpc import ASGIApp, AsyncIOServer, Message

# For type checking and documentation purposes, we define placeholder types
if TYPE_CHECKING:
    from pydantic_rpc import ASGIApp, AsyncIOServer, Message
else:
    # Placeholder for when pydantic-rpc is not installed
    Message = BaseModel

    class ASGIApp:
        def mount(self, service: Any) -> None: ...

    class AsyncIOServer:
        def __init__(self, port: int = 50051) -> None: ...
        async def run(self, *services: Any) -> None: ...


# =============================================================================
# Shared Pydantic Models (used by both Edda and pydantic-rpc)
# =============================================================================


class OrderItem(BaseModel):
    """Order item details."""

    product_id: str
    quantity: int = Field(ge=1)
    unit_price: float = Field(gt=0)


class OrderRequest(Message):
    """Request to create an order (RPC input / Workflow input)."""

    order_id: str
    customer_id: str
    items: list[OrderItem]


class OrderResponse(Message):
    """Response for order creation (RPC output)."""

    instance_id: str
    status: str


class OrderResult(BaseModel):
    """Final order result (Workflow output)."""

    order_id: str
    status: str
    total_amount: float
    payment_id: str | None = None


class PaymentRequest(Message):
    """Request to process payment (external RPC call)."""

    order_id: str
    amount: float
    customer_id: str


class PaymentResponse(Message):
    """Response from payment service."""

    transaction_id: str
    status: str


class WorkflowStatusRequest(Message):
    """Request to get workflow status."""

    instance_id: str


class WorkflowStatusResponse(Message):
    """Workflow status response."""

    instance_id: str
    workflow_name: str
    status: str
    current_activity: str | None
    output: dict[str, Any] | None


# =============================================================================
# Edda App Setup
# =============================================================================

edda_app = EddaApp(
    service_name="order-service",
    db_url="sqlite:///rpc_integration.db",
)


# =============================================================================
# Pattern 1: Trigger Edda Workflow from RPC Service
# =============================================================================


@activity
async def reserve_inventory(
    _ctx: WorkflowContext, order_id: str, items: list[dict[str, Any]]
) -> dict[str, Any]:
    """Reserve inventory for order items."""
    print(f"[Activity] Reserving inventory for order {order_id}")
    # Simulate inventory reservation
    await asyncio.sleep(0.1)
    return {"reservation_id": f"RES-{order_id}", "items_reserved": len(items)}


@activity
async def process_payment_internal(
    _ctx: WorkflowContext, order_id: str, amount: float
) -> dict[str, Any]:
    """Process payment internally (for demo without external RPC)."""
    print(f"[Activity] Processing payment ${amount} for order {order_id}")
    await asyncio.sleep(0.1)
    return {"payment_id": f"PAY-{order_id}", "status": "completed"}


@workflow
async def process_order_workflow(ctx: WorkflowContext, input: OrderRequest) -> OrderResult:
    """
    Durable workflow for order processing.

    This workflow is triggered by the RPC service and executes with
    Edda's durability guarantees (automatic retry, replay, compensation).
    """
    print(f"[Workflow] Processing order {input.order_id}")

    # Calculate total
    total = sum(item.quantity * item.unit_price for item in input.items)

    # Step 1: Reserve inventory
    reservation = await reserve_inventory(
        ctx, input.order_id, [item.model_dump() for item in input.items]
    )
    print(f"[Workflow] Inventory reserved: {reservation}")

    # Step 2: Process payment
    payment = await process_payment_internal(ctx, input.order_id, total)
    print(f"[Workflow] Payment processed: {payment}")

    return OrderResult(
        order_id=input.order_id,
        status="completed",
        total_amount=total,
        payment_id=payment["payment_id"],
    )


class OrderService:
    """
    RPC Service that triggers Edda workflows.

    This service exposes gRPC/ConnectRPC endpoints that start
    durable workflows in Edda.
    """

    async def create_order(self, request: OrderRequest) -> OrderResponse:
        """
        Create a new order (RPC endpoint).

        This method:
        1. Receives RPC request from any client (Go, Java, Rust, etc.)
        2. Starts an Edda durable workflow
        3. Returns immediately with instance_id (async pattern)
        """
        # Start Edda workflow - returns immediately
        instance_id = await process_order_workflow.start(input=request)

        return OrderResponse(
            instance_id=instance_id,
            status="accepted",
        )

    async def get_order_status(self, request: WorkflowStatusRequest) -> WorkflowStatusResponse:
        """Get order/workflow status."""
        instance = await edda_app.storage.get_instance(request.instance_id)
        if instance is None:
            raise ValueError(f"Workflow not found: {request.instance_id}")

        return WorkflowStatusResponse(
            instance_id=instance["instance_id"],
            workflow_name=instance["workflow_name"],
            status=instance["status"],
            current_activity=instance.get("current_activity_id"),
            output=instance.get("output_data"),
        )


# =============================================================================
# Pattern 2: Call External RPC Service as Edda Activity
# =============================================================================

# In a real application, you would create an RPC client:
# payment_client = Client("payment-service:50051")


@activity
async def call_external_payment_service(
    _ctx: WorkflowContext, request: PaymentRequest
) -> dict[str, Any]:
    """
    Call external payment service via RPC.

    This activity wraps an external RPC call with Edda's durability:
    - Automatic retry on transient failures
    - Result caching for replay
    - Saga compensation if later steps fail
    """
    print(f"[Activity] Calling external payment service for order {request.order_id}")

    # In a real implementation:
    # response = await payment_client.PaymentService.charge(request)
    # return {"transaction_id": response.transaction_id, "status": response.status}

    # Simulated response
    await asyncio.sleep(0.1)
    return {
        "transaction_id": f"TXN-{request.order_id}",
        "status": "success",
    }


@workflow
async def order_with_external_payment(ctx: WorkflowContext, input: OrderRequest) -> OrderResult:
    """
    Workflow that calls external RPC services.

    Edda manages:
    - Retry logic for failed RPC calls
    - Caching of successful results (no duplicate charges on replay)
    - Saga compensation (refund if shipping fails)
    """
    total = sum(item.quantity * item.unit_price for item in input.items)

    # Reserve inventory first
    await reserve_inventory(ctx, input.order_id, [item.model_dump() for item in input.items])

    # Call external payment service (with Edda durability)
    payment = await call_external_payment_service(
        ctx,
        PaymentRequest(
            order_id=input.order_id,
            amount=total,
            customer_id=input.customer_id,
        ),
    )

    return OrderResult(
        order_id=input.order_id,
        status="completed",
        total_amount=total,
        payment_id=payment["transaction_id"],
    )


# =============================================================================
# Pattern 3: Expose Workflow Status via RPC (with Streaming)
# =============================================================================


class WorkflowMonitorService:
    """
    RPC Service for monitoring Edda workflows.

    Provides gRPC endpoints for querying and watching workflow status,
    enabling multi-language clients to monitor workflow execution.
    """

    async def get_status(self, request: WorkflowStatusRequest) -> WorkflowStatusResponse:
        """Get current workflow status (unary RPC)."""
        instance = await edda_app.storage.get_instance(request.instance_id)
        if instance is None:
            raise ValueError(f"Workflow not found: {request.instance_id}")

        return WorkflowStatusResponse(
            instance_id=instance["instance_id"],
            workflow_name=instance["workflow_name"],
            status=instance["status"],
            current_activity=instance.get("current_activity_id"),
            output=instance.get("output_data"),
        )

    async def watch_status(
        self, request: WorkflowStatusRequest
    ) -> AsyncIterator[WorkflowStatusResponse]:
        """
        Watch workflow status changes (server streaming RPC).

        Yields status updates until workflow completes, fails, or is cancelled.
        Useful for real-time dashboards and monitoring tools.
        """
        terminal_statuses = {"completed", "failed", "cancelled"}

        while True:
            instance = await edda_app.storage.get_instance(request.instance_id)
            if instance is None:
                raise ValueError(f"Workflow not found: {request.instance_id}")

            yield WorkflowStatusResponse(
                instance_id=instance["instance_id"],
                workflow_name=instance["workflow_name"],
                status=instance["status"],
                current_activity=instance.get("current_activity_id"),
                output=instance.get("output_data"),
            )

            if instance["status"] in terminal_statuses:
                break

            await asyncio.sleep(1.0)  # Poll interval


# =============================================================================
# Pattern 4: Combined ASGI Application
# =============================================================================


def create_combined_asgi_app():
    """
    Create a combined ASGI app serving both RPC and CloudEvents.

    This allows a single deployment to handle:
    - gRPC/ConnectRPC requests (via pydantic-rpc)
    - CloudEvents (via Edda)
    - HTTP health checks, metrics, etc.
    """
    try:
        from pydantic_rpc import ASGIApp as RealASGIApp
        from starlette.applications import Starlette
        from starlette.routing import Mount

        # Create pydantic-rpc ASGI app
        rpc_app = RealASGIApp()
        rpc_app.mount(OrderService())
        rpc_app.mount(WorkflowMonitorService())

        # Create combined Starlette app
        app = Starlette(
            routes=[
                Mount("/rpc", app=rpc_app),  # gRPC-Web, ConnectRPC
                Mount("/", app=edda_app),  # CloudEvents, webhooks
            ]
        )

        return app

    except ImportError:
        print("Warning: pydantic-rpc or starlette not installed")
        print("Install with: pip install pydantic-rpc starlette")
        return edda_app


# Create the combined app (for uvicorn)
combined_app = create_combined_asgi_app()


# =============================================================================
# Main: Demo with gRPC Server
# =============================================================================


async def main():
    """Run demo showing all integration patterns."""
    print("=" * 70)
    print("Edda + pydantic-rpc Integration Example")
    print("=" * 70)

    # Initialize Edda
    await edda_app.initialize()

    try:
        # Demo Pattern 1: RPC triggers workflow
        print("\n--- Pattern 1: RPC triggers Edda workflow ---")
        service = OrderService()

        request = OrderRequest(
            order_id="ORD-001",
            customer_id="CUST-123",
            items=[
                OrderItem(product_id="PROD-A", quantity=2, unit_price=29.99),
                OrderItem(product_id="PROD-B", quantity=1, unit_price=49.99),
            ],
        )

        response = await service.create_order(request)
        print(f"Order created: instance_id={response.instance_id}")
        print(f"Status: {response.status}")

        # Demo Pattern 3: Monitor workflow status
        print("\n--- Pattern 3: Monitor workflow via RPC ---")
        monitor = WorkflowMonitorService()

        status_request = WorkflowStatusRequest(instance_id=response.instance_id)
        status = await monitor.get_status(status_request)
        print(f"Workflow status: {status.status}")
        print(f"Current activity: {status.current_activity}")

        # Note: In production, you would run the gRPC server:
        # server = AsyncIOServer(port=50051)
        # await server.run(OrderService(), WorkflowMonitorService())

        print("\n--- Integration Patterns Summary ---")
        print("1. RPC → Workflow: OrderService.create_order() starts workflow")
        print("2. Activity → RPC: call_external_payment_service() wraps RPC client")
        print("3. RPC Monitoring: WorkflowMonitorService exposes workflow status")
        print("4. Combined ASGI: Mount RPC and Edda on same app")

    finally:
        await edda_app.shutdown()

    print("\n" + "=" * 70)
    print("Example completed!")
    print("=" * 70)


if __name__ == "__main__":
    asyncio.run(main())
