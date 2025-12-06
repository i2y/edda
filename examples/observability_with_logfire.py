"""
Example: Integrating Pydantic Logfire with Kairo using WorkflowHooks.

This example demonstrates how to add observability to your Kairo workflows
using the hook system. The same pattern works for other tools like Datadog,
Jaeger, or custom logging solutions.

Requirements:
    pip install 'kairo' 'logfire>=0.60.0'

Usage:
    # Set your Logfire token
    export LOGFIRE_TOKEN="your-token-here"

    # Run the example
    python examples/observability_with_logfire.py
"""

import asyncio
from typing import Any

import logfire

from edda import EddaApp, HooksBase, WorkflowContext, activity, workflow


# Step 1: Implement WorkflowHooks for Logfire
class LogfireHooks(HooksBase):
    """
    Logfire implementation of WorkflowHooks.

    Inherits from HooksBase to avoid implementing all methods.
    Only implement the hooks you need.
    """

    async def on_workflow_start(
        self, instance_id: str, workflow_name: str, input_data: dict[str, Any]
    ) -> None:
        """Log workflow start with Logfire."""
        logfire.info(
            "workflow.start",
            instance_id=instance_id,
            workflow_name=workflow_name,
            # Scrub sensitive data from input_data if needed
            input_keys=list(input_data.keys()),
        )

    async def on_workflow_complete(
        self, instance_id: str, workflow_name: str, _result: Any
    ) -> None:
        """Log workflow completion with Logfire."""
        logfire.info(
            "workflow.complete",
            instance_id=instance_id,
            workflow_name=workflow_name,
        )

    async def on_workflow_failed(
        self, instance_id: str, workflow_name: str, error: Exception
    ) -> None:
        """Log workflow failure with Logfire."""
        logfire.error(
            "workflow.failed",
            instance_id=instance_id,
            workflow_name=workflow_name,
            error_type=type(error).__name__,
            error_message=str(error),
        )

    async def on_workflow_cancelled(
        self, instance_id: str, workflow_name: str
    ) -> None:
        """Log workflow cancellation with Logfire."""
        logfire.warn(
            "workflow.cancelled",
            instance_id=instance_id,
            workflow_name=workflow_name,
        )

    async def on_activity_start(
        self,
        instance_id: str,
        activity_id: str,
        activity_name: str,
        is_replaying: bool,
    ) -> None:
        """Log activity start with Logfire."""
        logfire.info(
            "activity.start",
            instance_id=instance_id,
            activity_id=activity_id,
            activity_name=activity_name,
            is_replaying=is_replaying,
        )

    async def on_activity_complete(
        self,
        instance_id: str,
        activity_id: str,
        activity_name: str,
        _result: Any,
        cache_hit: bool,
    ) -> None:
        """Log activity completion with Logfire."""
        logfire.info(
            "activity.complete",
            instance_id=instance_id,
            activity_id=activity_id,
            activity_name=activity_name,
            cache_hit=cache_hit,
        )

    async def on_activity_failed(
        self,
        instance_id: str,
        activity_id: str,
        activity_name: str,
        error: Exception,
    ) -> None:
        """Log activity failure with Logfire."""
        logfire.error(
            "activity.failed",
            instance_id=instance_id,
            activity_id=activity_id,
            activity_name=activity_name,
            error_type=type(error).__name__,
            error_message=str(error),
        )


# Step 2: Define your workflow with activities
@activity
async def reserve_inventory(_ctx: WorkflowContext, order_id: str, items: list[str]) -> dict:
    """Reserve inventory for an order."""
    print(f"[Activity] Reserving inventory for order {order_id}: {items}")
    await asyncio.sleep(0.1)  # Simulate API call
    return {"reservation_id": f"RES-{order_id}", "status": "reserved"}


@activity
async def process_payment(_ctx: WorkflowContext, order_id: str, amount: float) -> dict:
    """Process payment for an order."""
    print(f"[Activity] Processing payment for order {order_id}: ${amount}")
    await asyncio.sleep(0.1)  # Simulate API call
    return {"transaction_id": f"TXN-{order_id}", "status": "completed"}


@activity
async def ship_order(_ctx: WorkflowContext, order_id: str, address: str) -> dict:
    """Ship the order."""
    print(f"[Activity] Shipping order {order_id} to {address}")
    await asyncio.sleep(0.1)  # Simulate API call
    return {"tracking_number": f"TRACK-{order_id}", "status": "shipped"}


@workflow
async def order_workflow(
    ctx: WorkflowContext,
    order_id: str,
    items: list[str],
    amount: float,
    shipping_address: str,
) -> dict:
    """
    Complete order workflow with inventory, payment, and shipping.

    This workflow demonstrates how Logfire hooks automatically capture:
    - Workflow lifecycle (start, complete, failure)
    - Activity execution (start, complete, failure)
    - Cache hits during replay
    """
    print(f"\n=== Starting Order Workflow: {order_id} ===")

    # Step 1: Reserve inventory (Activity ID auto-generated: "reserve_inventory:1")
    reservation = await reserve_inventory(ctx, order_id, items)
    print(f"Reservation: {reservation}")

    # Step 2: Process payment (Activity ID auto-generated: "process_payment:1")
    payment = await process_payment(ctx, order_id, amount)
    print(f"Payment: {payment}")

    # Step 3: Ship order (Activity ID auto-generated: "ship_order:1")
    shipping = await ship_order(ctx, order_id, shipping_address)
    print(f"Shipping: {shipping}")

    print(f"=== Order Workflow Completed: {order_id} ===\n")

    return {
        "order_id": order_id,
        "status": "completed",
        "reservation_id": reservation["reservation_id"],
        "transaction_id": payment["transaction_id"],
        "tracking_number": shipping["tracking_number"],
    }


# Step 3: Initialize Logfire and create EddaApp with hooks
async def main():
    """Run the example workflow with Logfire observability."""
    print("=== Kairo + Logfire Observability Example ===\n")

    # Initialize Logfire
    # Note: Set LOGFIRE_TOKEN environment variable or pass token parameter
    logfire.configure(
        service_name="kairo-example",
        # token="your-token-here",  # Or use LOGFIRE_TOKEN env var
        # send_to_logfire="if-token-present",  # Default behavior
    )

    # Instrument SQLite (optional - automatically traces all queries)
    logfire.instrument_sqlite3()

    print("✓ Logfire initialized\n")

    # Create EddaApp with LogfireHooks
    app = EddaApp(
        service_name="order-service",
        db_url="sqlite:///example_observability.db",
        hooks=LogfireHooks(),  # <-- Pass hooks here
    )

    # Initialize the app
    await app.initialize()
    print("✓ EddaApp initialized with LogfireHooks\n")

    # Run the workflow
    try:
        instance_id = await order_workflow.start(
            order_id="ORD-12345",
            items=["laptop", "mouse", "keyboard"],
            amount=1299.99,
            shipping_address="42 Wallaby Way, Sydney, NSW",
        )

        print(f"✓ Workflow started: {instance_id}\n")

        # Simulate a crash and replay (to demonstrate cache hit logging)
        print("--- Simulating workflow replay (to show cache hits) ---\n")

        # Get the workflow instance
        replay_engine = app.replay_engine
        if replay_engine:
            await replay_engine.resume_workflow(
                instance_id=instance_id,
                workflow_func=order_workflow.__wrapped__,
            )
            print("✓ Workflow replayed successfully\n")

    finally:
        # Cleanup
        await app.shutdown()
        print("✓ App shutdown complete\n")

    print("=== Example Complete ===")
    print("\nCheck your Logfire dashboard to see:")
    print("  - Workflow lifecycle traces")
    print("  - Activity execution spans")
    print("  - Cache hit/miss tracking")
    print("  - SQLite query traces (if instrumented)")
    print("\nOr check your self-hosted OpenTelemetry collector if configured.")


# Alternative: Custom hook for your own logging solution
class CustomHooks(HooksBase):
    """
    Example: Custom hooks for your own logging/monitoring solution.

    You can integrate with any tool:
    - Datadog: https://docs.datadoghq.com/tracing/
    - Jaeger: https://www.jaegertracing.io/
    - Prometheus: https://prometheus.io/
    - Custom database/metrics service
    """

    async def on_workflow_start(
        self, instance_id: str, workflow_name: str, input_data: dict[str, Any]
    ) -> None:
        """Send metrics to your custom monitoring service."""
        # Example: Send to Datadog
        # datadog.statsd.increment('kairo.workflow.started', tags=[f'workflow:{workflow_name}'])

        # Example: Send to Prometheus
        # workflow_start_counter.labels(workflow=workflow_name).inc()

        # Example: Custom database logging
        # await your_db.insert_workflow_event(instance_id, 'started', workflow_name)

        pass

    # Implement other hooks as needed...


if __name__ == "__main__":
    # Run the example
    asyncio.run(main())
