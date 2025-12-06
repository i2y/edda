"""
Example: Integrating OpenTelemetry with Edda.

This example demonstrates how to add distributed tracing and optional metrics
to your Edda workflows using the official OpenTelemetry integration.

Installation:
    pip install edda-framework[opentelemetry]

    # Or using uv
    uv add edda-framework --extra opentelemetry

Usage:
    # Console exporter (local development)
    python examples/observability_with_opentelemetry.py

    # OTLP exporter (Jaeger, Tempo, etc.)
    python examples/observability_with_opentelemetry.py --otlp-endpoint http://localhost:4317

    # With metrics enabled
    python examples/observability_with_opentelemetry.py --metrics
"""

from __future__ import annotations

import argparse
import asyncio
from typing import Any

from edda import EddaApp, WorkflowContext, activity, workflow
from edda.integrations.opentelemetry import (
    OpenTelemetryHooks,
    inject_trace_context,
)

# =============================================================================
# Sample Activities and Workflow
# =============================================================================


@activity
async def reserve_inventory(
    _ctx: WorkflowContext, order_id: str, items: list[str]
) -> dict[str, Any]:
    """Reserve inventory for an order."""
    print(f"[Activity] Reserving inventory for order {order_id}: {items}")
    await asyncio.sleep(0.1)  # Simulate API call
    return {"reservation_id": f"RES-{order_id}", "status": "reserved"}


@activity
async def process_payment(
    _ctx: WorkflowContext, order_id: str, amount: float
) -> dict[str, Any]:
    """Process payment for an order."""
    print(f"[Activity] Processing payment for order {order_id}: ${amount}")
    await asyncio.sleep(0.1)  # Simulate API call
    return {"transaction_id": f"TXN-{order_id}", "status": "completed"}


@activity
async def ship_order(
    _ctx: WorkflowContext, order_id: str, address: str
) -> dict[str, Any]:
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
) -> dict[str, Any]:
    """
    Complete order workflow with inventory, payment, and shipping.

    OpenTelemetry hooks automatically capture:
    - Workflow lifecycle (start, complete, failure)
    - Activity execution (start, complete, failure)
    - Cache hits during replay
    - Parent-child span relationships
    """
    print(f"\n=== Starting Order Workflow: {order_id} ===")

    # Step 1: Reserve inventory
    reservation = await reserve_inventory(ctx, order_id, items)
    print(f"Reservation: {reservation}")

    # Step 2: Process payment
    payment = await process_payment(ctx, order_id, amount)
    print(f"Payment: {payment}")

    # Step 3: Ship order
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


# =============================================================================
# Main
# =============================================================================


async def main(otlp_endpoint: str | None = None, enable_metrics: bool = False) -> None:
    """Run the example workflow with OpenTelemetry observability."""
    print("=== Edda + OpenTelemetry Observability Example ===\n")

    # Create OpenTelemetry hooks using the official integration
    hooks = OpenTelemetryHooks(
        service_name="order-service",
        otlp_endpoint=otlp_endpoint,
        enable_metrics=enable_metrics,
    )

    print()

    # Create EddaApp with OpenTelemetryHooks
    app = EddaApp(
        service_name="order-service",
        db_url="sqlite:///example_opentelemetry.db",
        hooks=hooks,
    )

    # Initialize the app
    await app.initialize()
    print("EddaApp initialized with OpenTelemetryHooks\n")

    # Run the workflow
    try:
        instance_id = await order_workflow.start(
            order_id="ORD-12345",
            items=["laptop", "mouse", "keyboard"],
            amount=1299.99,
            shipping_address="42 Wallaby Way, Sydney, NSW",
        )

        print(f"Workflow started: {instance_id}\n")

        # Example: Inject trace context for CloudEvents propagation
        event_data = {"order_id": "ORD-12345", "status": "shipped"}
        event_data = inject_trace_context(hooks, instance_id, event_data)
        print(f"Event data with trace context: {event_data}\n")

        # Simulate a crash and replay (to demonstrate cache hit logging)
        print("--- Simulating workflow replay (to show cache hits) ---\n")

        # Get the workflow instance
        replay_engine = app.replay_engine
        if replay_engine:
            await replay_engine.resume_workflow(
                instance_id=instance_id,
                workflow_func=order_workflow.__wrapped__,
            )
            print("Workflow replayed successfully\n")

    finally:
        # Cleanup
        await app.shutdown()
        print("App shutdown complete\n")

    print("=== Example Complete ===")
    print("\nIf using OTLP exporter, check your tracing backend (Jaeger, Tempo, etc.) to see:")
    print("  - Workflow spans with child activity spans")
    print("  - Span attributes (instance_id, activity_id, cache_hit, etc.)")
    print("  - Error recording for failed activities")
    print("  - Retry events on activity spans")
    if enable_metrics:
        print("\nMetrics available:")
        print("  - edda.workflow.started (counter)")
        print("  - edda.workflow.completed (counter)")
        print("  - edda.workflow.failed (counter)")
        print("  - edda.workflow.duration (histogram)")
        print("  - edda.activity.executed (counter)")
        print("  - edda.activity.cache_hit (counter)")
        print("  - edda.activity.duration (histogram)")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Edda + OpenTelemetry observability example"
    )
    parser.add_argument(
        "--otlp-endpoint",
        type=str,
        default=None,
        help="OTLP endpoint URL (e.g., http://localhost:4317)",
    )
    parser.add_argument(
        "--metrics",
        action="store_true",
        help="Enable OpenTelemetry metrics",
    )

    args = parser.parse_args()

    asyncio.run(main(otlp_endpoint=args.otlp_endpoint, enable_metrics=args.metrics))
