"""
Order Processing MCP Server Example

This example demonstrates a more realistic use case: order processing workflow
with multiple activities including payment, inventory, and shipping.

AI assistants like Claude can use this to process customer orders as long-running
workflows with automatic crash recovery.
"""

import asyncio
import sys
from datetime import datetime
from pathlib import Path

from edda import WorkflowContext, activity
from edda.integrations.mcp import EddaMCPServer

# Create database directory in user's home directory
db_dir = Path.home() / ".edda"
db_dir.mkdir(exist_ok=True)
db_path = db_dir / "mcp_orders.db"

# Create MCP server with PostgreSQL (recommended for production)
# For demo purposes, we'll use SQLite
server = EddaMCPServer(
    name="Order Processing Service",
    db_url=f"sqlite+aiosqlite:///{db_path}",
    outbox_enabled=False,  # Set to True for event-driven architecture
)


@activity
async def validate_order(_ctx: WorkflowContext, order_id: str, items: list[dict]) -> dict:
    """Validate order items and availability."""
    await asyncio.sleep(1.0)  # Simulate validation

    # In real implementation, check inventory, pricing, etc.
    total_amount = sum(item["quantity"] * item["price"] for item in items)

    return {
        "valid": True,
        "order_id": order_id,
        "total_amount": total_amount,
        "validated_at": datetime.now().isoformat(),
    }


@activity
async def reserve_inventory(_ctx: WorkflowContext, items: list[dict]) -> dict:
    """Reserve inventory for order items."""
    await asyncio.sleep(1.5)  # Simulate inventory reservation

    reservation_ids = [f"RES-{item['product_id']}" for item in items]

    return {
        "reserved": True,
        "reservation_ids": reservation_ids,
        "reserved_at": datetime.now().isoformat(),
    }


@activity
async def process_payment(_ctx: WorkflowContext, order_id: str, amount: float) -> dict:
    """Process payment for the order."""
    await asyncio.sleep(2.0)  # Simulate payment processing

    # In real implementation, call payment gateway
    transaction_id = f"TXN-{order_id}-{datetime.now().timestamp()}"

    return {
        "paid": True,
        "transaction_id": transaction_id,
        "amount": amount,
        "paid_at": datetime.now().isoformat(),
    }


@activity
async def create_shipment(_ctx: WorkflowContext, order_id: str, address: dict) -> dict:
    """Create shipment for the order."""
    await asyncio.sleep(1.0)  # Simulate shipment creation

    tracking_number = f"TRACK-{order_id}"

    return {
        "shipped": True,
        "tracking_number": tracking_number,
        "address": address,
        "shipped_at": datetime.now().isoformat(),
    }


@server.durable_tool(
    description="Process a customer order: validate, reserve inventory, charge payment, and ship"
)
async def process_order(
    ctx: WorkflowContext,
    order_id: str,
    customer_email: str,
    items: list[dict],
    shipping_address: dict,
) -> dict:
    """
    Complete order processing workflow.

    This workflow handles:
    1. Order validation
    2. Inventory reservation
    3. Payment processing
    4. Shipment creation

    Args:
        order_id: Unique order identifier (e.g., "ORD-12345")
        customer_email: Customer's email address
        items: List of items with 'product_id', 'quantity', 'price'
        shipping_address: Dict with 'street', 'city', 'zip', 'country'

    Returns:
        dict with order status, transaction_id, tracking_number, etc.

    Example:
        items = [
            {"product_id": "PROD-A", "quantity": 2, "price": 29.99},
            {"product_id": "PROD-B", "quantity": 1, "price": 49.99}
        ]
        shipping_address = {
            "street": "123 Main St",
            "city": "New York",
            "zip": "10001",
            "country": "USA"
        }
    """
    # Step 1: Validate order
    validation = await validate_order(ctx, order_id, items)  # Auto: "validate_order:1"

    # Step 2: Reserve inventory
    _ = await reserve_inventory(ctx, items)  # Auto: "reserve_inventory:1"

    # Step 3: Process payment
    payment = await process_payment(
        ctx, order_id, validation["total_amount"]  # Auto: "process_payment:1"
    )

    # Step 4: Create shipment
    shipment = await create_shipment(ctx, order_id, shipping_address)  # Auto: "create_shipment:1"

    # Return complete order status
    return {
        "status": "completed",
        "order_id": order_id,
        "customer_email": customer_email,
        "total_amount": validation["total_amount"],
        "transaction_id": payment["transaction_id"],
        "tracking_number": shipment["tracking_number"],
        "completed_at": datetime.now().isoformat(),
    }


# Deploy the server
async def main():
    """Initialize and run the MCP server."""
    # Write to stderr to keep stdout clean for JSON-RPC messages (stdio transport compatibility)
    sys.stderr.write("Starting Order Processing MCP Server (stdio transport)...\n")
    sys.stderr.write("Server name: Order Processing Service\n")
    sys.stderr.write(f"Database: {db_path}\n")
    sys.stderr.write("\nAvailable MCP tools:\n")
    sys.stderr.write("  - process_order: Start order processing workflow\n")
    sys.stderr.write("  - process_order_status: Check workflow status\n")
    sys.stderr.write("  - process_order_result: Get processing result\n")
    sys.stderr.write("\nWorkflow steps:\n")
    sys.stderr.write("  1. Validate order (1s)\n")
    sys.stderr.write("  2. Reserve inventory (1.5s)\n")
    sys.stderr.write("  3. Process payment (2s)\n")
    sys.stderr.write("  4. Create shipment (1s)\n")
    sys.stderr.write("  Total: ~5.5 seconds\n")
    sys.stderr.write("\nPress Ctrl+C to stop\n")
    sys.stderr.flush()

    # Initialize EddaApp (setup replay engine, storage, etc.)
    await server.initialize()

    # Run with stdio transport (for MCP clients, e.g., Claude Desktop)
    # stdout is used for JSON-RPC messages, stderr for diagnostics
    await server.run_stdio()


if __name__ == "__main__":
    asyncio.run(main())
