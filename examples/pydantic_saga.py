"""
Example: Pydantic Models in Saga Workflows

This example demonstrates how to use Pydantic models for type-safe workflow
parameters and return values in Edda.

Features demonstrated:
- Pydantic model parameters with automatic validation
- Nested Pydantic models
- Pydantic model return values
- Type restoration during workflow execution
- JSON storage of models
"""

from datetime import UTC, datetime

from pydantic import BaseModel, Field

from edda import EddaApp, workflow
from edda.context import WorkflowContext


# Define Pydantic models for type-safe data structures
class Address(BaseModel):
    """Customer address model."""

    street: str
    city: str
    state: str
    zip_code: str = Field(..., pattern=r"^\d{5}(-\d{4})?$")


class Customer(BaseModel):
    """Customer model with validation."""

    customer_id: str = Field(..., pattern=r"^CUST-\d+$")
    name: str = Field(..., min_length=1, max_length=100)
    email: str = Field(..., pattern=r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$")
    phone: str | None = None
    address: Address


class OrderItem(BaseModel):
    """Order item model."""

    product_id: str = Field(..., pattern=r"^PROD-\d+$")
    quantity: int = Field(..., ge=1, le=100)
    unit_price: float = Field(..., ge=0.01)

    @property
    def subtotal(self) -> float:
        """Calculate item subtotal."""
        return self.quantity * self.unit_price


class OrderInput(BaseModel):
    """Order workflow input parameters."""

    order_id: str = Field(..., pattern=r"^ORD-\d+$")
    customer: Customer
    items: list[OrderItem] = Field(..., min_length=1)
    priority: str = Field("normal", pattern="^(low|normal|high|urgent)$")
    notes: str | None = None


class OrderResult(BaseModel):
    """Order workflow result."""

    order_id: str
    status: str
    confirmation_number: str
    total_amount: float
    estimated_delivery: datetime | None = None
    message: str


# Define the Saga workflow with Pydantic models
@workflow
async def process_order(ctx: WorkflowContext, order: OrderInput) -> OrderResult:
    """
    E-commerce order processing workflow with Pydantic models.

    This workflow demonstrates type-safe parameter handling using Pydantic.
    Input validation happens automatically when the workflow is started.

    Args:
        ctx: Workflow context
        order: Order input (Pydantic model, automatically validated)

    Returns:
        OrderResult: Pydantic model with order processing results
    """
    print(f"[Workflow {ctx.instance_id}] Processing order: {order.order_id}")
    print(f"[Workflow {ctx.instance_id}] Customer: {order.customer.name}")
    print(f"[Workflow {ctx.instance_id}] Items: {len(order.items)}")

    # Calculate total
    total = sum(item.subtotal for item in order.items)
    print(f"[Workflow {ctx.instance_id}] Total amount: ${total:.2f}")

    # Validate customer information
    if not order.customer.email:
        return OrderResult(
            order_id=order.order_id,
            status="failed",
            confirmation_number="",
            total_amount=total,
            estimated_delivery=None,
            message="Customer email is required",
        )

    # Process order (simplified - in reality, would call Activities)
    print(f"[Workflow {ctx.instance_id}] Validating inventory...")
    # await check_inventory(ctx, order.items)

    print(f"[Workflow {ctx.instance_id}] Processing payment for ${total:.2f}...")
    # await process_payment(ctx, order.customer.customer_id, total)

    print(f"[Workflow {ctx.instance_id}] Shipping to {order.customer.address.city}, {order.customer.address.state}...")
    # await create_shipment(ctx, order)

    # Generate confirmation
    confirmation_number = f"CONF-{order.order_id}-{datetime.now(UTC).strftime('%Y%m%d%H%M%S')}"
    estimated_delivery = datetime.now(UTC).replace(hour=0, minute=0, second=0, microsecond=0)

    # Return Pydantic model result (automatically converted to JSON for storage)
    return OrderResult(
        order_id=order.order_id,
        status="completed",
        confirmation_number=confirmation_number,
        total_amount=total,
        estimated_delivery=estimated_delivery,
        message=f"Order confirmed! Thank you, {order.customer.name}!",
    )


# Example usage
async def main():
    """Example usage of Pydantic models in workflows."""
    # Create Kairo app
    app = EddaApp(
        service_name="order-service",
        db_url="sqlite:///order_workflow.db",
    )
    await app.initialize()

    # Create Pydantic model instances
    customer = Customer(
        customer_id="CUST-12345",
        name="Alice Johnson",
        email="alice@example.com",
        phone="+1-555-0123",
        address=Address(
            street="123 Main St",
            city="Springfield",
            state="IL",
            zip_code="62701",
        ),
    )

    items = [
        OrderItem(product_id="PROD-001", quantity=2, unit_price=29.99),
        OrderItem(product_id="PROD-002", quantity=1, unit_price=49.99),
    ]

    order = OrderInput(
        order_id="ORD-2025001",
        customer=customer,
        items=items,
        priority="high",
        notes="Please deliver before 5 PM",
    )

    # Start workflow with Pydantic model
    # Models are automatically converted to JSON for storage
    instance_id = await process_order.start(order=order)

    print(f"\n✅ Workflow started: {instance_id}")
    print("Pydantic models are automatically:")
    print("  - Validated on input (raises ValidationError if invalid)")
    print("  - Converted to JSON for storage")
    print("  - Restored to Pydantic models during execution")
    print("  - Converted to JSON for return values")

    # Retrieve workflow result
    # Note: In production, you'd wait for completion or use event notifications
    # For this example, we assume synchronous execution


if __name__ == "__main__":
    import asyncio

    asyncio.run(main())
