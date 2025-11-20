# E-commerce with Pydantic

This example demonstrates type-safe workflows using Pydantic models.

## What This Example Shows

- ✅ Pydantic model parameters with automatic validation
- ✅ Nested Pydantic models (`Customer`, `Address`, `OrderItem`)
- ✅ Pydantic model return values
- ✅ Type restoration during workflow execution
- ✅ JSON storage of models

## Code Overview

### Define Pydantic Models

```python
from datetime import datetime
from pydantic import BaseModel, Field

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
```

### Define Workflow

```python
from edda import workflow, WorkflowContext

@workflow
async def process_order(ctx: WorkflowContext, order: OrderInput) -> OrderResult:
    """
    E-commerce order processing workflow with Pydantic models.

    Edda automatically:
    - Validates the input (raises ValidationError if invalid)
    - Stores order as JSON in the database
    - Restores OrderInput type on replay
    - Returns OrderResult with proper typing
    """

    # Calculate total from items
    total_amount = sum(item.subtotal for item in order.items)

    print(f"Processing order {order.order_id}")
    print(f"Customer: {order.customer.name} ({order.customer.email})")
    print(f"Items: {len(order.items)}, Total: ${total_amount:.2f}")

    # Workflow logic here...

    return OrderResult(
        order_id=order.order_id,
        status="completed",
        confirmation_number=f"CONF-{order.order_id}",
        total_amount=total_amount,
        message=f"Order processed for {order.customer.name}"
    )
```

### Start the Workflow

```python
from edda import EddaApp

async def main():
    app = EddaApp(service_name="order-service", db_url="sqlite:///orders.db")
    await app.initialize()

    # Create order with Pydantic models
    order = OrderInput(
        order_id="ORD-12345",
        customer=Customer(
            customer_id="CUST-001",
            name="Alice Johnson",
            email="alice@example.com",
            address=Address(
                street="123 Main St",
                city="Springfield",
                state="IL",
                zip_code="62701"
            )
        ),
        items=[
            OrderItem(product_id="PROD-101", quantity=2, unit_price=29.99),
            OrderItem(product_id="PROD-202", quantity=1, unit_price=49.99),
        ],
        priority="high"
    )

    # Start workflow - automatic validation
    instance_id = await process_order.start(order=order)
    print(f"Order started: {instance_id}")
```

## Benefits of Pydantic Integration

### 1. Automatic Validation

```python
# This will raise ValidationError:
bad_order = OrderInput(
    order_id="INVALID",  # ❌ Doesn't match pattern ^ORD-\d+$
    customer=...,
    items=[]  # ❌ min_length=1 violated
)
```

### 2. Type Safety

```python
@workflow
async def process_order(ctx: WorkflowContext, order: OrderInput) -> OrderResult:
    # IDE autocomplete works!
    customer_name = order.customer.name  # ✅ Type-safe
    total = sum(item.subtotal for item in order.items)  # ✅ Type-safe
    return OrderResult(...)  # ✅ Return type checked
```

### 3. Viewer UI Auto-Forms

When you use Pydantic models, the Viewer UI automatically generates input forms:

- **Field Types**: Text, number, checkbox based on type annotations
- **Validation**: Client-side validation from Field constraints
- **Nested Models**: Automatic form generation for nested structures

### 4. JSON Storage with Type Restoration

Edda stores Pydantic models as JSON and automatically restores them:

```python
# First run: OrderInput → JSON → Database
# Replay: Database → JSON → OrderInput (automatic restoration)
```

## Running the Example

Create a file named `pydantic_order_workflow.py` with the Pydantic models and workflow shown above, then run:

```bash
# Install Edda if you haven't already
uv add edda-framework

# Run your workflow
uv run python pydantic_order_workflow.py
```

## Complete Code

See a reference implementation in [examples/pydantic_saga.py](https://github.com/i2y/edda/blob/main/examples/pydantic_saga.py) in the Edda repository.

## What You Learned

- ✅ **Pydantic Models** provide type safety and validation
- ✅ **Nested Models** work seamlessly with Edda
- ✅ **Automatic Validation** happens before workflow starts
- ✅ **Type Restoration** works during replay
- ✅ **Viewer UI** auto-generates forms from models

## Next Steps

- **[Saga Pattern](saga.md)**: Add compensation to this workflow
- **[Your First Workflow](../getting-started/first-workflow.md)**: Step-by-step order processing tutorial
