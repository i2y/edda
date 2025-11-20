# FastAPI Integration

This guide demonstrates how to integrate Edda with FastAPI.

## Overview

Edda provides two patterns for triggering workflows:

1. **Direct Invocation**    
    - Call `await workflow.start()` from custom HTTP endpoints
        - REST API style, synchronous user actions
        - Example: `POST /api/orders` → start order workflow


2. **CloudEvents Integration**
    - Event-driven, automatic workflow dispatch
        - Mount EddaApp at `/workflows/events`
        - Workflows with `@workflow(event_handler=True)` auto-start on matching CloudEvents


Both patterns can coexist in the same application.

---

## Complete Code Example

Create `main.py`:

```python
"""FastAPI + Edda Integration Example"""

from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field
from edda import EddaApp, workflow, activity, WorkflowContext

# ===========================
# Pydantic Models
# ===========================

class OrderInput(BaseModel):
    order_id: str = Field(..., pattern=r"^ORD-\d+$")
    customer_email: str
    amount: float = Field(..., gt=0)

class OrderResult(BaseModel):
    order_id: str
    status: str
    confirmation_number: str

class PaymentEventInput(BaseModel):
    transaction_id: str
    order_id: str
    amount: float

class PaymentResult(BaseModel):
    transaction_id: str
    status: str

# Activity Result Models (Pydantic)
# Note: TypedDict also works, but Pydantic is recommended for runtime validation

class ValidationResult(BaseModel):
    order_id: str
    valid: bool

class PaymentChargeResult(BaseModel):
    amount: float
    transaction_id: str

class EmailConfirmationResult(BaseModel):
    email: str
    sent: bool

# ===========================
# Activities
# ===========================

@activity
async def validate_order(ctx: WorkflowContext, order_id: str) -> ValidationResult:
    """Validate order details."""
    return ValidationResult(order_id=order_id, valid=True)

@activity
async def charge_payment(ctx: WorkflowContext, amount: float) -> PaymentChargeResult:
    """Charge payment."""
    return PaymentChargeResult(
        amount=amount,
        transaction_id=f"TX-{ctx.instance_id[:8]}"
    )

@activity
async def send_confirmation_email(
    ctx: WorkflowContext,
    customer_email: str,
    confirmation_number: str
) -> EmailConfirmationResult:
    """Send confirmation email."""
    return EmailConfirmationResult(email=customer_email, sent=True)

# ===========================
# Workflows
# ===========================

@workflow
async def process_order(ctx: WorkflowContext, input: OrderInput) -> OrderResult:
    """
    Order processing workflow (Direct Invocation).

    Note: Activity IDs are auto-generated for sequential execution.
    """
    # Step 1: Validate order (auto-generated ID: "validate_order:1")
    await validate_order(ctx, order_id=input.order_id)

    # Step 2: Charge payment (auto-generated ID: "charge_payment:1")
    await charge_payment(ctx, amount=input.amount)

    # Step 3: Send confirmation email (auto-generated ID: "send_confirmation_email:1")
    confirmation_number = f"CONF-{ctx.instance_id[:8]}"
    await send_confirmation_email(
        ctx,
        customer_email=input.customer_email,
        confirmation_number=confirmation_number
    )

    return OrderResult(
        order_id=input.order_id,
        status="completed",
        confirmation_number=confirmation_number
    )

@workflow(event_handler=True)
async def payment_received_workflow(
    ctx: WorkflowContext,
    input: PaymentEventInput
) -> PaymentResult:
    """Payment received workflow (CloudEvents Triggered)."""
    return PaymentResult(
        transaction_id=input.transaction_id,
        status="recorded"
    )

# ===========================
# FastAPI Application with Lifespan
# ===========================

# Create Edda app (before FastAPI app)
edda_app = EddaApp(service_name="order-service", db_url="sqlite:///workflows.db")

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Manage application lifespan (startup and shutdown)."""
    # Startup: Initialize Edda
    await edda_app.initialize()
    yield
    # Shutdown: Cleanup Edda
    await edda_app.shutdown()

# Create FastAPI app with lifespan
api = FastAPI(
    title="Order Processing Service",
    version="1.0.0",
    lifespan=lifespan
)

# Mount Edda app (CloudEvents endpoint at /workflows/events)
api.mount("/workflows", edda_app)

# ===========================
# REST API Endpoints (Direct Invocation)
# ===========================

@api.post("/api/orders")
async def create_order(input: OrderInput):
    """Create and process a new order (Direct Invocation)."""
    try:
        instance_id = await process_order.start(input=input)
        return {
            "instance_id": instance_id,
            "status": "started",
            "message": f"Order {input.order_id} is being processed"
        }
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail="Internal server error")

@api.get("/api/orders/{instance_id}/status")
async def get_order_status(instance_id: str):
    """Get the status of an order workflow."""
    instance = await edda_app.storage.get_instance(instance_id)
    if not instance:
        raise HTTPException(status_code=404, detail="Workflow not found")
    return {
        "instance_id": instance_id,
        "workflow_name": instance["workflow_name"],
        "status": instance["status"],
        "output_data": instance.get("output_data")
    }

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(api, host="0.0.0.0", port=8000)
```

---

## Running the Application

```bash
# Install dependencies
pip install edda-framework fastapi uvicorn[standard]

# Or using uv
uv add edda-framework fastapi uvicorn[standard]

# Development mode
uvicorn main:api --reload

# Production mode (multiple workers)
uvicorn main:api --host 0.0.0.0 --port 8000 --workers 4
```

---

## Testing

### Create order (Direct Invocation)

```bash
curl -X POST http://localhost:8000/api/orders \
  -H "Content-Type: application/json" \
  -d '{"order_id": "ORD-123", "customer_email": "user@example.com", "amount": 99.99}'

# Response: {"instance_id": "abc-123", "status": "started", ...}
```

### Check status

```bash
curl http://localhost:8000/api/orders/abc-123/status
```

### Send CloudEvent

```bash
curl -X POST http://localhost:8000/workflows/events \
  -H "Content-Type: application/cloudevents+json" \
  -d '{
    "specversion": "1.0",
    "type": "payment_received_workflow",
    "source": "payment-gateway",
    "id": "evt-001",
    "data": {"transaction_id": "TX-999", "order_id": "ORD-123", "amount": 99.99}
  }'

# Response: {"status": "accepted"}
```

---

## Pattern Comparison

### Pattern 1: Direct Invocation

```python
@api.post("/api/orders")
async def create_order(input: OrderInput):
    # Direct workflow invocation
    instance_id = await process_order.start(input=input)
    return {"instance_id": instance_id}
```

**When to use:**
- User-initiated actions (REST APIs)
- Immediate feedback needed
- Synchronous operations

### Pattern 2: CloudEvents

```python
@workflow(event_handler=True)
async def payment_received(ctx: WorkflowContext, transaction_id: str):
    # This workflow auto-starts when CloudEvent type="payment_received" arrives
    return {"transaction_id": transaction_id}
```

**When to use:**
- Event-driven architectures
- Asynchronous notifications
- Microservices communication

### Using Both Together

```python
# Custom REST endpoint (Direct Invocation)
@api.post("/api/orders")
async def create_order(input: OrderInput):
    instance_id = await process_order.start(input=input)
    return {"instance_id": instance_id}

# CloudEvents endpoint (automatic)
api.mount("/workflows", edda_app)

# Workflows
@workflow  # Direct invocation only
async def process_order(ctx, input): ...

@workflow(event_handler=True)  # CloudEvents only
async def payment_received(ctx, input): ...
```

---

## Best Practices

### 1. Use Pydantic for Type Safety

```python
from pydantic import BaseModel, Field

class OrderInput(BaseModel):
    order_id: str = Field(..., pattern=r"^ORD-\d+$")
    amount: float = Field(..., gt=0)

@workflow
async def process_order(ctx: WorkflowContext, input: OrderInput):
    # Auto-validated
    return {"order_id": input.order_id}
```

### 2. Use PostgreSQL for Production

SQLite is single-writer; for production use PostgreSQL:

```python
edda_app = EddaApp(
    service_name="order-service",
    db_url="postgresql+asyncpg://user:password@localhost/edda"
)
```

### 3. Return `instance_id` Immediately

Workflows run in background. Return `instance_id` immediately and provide status endpoint:

```python
@api.post("/api/orders")
async def create_order(input: OrderInput):
    instance_id = await process_order.start(input=input)
    return {
        "instance_id": instance_id,
        "status": "started",
        "status_url": f"/api/orders/{instance_id}/status"
    }
```

### 4. Use ctx.session for External Database Operations

Access Edda-managed session for external DB operations within activities:

```python
from sqlalchemy import Column, String, Float
from sqlalchemy.orm import declarative_base

Base = declarative_base()

class Payment(Base):
    __tablename__ = "payments"
    order_id = Column(String, primary_key=True)
    amount = Column(Float, nullable=False)

@activity  # Edda manages the transaction automatically
async def process_payment(ctx: WorkflowContext, order_id: str, amount: float):
    # Access Edda-managed session (must be inside transaction)
    session = ctx.session

    # Your business logic (same transaction as Edda history recording)
    payment = Payment(order_id=order_id, amount=amount)
    session.add(payment)

    # Edda automatically commits (or rolls back on error)
    return {"order_id": order_id, "status": "processed"}
```

**Key Points:**

- `ctx.session` is only available inside `async with ctx.transaction():` or within `@activity` (which auto-wraps in transaction)
- Your DB operations and Edda's history recording share the same transaction
- If activity fails, both your DB changes and Edda history are rolled back atomically
- This ensures consistency between your business data and workflow state

**Example with manual transaction:**

```python
@workflow
async def manual_transaction_example(ctx: WorkflowContext, order_id: str):
    async with ctx.transaction():
        # Access Edda-managed session
        session = ctx.session

        # Multiple DB operations in same transaction
        payment = Payment(order_id=order_id, amount=99.99)
        session.add(payment)

        # Send event (also in same transaction via outbox pattern)
        from edda.outbox.transactional import send_event_transactional
        await send_event_transactional(
            ctx,
            event_type="payment.processed",
            event_source="order-service",
            event_data={"order_id": order_id, "amount": 99.99}
        )

        # Transaction commits automatically when exiting context
        # Or rolls back if exception occurs

    return {"order_id": order_id, "status": "completed"}
```

---

## Database Configuration

```python
# SQLite (development)
edda_app = EddaApp(service_name="order-service", db_url="sqlite:///workflows.db")

# PostgreSQL (production)
edda_app = EddaApp(
    service_name="order-service",
    db_url="postgresql+asyncpg://user:password@localhost/edda"
)

# MySQL (production)
edda_app = EddaApp(
    service_name="order-service",
    db_url="mysql+aiomysql://user:password@localhost/edda"
)
```

---

## Workflow Visualization

Use the Edda Viewer to visualize workflows:

```bash
# Install viewer dependencies
pip install edda-framework[viewer]

# Or using uv
uv add edda-framework --extra viewer

# Create viewer_app.py
cat > viewer_app.py << 'EOF'
from edda import EddaApp
from edda.viewer_ui import start_viewer

# Create Edda app for viewer (database access only)
edda_app = EddaApp(
    service_name="viewer",
    db_url="sqlite:///workflows.db"
)

if __name__ == "__main__":
    start_viewer(edda_app, port=8080)
EOF

# Run viewer
python viewer_app.py

# Or using uv
uv run python viewer_app.py
```

Open http://localhost:8080 to view workflow execution history and Mermaid diagrams.


## Next Steps

- [CloudEvents HTTP Binding](../core-features/events/cloudevents-http-binding.md) - CloudEvents specification
- [Workflow Viewer UI](../viewer-ui/setup.md) - Visualization setup
- [Core Concepts](../getting-started/concepts.md) - Learn about workflows and activities

---

## Related Documentation

- [FastAPI Documentation](https://fastapi.tiangolo.com/)
- [Uvicorn Documentation](https://www.uvicorn.org/)
- [CloudEvents Specification](https://github.com/cloudevents/spec)
