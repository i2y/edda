# pydantic-rpc Integration

Edda integrates with [pydantic-rpc](https://github.com/pydantic/pydantic-rpc), a library that provides gRPC and ConnectRPC support for Python using Pydantic models. This enables multi-language RPC clients (Go, Java, Rust, etc.) to trigger Edda durable workflows.

## Overview

pydantic-rpc allows you to define RPC services using Pydantic models. When combined with Edda:

- **Type-safe RPC**: Share Pydantic models between RPC and workflow definitions
- **Multi-language clients**: gRPC/ConnectRPC clients in any language can trigger Python workflows
- **Durable execution**: RPC-triggered workflows benefit from Edda's durability guarantees
- **Combined deployment**: Serve RPC and CloudEvents from a single ASGI application

## Installation

Install pydantic-rpc alongside Edda:

```bash
pip install edda-framework pydantic-rpc

# Or using uv
uv add edda-framework pydantic-rpc
```

## Pattern 1: Trigger Edda Workflows from RPC Services

Define an RPC service that starts Edda workflows:

```python
from pydantic import BaseModel, Field
from pydantic_rpc import Message

from edda import EddaApp, WorkflowContext, activity, workflow

# Shared Pydantic models (used by both RPC and Edda)
class OrderItem(BaseModel):
    product_id: str
    quantity: int = Field(ge=1)
    unit_price: float = Field(gt=0)


class OrderRequest(Message):
    """RPC input / Workflow input."""
    order_id: str
    customer_id: str
    items: list[OrderItem]


class OrderResponse(Message):
    """RPC output."""
    instance_id: str
    status: str


class OrderResult(BaseModel):
    """Workflow output."""
    order_id: str
    status: str
    total_amount: float


# Edda setup
edda_app = EddaApp(
    service_name="order-service",
    db_url="postgresql://user:pass@localhost/orders",
)


@activity
async def reserve_inventory(ctx: WorkflowContext, order_id: str, items: list[dict]):
    # Business logic here
    return {"reservation_id": f"RES-{order_id}"}


@activity
async def process_payment(ctx: WorkflowContext, order_id: str, amount: float):
    # Payment processing
    return {"payment_id": f"PAY-{order_id}"}


@workflow
async def process_order_workflow(ctx: WorkflowContext, input: OrderRequest) -> OrderResult:
    """Durable workflow triggered by RPC."""
    total = sum(item.quantity * item.unit_price for item in input.items)

    # Activities with automatic retry and replay
    await reserve_inventory(ctx, input.order_id, [i.model_dump() for i in input.items])
    await process_payment(ctx, input.order_id, total)

    return OrderResult(order_id=input.order_id, status="completed", total_amount=total)


# RPC Service that triggers Edda workflows
class OrderService:
    """gRPC/ConnectRPC service."""

    async def create_order(self, request: OrderRequest) -> OrderResponse:
        """Start Edda workflow and return immediately."""
        instance_id = await process_order_workflow.start(input=request)

        return OrderResponse(instance_id=instance_id, status="accepted")
```

## Pattern 2: Combined ASGI Application

Serve both RPC and CloudEvents from a single application:

```python
from pydantic_rpc import ASGIApp
from starlette.applications import Starlette
from starlette.routing import Mount

# Create pydantic-rpc ASGI app
rpc_app = ASGIApp()
rpc_app.mount(OrderService())

# Create combined Starlette app
app = Starlette(
    routes=[
        Mount("/rpc", app=rpc_app),   # ConnectRPC
        Mount("/", app=edda_app),     # CloudEvents, webhooks
    ]
)
```

Run with uvicorn:

```bash
uvicorn your_app:app --host 0.0.0.0 --port 8000
```

This enables:

- **RPC clients**: `POST /rpc/OrderService/CreateOrder`
- **CloudEvents**: `POST /` with CloudEvents headers
- **Webhooks**: External services can trigger workflows via HTTP

## Architecture

```
┌─────────────────────────────────────────────────┐
│              Combined ASGI App                  │
├─────────────────────┬───────────────────────────┤
│    /rpc             │         /                 │
│  ┌───────────────┐  │  ┌───────────────────┐    │
│  │ pydantic-rpc  │  │  │     EddaApp       │    │
│  │   (Connect)   │  │  │  (CloudEvents)    │    │
│  └───────┬───────┘  │  └─────────┬─────────┘    │
│          │          │            │              │
│          ▼          │            ▼              │
│  ┌───────────────────────────────────────┐      │
│  │          Edda Workflows               │      │
│  │   (Durable, Retry, Compensation)      │      │
│  └───────────────────────────────────────┘      │
│                      │                          │
│                      ▼                          │
│             ┌────────────────┐                  │
│             │    Database    │                  │
│             └────────────────┘                  │
└─────────────────────────────────────────────────┘
```

## Shared Pydantic Models

One key advantage is sharing models between RPC and Edda. Since `pydantic_rpc.Message` is just an alias for `pydantic.BaseModel`, the same models work for both:

```python
from pydantic import BaseModel

class OrderRequest(BaseModel):
    """Used by both RPC service and Edda workflow."""
    order_id: str
    items: list[OrderItem]

class OrderResult(BaseModel):
    """Workflow output, can also be returned via RPC."""
    order_id: str
    status: str
```

These models work seamlessly with Edda's Pydantic integration for:

- Automatic JSON serialization
- Type-safe replay
- Viewer UI form generation

## Related Documentation

- [Workflows and Activities](../core-features/workflows-activities.md)
- [Pydantic Integration](../core-features/workflows-activities.md#pydantic-integration)
- [Example Code](https://github.com/i2y/edda/blob/main/examples/pydantic_rpc_integration.py)
- [pydantic-rpc](https://github.com/i2y/pydantic-rpc)
- [connect-python](https://github.com/connectrpc/connect-python)
