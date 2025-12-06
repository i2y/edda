"""
TypedDict Example for Kairo Framework

This example demonstrates using TypedDict with Kairo workflows.

IMPORTANT: TypedDict provides static typing only (for mypy and IDEs).
It does NOT provide:
- Runtime validation (use Pydantic for that)
- Automatic form generation in Viewer UI
- JSON Schema generation

Use TypedDict when:
- You need static type checking without runtime overhead
- You're working with simple dict-based APIs
- Documentation purposes (code readability)

Use Pydantic when:
- You need runtime validation
- You want automatic Viewer UI form generation
- You need JSON Schema for API docs
"""

from typing import TypedDict

from edda import EddaApp, WorkflowContext, activity, workflow

# ========== TypedDict Definitions ==========


class UserInput(TypedDict):
    """User input data (TypedDict for static typing only)."""

    user_id: str
    name: str
    email: str
    age: int


class UserResult(TypedDict):
    """User creation result (TypedDict for static typing only)."""

    user_id: str
    name: str
    status: str
    created_at: str


class OrderInput(TypedDict):
    """Order input data (TypedDict for static typing only)."""

    order_id: str
    product_name: str
    quantity: int
    price: float


class OrderResult(TypedDict):
    """Order processing result (TypedDict for static typing only)."""

    order_id: str
    total_amount: float
    status: str
    processed_at: str


# ========== Kairo App Setup ==========

app = EddaApp(
    service_name="typeddict-example-service",
    db_url="sqlite:///typeddict_example.db",
)


# ========== Activities ==========


@activity
async def create_user(ctx: WorkflowContext, user_data: dict) -> dict:  # noqa: ARG001
    """
    Create a user (TypedDict annotated as dict at runtime).

    Note: user_data is annotated as dict because TypedDict is just
    a static type hint - at runtime it's a regular dict.
    """
    print(f"[Activity] Creating user: {user_data}")
    # No automatic validation - user_data could be missing fields!
    return {
        "user_id": user_data["user_id"],
        "name": user_data["name"],
        "status": "created",
        "created_at": "2025-11-04T00:00:00Z",
    }


@activity
async def process_order(ctx: WorkflowContext, order_data: dict) -> dict:  # noqa: ARG001
    """
    Process an order (TypedDict annotated as dict at runtime).

    Note: order_data is annotated as dict because TypedDict is just
    a static type hint - at runtime it's a regular dict.
    """
    print(f"[Activity] Processing order: {order_data}")
    total_amount = order_data["quantity"] * order_data["price"]
    return {
        "order_id": order_data["order_id"],
        "total_amount": total_amount,
        "status": "processed",
        "processed_at": "2025-11-04T00:00:00Z",
    }


# ========== Workflows ==========


@workflow(event_handler=True)
async def typeddict_user_workflow(ctx: WorkflowContext, input: dict) -> dict:
    """
    User creation workflow using TypedDict.

    Note: Edda automatically generates activity IDs for sequential execution.

    CloudEvent type: "typeddict_user_workflow"

    Example CloudEvent data:
    {
        "user_id": "USER-123",
        "name": "Alice",
        "email": "alice@example.com",
        "age": 30
    }

    LIMITATIONS:
    - No runtime validation (invalid data will cause errors)
    - No automatic form generation in Viewer UI
    - Input must be provided as raw JSON in Viewer

    To start from Viewer:
    1. Click "Start New Workflow"
    2. Select "typeddict_user_workflow"
    3. You'll see a "json" textarea (no automatic form fields)
    4. Enter JSON manually:
       {"user_id": "USER-123", "name": "Alice", "email": "alice@example.com", "age": 30}
    """
    print(f"\n{'='*60}")
    print("[Workflow] TypedDict User Workflow")
    print(f"[Workflow] Input: {input}")
    print(f"{'='*60}\n")

    # For static type checking, we can cast to TypedDict
    # (This is only for mypy - at runtime it's still a dict)
    user_input: UserInput = input  # type: ignore

    # Create user (Activity ID auto-generated: "create_user:1")
    user_result = await create_user(ctx, user_input)
    print(f"[Workflow] User created: {user_result}")

    print(f"\n{'='*60}")
    print("[Workflow] Workflow completed!")
    print(f"{'='*60}\n")

    return user_result


@workflow(event_handler=True)
async def typeddict_order_workflow(ctx: WorkflowContext, input: dict) -> dict:
    """
    Order processing workflow using TypedDict.

    Note: Edda automatically generates activity IDs for sequential execution.

    CloudEvent type: "typeddict_order_workflow"

    Example CloudEvent data:
    {
        "order_id": "ORDER-456",
        "product_name": "Widget",
        "quantity": 5,
        "price": 19.99
    }

    LIMITATIONS:
    - No runtime validation (invalid data will cause errors)
    - No automatic form generation in Viewer UI
    - Input must be provided as raw JSON in Viewer
    """
    print(f"\n{'='*60}")
    print("[Workflow] TypedDict Order Workflow")
    print(f"[Workflow] Input: {input}")
    print(f"{'='*60}\n")

    # For static type checking, we can cast to TypedDict
    order_input: OrderInput = input  # type: ignore

    # Process order (Activity ID auto-generated: "process_order:1")
    order_result = await process_order(ctx, order_input)
    print(f"[Workflow] Order processed: {order_result}")

    print(f"\n{'='*60}")
    print("[Workflow] Workflow completed!")
    print(f"{'='*60}\n")

    return order_result


# ========== Comparison: TypedDict vs Pydantic ==========
#
# TypedDict:
# - ✅ Static type checking (mypy, IDE completion)
# - ✅ Zero runtime overhead
# - ✅ Simple, no dependencies
# - ❌ No runtime validation
# - ❌ No automatic Viewer UI form generation
# - ❌ No JSON Schema generation
# - ❌ Manual JSON input required in Viewer
#
# Pydantic:
# - ✅ Static type checking (mypy, IDE completion)
# - ✅ Runtime validation with clear error messages
# - ✅ Automatic Viewer UI form generation
# - ✅ JSON Schema generation
# - ✅ Field-level constraints (min/max, regex, etc.)
# - ✅ Custom validators
# - ❌ Small runtime overhead (usually negligible)
#
# RECOMMENDATION: Use Pydantic for Kairo workflows unless you have
# a specific reason to use TypedDict (e.g., extreme performance
# requirements, compatibility with existing dict-based code).


# Export ASGI application
application = app
