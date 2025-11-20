# Workflows and Activities

This guide covers the basics of creating workflows and activities in Edda.

## The `@workflow` Decorator

The `@workflow` decorator marks a function as a workflow orchestrator.

### Basic Usage

```python
from edda import workflow, WorkflowContext

@workflow
async def my_workflow(ctx: WorkflowContext, param1: str, param2: int):
    """A simple workflow"""
    # Orchestration logic here
    result = await some_activity(ctx, param1)
    return {"result": result, "param2": param2}
```

###  Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `event_handler` | `bool` | `False` | If `True`, automatically registers as CloudEvents handler |

### Starting Workflows

```python
# Start a workflow programmatically
instance_id = await my_workflow.start(param1="hello", param2=42)

# Start with Pydantic model
instance_id = await my_workflow.start(data=MyInput(...))
```

### CloudEvents Auto-Registration (Opt-in)

By default, workflows are NOT automatically registered as CloudEvents handlers (security).

To enable auto-registration:

```python
@workflow(event_handler=True)
async def order_workflow(ctx: WorkflowContext, order_id: str):
    # This workflow will automatically handle CloudEvents
    # with type="order_workflow"
    pass
```

**How it works:**

1. CloudEvent arrives with `type="order_workflow"`
2. Edda extracts `data` from the event
3. Workflow starts with `data` as parameters
4. Workflow instance ID is returned

**Security note:** Only use `event_handler=True` for workflows you want publicly accessible via CloudEvents.

## The `@activity` Decorator

The `@activity` decorator marks a function as an activity that performs business logic.

### Basic Usage

```python
from edda import activity, WorkflowContext

@activity
async def send_email(ctx: WorkflowContext, email: str, subject: str):
    """Send an email (business logic)"""
    # Call external service
    response = await email_service.send(email, subject)
    return {"sent": True, "message_id": response.id}
```

### Automatic Transactions

Activities are automatically transactional:

```python
from edda import activity, WorkflowContext
from edda.outbox.transactional import send_event_transactional

@activity
async def create_order(ctx: WorkflowContext, order_id: str):
    # All operations in a single transaction:
    # 1. Activity execution
    # 2. History recording
    # 3. Event publishing (if using send_event_transactional)

    await send_event_transactional(
        ctx,
        event_type="order.created",
        event_source="order-service",
        event_data={"order_id": order_id}
    )
    return {"order_id": order_id}
```

### Custom Database Operations

For atomic operations with your own database tables:

```python
@activity
async def create_order_with_db(ctx: WorkflowContext, order_id: str):
    # Access Edda-managed session (same database as Edda)
    session = ctx.session

    # Your database operations
    order = Order(order_id=order_id)
    session.add(order)

    # Events in same transaction
    await send_event_transactional(ctx, "order.created", ...)

    # Edda automatically commits (or rolls back on exception)
    return {"order_id": order_id}
```

## Activity IDs and Deterministic Replay

Edda automatically assigns IDs to activities for deterministic replay after crashes. Understanding when to use manual IDs vs. auto-generated IDs is important.

### Auto-Generated IDs (Default - Recommended)

For **sequential execution**, Edda automatically generates IDs in the format `"{function_name}:{counter}"`:

```python
@workflow
async def my_workflow(ctx: WorkflowContext, order_id: str):
    # Auto-generated IDs: "validate:1", "process:1", "notify:1"
    result1 = await validate(ctx, order_id)    # "validate:1"
    result2 = await process(ctx, order_id)      # "process:1"
    result3 = await notify(ctx, order_id)       # "notify:1"
    return {"status": "completed"}
```

**How it works:**

- First call to `validate()` → `"validate:1"`
- Second call to `validate()` → `"validate:2"`
- First call to `process()` → `"process:1"`

**Even with conditional branches**, auto-generation works correctly:

```python
@workflow
async def loan_approval(ctx: WorkflowContext, applicant_id: str):
    credit_score = await check_credit(ctx, applicant_id)  # "check_credit:1"

    if credit_score >= 700:
        result = await approve(ctx, applicant_id)    # "approve:1"
    else:
        result = await reject(ctx, applicant_id)     # "reject:1"

    return result
```

### Manual IDs (Required for Concurrent Execution)

Manual `activity_id` specification is **required ONLY** for concurrent execution:

```python
import asyncio

@workflow
async def concurrent_workflow(ctx: WorkflowContext, urls: list[str]):
    # Manual IDs required for asyncio.gather
    results = await asyncio.gather(
        fetch_data(ctx, urls[0], activity_id="fetch_data:1"),
        fetch_data(ctx, urls[1], activity_id="fetch_data:2"),
        fetch_data(ctx, urls[2], activity_id="fetch_data:3"),
    )
    return {"results": results}
```

**When manual IDs are required:**

- `asyncio.gather()` - Multiple activities executed concurrently
- `async for` loops - Dynamic parallel execution
- Any scenario where execution order is non-deterministic

### Best Practices

✅ **Do:** Rely on auto-generation for sequential execution
```python
result1 = await activity_one(ctx, data)
result2 = await activity_two(ctx, data)
```

❌ **Don't:** Manually specify IDs for sequential execution
```python
# Unnecessary - adds noise
result1 = await activity_one(ctx, data, activity_id="activity_one:1")
result2 = await activity_two(ctx, data, activity_id="activity_two:1")
```

For more details, see [MIGRATION_GUIDE_ACTIVITY_ID.md](../../MIGRATION_GUIDE_ACTIVITY_ID.md).

## Workflow vs. Activity: When to Use Which?

### Use `@workflow` for:

- ✅ Orchestrating multiple steps
- ✅ Coordinating activities
- ✅ Defining business processes
- ✅ Decision logic (if/else, loops)

**Example:**

```python
@workflow
async def user_onboarding(ctx: WorkflowContext, user_id: str):
    # Orchestration logic
    account = await create_account(ctx, user_id)
    await send_welcome_email(ctx, account["email"])
    await setup_preferences(ctx, user_id)
    return {"status": "completed"}
```

### Use `@activity` for:

- ✅ Database writes
- ✅ API calls
- ✅ File I/O
- ✅ External service calls
- ✅ Any side-effecting operation

**Example:**

```python
@activity
async def create_account(ctx: WorkflowContext, user_id: str):
    # Business logic
    account = await db.create_user(user_id)
    return {"account_id": account.id, "email": account.email}
```

## Complete Example

Here's a complete example showing workflows and activities together:

```python
from edda import EddaApp, workflow, activity, WorkflowContext
from pydantic import BaseModel, Field

# Data models
class UserInput(BaseModel):
    user_id: str
    email: str = Field(..., pattern=r"^[^@]+@[^@]+\.[^@]+$")
    name: str

class UserResult(BaseModel):
    user_id: str
    account_id: str
    status: str

# Activities
@activity
async def create_database_record(
    ctx: WorkflowContext,
    user_id: str,
    email: str,
    name: str
) -> dict:
    """Create user record in database"""
    print(f"Creating user {user_id} in database")
    # Simulate database write
    return {
        "account_id": f"ACC-{user_id}",
        "email": email,
        "name": name
    }

@activity
async def send_welcome_email(
    ctx: WorkflowContext,
    email: str,
    name: str
) -> dict:
    """Send welcome email to user"""
    print(f"Sending welcome email to {email}")
    # Simulate email service
    return {"sent": True, "email": email}

@activity
async def create_user_profile(
    ctx: WorkflowContext,
    account_id: str,
    name: str
) -> dict:
    """Create user profile with default settings"""
    print(f"Creating profile for {account_id}")
    # Simulate profile creation
    return {
        "profile_id": f"PROF-{account_id}",
        "settings": {"theme": "light", "notifications": True}
    }

# Workflow
@workflow
async def user_registration_workflow(
    ctx: WorkflowContext,
    data: UserInput
) -> UserResult:
    """
    Complete user registration workflow.

    Steps:
    1. Create database record
    2. Send welcome email
    3. Create user profile
    """

    # Step 1: Database record
    account = await create_database_record(
        ctx,
        data.user_id,
        data.email,
        data.name
    )

    # Step 2: Welcome email
    await send_welcome_email(
        ctx,
        account["email"],
        account["name"]
    )

    # Step 3: User profile
    profile = await create_user_profile(
        ctx,
        account["account_id"],
        account["name"]
    )

    return UserResult(
        user_id=data.user_id,
        account_id=account["account_id"],
        status="completed"
    )

# Main
async def main():
    app = EddaApp(service_name="user-service", db_url="sqlite:///users.db")
    await app.initialize()  # Initialize before starting workflows

    # Start workflow
    instance_id = await user_registration_workflow.start(
        data=UserInput(
            user_id="user_123",
            email="user@example.com",
            name="John Doe"
        )
    )

    print(f"Workflow started: {instance_id}")
```

## Best Practices

### 1. Keep Workflows Simple

✅ **Good:**

```python
@workflow
async def process_order(ctx: WorkflowContext, order_id: str):
    inventory = await reserve_inventory(ctx, order_id)
    payment = await process_payment(ctx, inventory["total"])
    await ship_order(ctx, order_id)
    return {"status": "completed"}
```

❌ **Bad:**

```python
@workflow
async def process_order(ctx: WorkflowContext, order_id: str):
    # Don't put business logic in workflows!
    inventory_data = await db.query("SELECT ...")  # ❌
    total = sum(item["price"] for item in inventory_data)  # ❌
    await external_api.call(...)  # ❌
    return {"status": "completed"}
```

### 2. Activities Should Be Focused

✅ **Good:**

```python
@activity
async def send_email(ctx: WorkflowContext, email: str, subject: str):
    # Single responsibility: send email
    response = await email_service.send(email, subject)
    return {"sent": True}
```

❌ **Bad:**

```python
@activity
async def send_email_and_update_db_and_log(ctx: WorkflowContext, ...):
    # Too many responsibilities!
    await email_service.send(...)
    await db.update(...)
    await logger.log(...)
    # Break this into 3 separate activities!
```

### 3. Use Pydantic Models

✅ **Good:**

```python
class OrderInput(BaseModel):
    order_id: str = Field(..., pattern=r"^ORD-\d+$")
    amount: float = Field(..., gt=0)

@workflow
async def order_workflow(ctx: WorkflowContext, data: OrderInput):
    # Type-safe, validated input
    pass
```

❌ **Bad:**

```python
@workflow
async def order_workflow(ctx: WorkflowContext, order_id: str, amount: float):
    # No validation, prone to errors
    pass
```

### 4. Don't Mix Async/Sync

❌ **Bad:**

```python
@activity
def sync_activity(ctx: WorkflowContext, param: str):  # ❌ Not async!
    # This won't work!
    pass
```

✅ **Good:**

```python
@activity
async def async_activity(ctx: WorkflowContext, param: str):  # ✅ Async
    # Correct!
    pass
```

## Next Steps

- **[Durable Execution](durable-execution/replay.md)**: Learn how Edda ensures workflows never lose progress
- **[Saga Pattern](saga-compensation.md)**: Automatic compensation on failure
- **[Event Handling](events/wait-event.md)**: Wait for external events in workflows
- **[Examples](../examples/simple.md)**: See workflows and activities in action
