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
| `lock_timeout_seconds` | `int \| None` | `None` | Lock timeout in seconds (uses global default 300s if `None`). See [Lock Timeout Customization](#lock-timeout-customization) |

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

### Sync Activities (WSGI Compatibility)

For WSGI environments (gunicorn, uWSGI) or legacy codebases, Edda supports synchronous activities:

```python
from edda import activity, WorkflowContext

@activity
def create_user_record(ctx: WorkflowContext, user_id: str, email: str) -> dict:
    """Sync activity - executed in thread pool"""
    # Traditional sync code - no async/await needed!
    user = User(user_id=user_id, email=email)
    db.session.add(user)
    db.session.commit()
    return {"user_id": user.id}

@activity
async def async_activity(ctx: WorkflowContext, data: str) -> dict:
    """Async activity - recommended for I/O operations"""
    result = await httpx.get(f"https://api.example.com/{data}")
    return result.json()

@workflow
async def mixed_workflow(ctx: WorkflowContext, user_id: str) -> dict:
    # Workflows are always async (for deterministic replay)
    # But can call both sync and async activities
    user = await create_user_record(ctx, user_id, "user@example.com")
    data = await async_activity(ctx, user_id)
    return {"user": user, "data": data}
```

**When to use sync activities:**

- ✅ Existing sync codebases (Flask, Django)
- ✅ WSGI deployments (gunicorn, uWSGI)
- ✅ Libraries without async support
- ✅ Simple CPU-bound operations

**Performance note:** Async activities are recommended for I/O-bound operations (database queries, HTTP requests, file I/O) for better performance. Sync activities are executed in a thread pool to avoid blocking the event loop.

### WSGI Deployment

For WSGI servers (gunicorn, uWSGI), use `create_wsgi_app()`:

```python
from edda import EddaApp
from edda.wsgi import create_wsgi_app

app = EddaApp(
    service_name="order-service",
    db_url="sqlite:///workflow.db",
)

# Create WSGI application
wsgi_application = create_wsgi_app(app)
```

Run with gunicorn:

```bash
gunicorn demo_app:wsgi_application --workers 4
```

Run with uWSGI:

```bash
uwsgi --http :8000 --wsgi-file demo_app.py --callable wsgi_application
```

## Retry Policies

Activities automatically retry on failure with exponential backoff. This provides resilience against transient failures like network timeouts or temporary service unavailability.

### Default Retry Behavior

By default, activities retry **5 times** with exponential backoff:

```python
@activity  # Default: 5 attempts with exponential backoff
async def call_payment_api(ctx: WorkflowContext, amount: float):
    response = await payment_service.charge(amount)
    return {"transaction_id": response.id}
```

**Default schedule:**

- Attempt 1: Immediate execution
- Attempt 2: 1 second delay
- Attempt 3: 2 seconds delay
- Attempt 4: 4 seconds delay
- Attempt 5: 8 seconds delay

If all 5 attempts fail, `RetryExhaustedError` is raised.

### Custom Retry Policies

Configure retry behavior per activity using `RetryPolicy`:

```python
from edda import activity, RetryPolicy

@activity(retry_policy=RetryPolicy(
    max_attempts=10,           # More attempts for critical operations
    initial_interval=0.5,      # Faster initial retry (0.5 seconds)
    backoff_coefficient=1.5,   # Slower exponential growth
    max_interval=30.0,         # Cap delay at 30 seconds
    max_duration=120.0         # Stop after 2 minutes total
))
async def critical_payment_operation(ctx: WorkflowContext, order_id: str):
    # This activity retries aggressively (up to 10 times)
    response = await payment_service.process(order_id)
    return {"status": response.status}
```

**RetryPolicy parameters:**

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `max_attempts` | `int \| None` | `5` | Maximum retry attempts (`None` = infinite) |
| `initial_interval` | `float` | `1.0` | First retry delay (seconds) |
| `backoff_coefficient` | `float` | `2.0` | Exponential backoff multiplier |
| `max_interval` | `float` | `60.0` | Maximum retry delay (seconds) |
| `max_duration` | `float \| None` | `300.0` | Maximum total retry time (seconds) |

### Application-Level Default Policy

Set a default retry policy for all activities in your application:

```python
from edda import EddaApp, RetryPolicy

app = EddaApp(
    db_url="postgresql://localhost/workflows",
    default_retry_policy=RetryPolicy(
        max_attempts=10,
        initial_interval=2.0,
        max_interval=120.0
    )
)
```

**Policy resolution order:**

1. Activity-level policy (highest priority)
2. Application-level policy
3. Framework default (5 attempts)

### Non-Retryable Errors

Use `TerminalError` for errors that should **never** be retried:

```python
from edda import activity, TerminalError

@activity
async def validate_order(ctx: WorkflowContext, order_id: str):
    order = await db.get_order(order_id)

    if not order:
        # Don't retry - order doesn't exist
        raise TerminalError(f"Order {order_id} not found")

    if order.status == "cancelled":
        # Business rule violation - don't retry
        raise TerminalError(f"Order {order_id} is cancelled")

    return {"order_id": order_id, "valid": True}
```

**When to use `TerminalError`:**

- ✅ Validation failures (invalid input, malformed data)
- ✅ Business rule violations (insufficient funds, order cancelled)
- ✅ Permanent errors (resource not found, access denied)
- ❌ Transient errors (network timeout, service unavailable) - let these retry!

### Handling Retry Exhaustion

When all retry attempts fail, `RetryExhaustedError` is raised:

```python
from edda import workflow, activity, RetryExhaustedError

@workflow
async def order_processing(ctx: WorkflowContext, order_id: str):
    try:
        # Activity retries automatically (5 attempts by default)
        payment = await charge_customer(ctx, order_id)
    except RetryExhaustedError as e:
        # All retry attempts failed - handle gracefully
        await notify_admin(ctx, order_id, error=str(e))
        await cancel_order(ctx, order_id)
        raise  # Re-raise to fail the workflow

    return {"status": "completed"}
```

**`RetryExhaustedError` provides:**

- Original exception via `__cause__` (exception chaining)
- Retry metadata (total attempts, duration)
- All error messages from failed attempts

### Retry Metadata

Retry information is automatically recorded in workflow history for observability:

```python
{
    "event_type": "ActivityCompleted",
    "event_data": {
        "activity_name": "call_payment_api",
        "result": {"transaction_id": "txn_123"},
        "retry_metadata": {
            "total_attempts": 3,
            "total_duration_ms": 7200,
            "exhausted": false,
            "last_error": {
                "error_type": "ConnectionError",
                "message": "Network timeout"
            },
            "errors": [
                {"attempt": 1, "error_type": "ConnectionError", ...},
                {"attempt": 2, "error_type": "ConnectionError", ...}
            ]
        }
    }
}
```

**Use retry metadata for:**

- 📊 Monitoring retry patterns and failure rates
- 🐛 Debugging transient failures
- ⚡ Performance analysis (identifying slow external services)
- 🚨 Alerting on high retry rates

### Preset Retry Policies

Edda provides preset policies for common scenarios:

```python
from edda.retry import AGGRESSIVE_RETRY, CONSERVATIVE_RETRY, INFINITE_RETRY

# Fast retries for low-latency services
@activity(retry_policy=AGGRESSIVE_RETRY)
async def fast_api_call(ctx: WorkflowContext, url: str):
    # 10 attempts, 0.1s initial delay, 1 minute max
    pass

# Slow retries for rate-limited APIs
@activity(retry_policy=CONSERVATIVE_RETRY)
async def rate_limited_api(ctx: WorkflowContext, endpoint: str):
    # 3 attempts, 5s initial delay, 15 minutes max
    pass

# Infinite retries (Restate-style, use with caution)
@activity(retry_policy=INFINITE_RETRY)
async def critical_operation(ctx: WorkflowContext, data: dict):
    # Retries forever until success
    # Warning: Only use for truly critical operations
    pass
```

### Best Practices

1. **Use default retry for most activities** - The default policy (5 attempts, exponential backoff) handles most transient failures

2. **Use `TerminalError` for permanent failures** - Don't waste time retrying validation errors or business rule violations

3. **Customize retry for critical operations** - Payment processing, data consistency operations may need more aggressive retry

4. **Monitor retry metadata** - High retry rates indicate systemic issues (e.g., unreliable external service)

5. **Handle `RetryExhaustedError` gracefully** - Implement fallback logic (notifications, compensations) when retries fail

6. **Avoid infinite retry in production** - Use finite `max_attempts` and `max_duration` to prevent runaway retries

## Lock Timeout Customization

Control how long a workflow instance can hold a lock before it's considered stale and automatically released.

### Default Behavior

By default, workflow locks expire after **300 seconds (5 minutes)**. This prevents workflows from holding locks indefinitely if a worker crashes.

### Customization Levels

Lock timeout can be customized at three levels with the following priority (highest to lowest):

#### 1. Runtime Override (Highest Priority)

Specify timeout when starting a workflow:

```python
# Override timeout for this specific instance
instance_id = await my_workflow.start(
    user_id=123,
    lock_timeout_seconds=900  # 15 minutes
)
```

**Use case:** One-off long-running workflows that need more time

#### 2. Decorator-Level Default

Set a default timeout for all instances of a workflow:

```python
@workflow(lock_timeout_seconds=600)  # 10 minutes
async def long_running_workflow(ctx: WorkflowContext, data: str):
    # All instances of this workflow get 10-minute lock timeout
    result = await some_activity(ctx, data)
    return result
```

**Use case:** Workflows that consistently need longer execution time

#### 3. Global Default (Lowest Priority)

If not specified at runtime or decorator level, the global default of **300 seconds (5 minutes)** is used.

### Complete Example

```python
from edda import workflow, WorkflowContext

# Example 1: Decorator-level timeout (10 minutes)
@workflow(lock_timeout_seconds=600)
async def batch_processing(ctx: WorkflowContext, batch_id: str):
    # This workflow gets 10 minutes by default
    result = await process_large_batch(ctx, batch_id)
    return result

# Example 2: Default timeout (5 minutes)
@workflow
async def quick_workflow(ctx: WorkflowContext, data: str):
    # This workflow gets default 5 minutes
    result = await simple_task(ctx, data)
    return result

# Usage:
# Use decorator-level timeout (10 minutes)
await batch_processing.start(batch_id="batch_123")

# Override to 15 minutes for this specific instance
await batch_processing.start(
    batch_id="batch_456",
    lock_timeout_seconds=900
)

# Use default timeout (5 minutes)
await quick_workflow.start(data="hello")
```

### How It Works

When a workflow acquires a lock:

1. **Calculate expiry time**: `lock_expires_at = current_time + timeout_seconds`
2. **Store in database**: The absolute expiry time is saved to `lock_expires_at` column
3. **Background cleanup**: Every 60 seconds, stale locks (`lock_expires_at < now`) are automatically released
4. **Auto-resume**: Workflows with `status="running"` are automatically resumed after lock release

**Priority resolution:**

```python
# Pseudo-code showing priority order
actual_timeout = (
    runtime_timeout              # 1. Runtime (saga.start(lock_timeout_seconds=X))
    if runtime_timeout is not None
    else decorator_timeout       # 2. Decorator (@workflow(lock_timeout_seconds=Y))
    if decorator_timeout is not None
    else 300                     # 3. Global default
)
```

### Best Practices

1. **Use default (5 minutes) for most workflows** - Sufficient for typical operations
2. **Use decorator-level for consistently long workflows** - Batch processing, report generation
3. **Use runtime override sparingly** - Only for exceptional cases that need more time
4. **Don't set too high** - Higher timeouts delay crash recovery (max 60s to 5min typical range)
5. **Monitor lock expiry** - If workflows frequently hit timeout, optimize activity execution time

### Related Documentation

- **[Replay Mechanism](durable-execution/replay.md)**: How workflows resume from crashes and stale locks

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

## Long-Running Loops with `recur()`

For workflows with infinite loops or long-running iterations, Edda provides `ctx.recur()` to prevent unbounded history growth. This is similar to Erlang's tail recursion pattern.

### The Problem

In long-running loops, every activity adds an entry to the workflow history. After thousands of iterations, this causes:

- **Memory issues**: Loading history for replay consumes increasing memory
- **Performance degradation**: Replay time grows with O(N) history entries
- **Storage growth**: Database size increases continuously

```python
# ❌ Problematic: History grows forever
@workflow
async def notification_service(ctx: WorkflowContext):
    await subscribe(ctx, "order.completed", mode="broadcast")

    while True:
        msg = await receive(ctx, "order.completed")
        await send_notification(ctx, msg.data)
        # After 10,000 iterations: 10,000+ history entries!
```

### The Solution: `ctx.recur()`

Use `ctx.recur()` to restart the workflow with fresh history while preserving state:

```python
# ✅ Good: Reset history periodically
@workflow
async def notification_service(ctx: WorkflowContext, processed_count: int = 0):
    await subscribe(ctx, "order.completed", mode="broadcast")

    count = 0
    while True:
        msg = await receive(ctx, "order.completed")
        await send_notification(ctx, msg.data, activity_id=f"notify:{msg.id}")

        count += 1
        if count >= 1000:
            # Reset history every 1000 iterations
            await ctx.recur(processed_count=processed_count + count)
            # Code after recur() is never executed
```

### How It Works

When `ctx.recur()` is called:

1. **Current workflow completes** with status `"recurred"`
2. **History is archived** (moved to archive table, not deleted)
3. **New workflow instance starts** with provided arguments
4. **Chain is tracked** via `continued_from` field

```
┌─────────────────────────────────────────────────────────────────────┐
│ Instance 1 (processed_count=0)                                      │
│ ├─ Activity 1...1000                                                │
│ └─ Status: "recurred" → Archive history → Instance 2 starts         │
├─────────────────────────────────────────────────────────────────────┤
│ Instance 2 (processed_count=1000, continued_from=Instance 1)        │
│ ├─ Activity 1...1000                                                │
│ └─ Status: "recurred" → Archive history → Instance 3 starts         │
├─────────────────────────────────────────────────────────────────────┤
│ Instance 3 (processed_count=2000, continued_from=Instance 2)        │
│ └─ ... continues ...                                                │
└─────────────────────────────────────────────────────────────────────┘
```

### API Reference

```python
async def recur(self, **kwargs: Any) -> None:
    """
    Restart the workflow with fresh history.

    Args:
        **kwargs: Arguments to pass to the new workflow instance.
                 These become the input parameters for the next iteration.

    Raises:
        RecurException: Always raised to signal the ReplayEngine.
                       This exception should not be caught by user code.
    """
```

### Complete Example

```python
from edda import workflow, WorkflowContext, subscribe, receive
from pydantic import BaseModel

class ServiceState(BaseModel):
    total_processed: int = 0
    errors_count: int = 0

@workflow
async def event_processor(ctx: WorkflowContext, state: ServiceState):
    """
    Long-running event processor that resets history every 500 events.
    """
    await subscribe(ctx, "events", mode="broadcast")

    local_count = 0
    local_errors = 0

    while True:
        try:
            msg = await receive(ctx, channel="events")
            await process_event(ctx, msg.data, activity_id=f"process:{msg.id}")
            local_count += 1
        except Exception:
            local_errors += 1

        # Recur every 500 events to prevent unbounded history
        if local_count + local_errors >= 500:
            await ctx.recur(state=ServiceState(
                total_processed=state.total_processed + local_count,
                errors_count=state.errors_count + local_errors,
            ))
```

### Important Notes

1. **Channel subscriptions are NOT transferred** - You must re-subscribe to channels in the new workflow iteration
2. **Compensations are cleared** - Registered compensations do not carry over
3. **History is archived, not deleted** - Old history is preserved for auditing
4. **Code after `recur()` is never executed** - Always place recur at the end of a branch

### When to Use

✅ **Good use cases:**

- Event processors that run indefinitely
- Message queue consumers
- Polling workers
- Long-running monitoring services

❌ **Not needed for:**

- Workflows that complete after a few activities
- One-shot batch processes
- Workflows with bounded iterations

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

### 4. Choose Async or Sync Appropriately

✅ **Preferred: Async activities** (better performance for I/O)

```python
@activity
async def fetch_user_data(ctx: WorkflowContext, user_id: str) -> dict:
    # Async I/O operations (recommended)
    result = await httpx.get(f"https://api.example.com/users/{user_id}")
    return result.json()
```

✅ **Valid: Sync activities** (WSGI compatibility, legacy code)

```python
@activity
def process_legacy_data(ctx: WorkflowContext, data: str) -> dict:
    # Sync operations (executed in thread pool)
    result = legacy_library.process(data)  # No async support
    return {"processed": result}
```

✅ **Good: Mix sync and async in same workflow**

```python
@workflow
async def order_workflow(ctx: WorkflowContext, order_id: str) -> dict:
    # Both sync and async activities work fine
    user = await create_user_record(ctx, order_id)  # Sync
    payment = await process_payment(ctx, 99.99)  # Async
    return {"user": user, "payment": payment}
```

**Performance tip**: Prefer async activities for I/O-bound operations (database queries, HTTP requests, file I/O). Use sync activities when integrating with legacy code or libraries without async support.

## Next Steps

- **[Durable Execution](durable-execution/replay.md)**: Learn how Edda ensures workflows never lose progress
- **[Saga Pattern](saga-compensation.md)**: Automatic compensation on failure
- **[Event Handling](events/wait-event.md)**: Wait for external events in workflows
- **[Examples](../examples/simple.md)**: See workflows and activities in action
