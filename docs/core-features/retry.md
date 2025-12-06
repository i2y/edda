# Automatic Activity Retry

Edda provides comprehensive automatic retry functionality for activities with exponential backoff, ensuring resilience against transient failures.

## Overview

When an activity fails (e.g., network timeout, temporary service unavailability), Edda automatically retries the activity with exponential backoff before marking the workflow as failed. This built-in resilience eliminates the need for manual retry logic in your business code.

### Key Features

- ✅ **Automatic**: Activities retry without manual retry logic
- ✅ **Exponential Backoff**: Delays increase exponentially to avoid overwhelming failing services
- ✅ **Configurable**: Per-activity or application-wide retry policies
- ✅ **Observable**: Retry metadata automatically recorded in workflow history
- ✅ **Selective**: Use `TerminalError` for non-retryable errors
- ✅ **Graceful**: Handle retry exhaustion with `RetryExhaustedError`

## How Retry Works

### Retry Loop Architecture

```
Activity Execution
    ↓
[Try]
    ↓
Execute activity → Success? → Record result → Return ✅
    ↓
    No (Exception)
    ↓
Check retry policy
    ↓
Should retry? → No → Record failure → Raise RetryExhaustedError ❌
    ↓
    Yes
    ↓
Calculate backoff delay (exponential)
    ↓
Wait (asyncio.sleep)
    ↓
[Try again] → (Loop back to Execute)
```

### Transaction Boundaries

**Important**: The retry loop is **outside** the transaction. Each retry attempt is an **independent transaction**.

```python
# Pseudocode (simplified)
attempt = 0
while True:
    attempt += 1
    try:
        # Each attempt is in its own transaction
        async with ctx.transaction():
            result = await activity_function(...)
            # Record success
            return result
    except Exception as error:
        # Check if should retry
        if not should_retry(error, attempt):
            # Record failure
            raise RetryExhaustedError(...) from error

        # Calculate backoff and retry
        delay = calculate_delay(attempt)
        await asyncio.sleep(delay)
```

**Benefits:**

- ✅ Failed attempts don't leave partial state in database
- ✅ Each retry is a fresh attempt with clean transaction
- ✅ Automatic rollback on failure

## Default Retry Policy

By default, activities retry **5 times** with exponential backoff:

```python
from edda import activity, WorkflowContext

@activity  # Uses default retry policy
async def call_external_api(ctx: WorkflowContext, url: str):
    response = await http_client.get(url)
    return {"data": response.json()}
```

### Default Schedule

| Attempt | Delay Before Retry | Total Time Elapsed |
|---------|-------------------|-------------------|
| 1 | 0s (immediate) | 0s |
| 2 | 1s | 1s |
| 3 | 2s | 3s |
| 4 | 4s | 7s |
| 5 | 8s | 15s |

**Default parameters:**
```python
RetryPolicy(
    max_attempts=5,
    initial_interval=1.0,        # 1 second
    backoff_coefficient=2.0,     # Exponential (2^n)
    max_interval=60.0,           # Cap at 60 seconds
    max_duration=300.0           # 5 minutes total
)
```

**Delay formula:**
```
delay = initial_interval * (backoff_coefficient ^ (attempt - 1))
delay = min(delay, max_interval)  # Capped
```

Example calculation:

- Attempt 2: `1.0 * (2.0 ^ 1) = 2.0s` → 1.0s (wait before attempt 2)
- Attempt 3: `1.0 * (2.0 ^ 2) = 4.0s` → 2.0s (wait before attempt 3)
- Attempt 4: `1.0 * (2.0 ^ 3) = 8.0s` → 4.0s (wait before attempt 4)

## Custom Retry Policies

Configure retry behavior per activity or application-wide.

### Activity-Level Policy

```python
from edda import activity, RetryPolicy, WorkflowContext

@activity(retry_policy=RetryPolicy(
    max_attempts=10,           # More attempts for critical operations
    initial_interval=0.5,      # Faster initial retry (500ms)
    backoff_coefficient=1.5,   # Slower exponential growth
    max_interval=30.0,         # Cap at 30 seconds
    max_duration=120.0         # Stop after 2 minutes total
))
async def process_payment(ctx: WorkflowContext, amount: float):
    """
    Critical payment processing with aggressive retry.

    Retry schedule:

    - Attempt 1: 0s
    - Attempt 2: 0.5s
    - Attempt 3: 0.75s (0.5 * 1.5)
    - Attempt 4: 1.125s (0.75 * 1.5)
    - ...
    - Up to 10 attempts or 120 seconds total
    """
    response = await payment_service.charge(amount)
    return {"transaction_id": response.id}
```

### Application-Level Policy

Set a default policy for **all** activities in your application:

```python
from edda import EddaApp, RetryPolicy

app = EddaApp(
    db_url="postgresql://localhost/workflows",
    default_retry_policy=RetryPolicy(
        max_attempts=7,
        initial_interval=2.0,
        max_interval=120.0
    )
)
```

### Policy Resolution Order

When an activity is executed, Edda resolves the retry policy in this order:

1. **Activity-level policy** (highest priority) - `@activity(retry_policy=...)`
2. **Application-level policy** - `EddaApp(default_retry_policy=...)`
3. **Framework default** - `RetryPolicy(max_attempts=5, ...)`

Example:

```python
# Application-level: 10 attempts
app = EddaApp(
    db_url="...",
    default_retry_policy=RetryPolicy(max_attempts=10)
)

# Activity A: Uses application-level (10 attempts)
@activity
async def activity_a(ctx: WorkflowContext):
    pass

# Activity B: Overrides with activity-level (3 attempts)
@activity(retry_policy=RetryPolicy(max_attempts=3))
async def activity_b(ctx: WorkflowContext):
    pass

# Activity C: Uses framework default (5 attempts)
# (if no application-level policy is set)
@activity
async def activity_c(ctx: WorkflowContext):
    pass
```

## RetryPolicy Parameters

Complete reference for all `RetryPolicy` parameters:

| Parameter | Type | Default | Description | Example |
|-----------|------|---------|-------------|---------|
| `max_attempts` | `int \| None` | `5` | Maximum retry attempts. `None` = infinite (use with caution) | `max_attempts=10` |
| `initial_interval` | `float` | `1.0` | First retry delay in seconds | `initial_interval=0.5` (500ms) |
| `backoff_coefficient` | `float` | `2.0` | Exponential backoff multiplier | `backoff_coefficient=1.5` (slower growth) |
| `max_interval` | `float` | `60.0` | Maximum retry delay in seconds (caps exponential growth) | `max_interval=30.0` |
| `max_duration` | `float \| None` | `300.0` | Maximum total retry time in seconds (5 minutes). `None` = infinite | `max_duration=120.0` (2 minutes) |
| `retryable_error_types` | `tuple[Type[Exception], ...]` | `(Exception,)` | Tuple of exception types to retry | `(ConnectionError, TimeoutError)` |
| `non_retryable_error_types` | `tuple[Type[Exception], ...]` | `()` | Tuple of exception types to never retry | `(ValueError, KeyError)` |

### Backoff Examples

#### Fast Retry (Low-Latency Services)

```python
RetryPolicy(
    max_attempts=10,
    initial_interval=0.1,      # 100ms
    backoff_coefficient=1.5,
    max_interval=10.0,         # 10 seconds max
    max_duration=60.0          # 1 minute total
)
```

Schedule: 100ms → 150ms → 225ms → 337ms → 506ms → ...

#### Slow Retry (Rate-Limited APIs)

```python
RetryPolicy(
    max_attempts=3,
    initial_interval=5.0,      # 5 seconds
    backoff_coefficient=2.0,
    max_interval=300.0,        # 5 minutes max
    max_duration=900.0         # 15 minutes total
)
```

Schedule: 5s → 10s → 20s (capped at 3 attempts)

#### Constant Delay

```python
RetryPolicy(
    max_attempts=5,
    initial_interval=2.0,
    backoff_coefficient=1.0,   # No exponential growth
    max_interval=2.0
)
```

Schedule: 2s → 2s → 2s → 2s → 2s

## Non-Retryable Errors

Use `TerminalError` for errors that should **never** be retried.

### When to Use TerminalError

- ✅ **Validation failures**: Invalid input, malformed data
- ✅ **Business rule violations**: Insufficient funds, order cancelled
- ✅ **Permanent errors**: Resource not found, access denied, authentication failed
- ❌ **Transient errors**: Network timeout, service unavailable (let these retry!)

### Example

```python
from edda import activity, TerminalError, WorkflowContext

@activity
async def validate_order(ctx: WorkflowContext, order_id: str, user_id: str):
    """Validate order before processing."""

    # Check if order exists
    order = await db.get_order(order_id)
    if not order:
        # Don't retry - order doesn't exist (permanent error)
        raise TerminalError(f"Order {order_id} not found")

    # Check if user is authorized
    user = await db.get_user(user_id)
    if not user or user.status == "banned":
        # Don't retry - business rule violation
        raise TerminalError(f"User {user_id} is not authorized")

    # Check if order is already processed
    if order.status in ["completed", "cancelled"]:
        # Don't retry - invalid state
        raise TerminalError(f"Order {order_id} is already {order.status}")

    return {"order_id": order_id, "valid": True}
```

### TerminalError Behavior

When `TerminalError` is raised:

1. ✅ Activity **immediately fails** (no retry)
2. ✅ Error is recorded in workflow history
3. ✅ Exception propagates to workflow
4. ✅ Workflow can catch and handle it

```python
@workflow
async def order_workflow(ctx: WorkflowContext, order_id: str):
    try:
        await validate_order(ctx, order_id, user_id="123")
    except TerminalError as e:
        # Handle non-retryable error gracefully
        await log_validation_failure(ctx, order_id, error=str(e))
        raise  # Re-raise to fail the workflow
```

## RetryExhaustedError

When all retry attempts fail, `RetryExhaustedError` is raised.

### Exception Chaining

`RetryExhaustedError` uses Python's exception chaining (`__cause__`) to preserve the original error:

```python
from edda import activity, RetryExhaustedError, WorkflowContext

@activity(retry_policy=RetryPolicy(max_attempts=3))
async def flaky_operation(ctx: WorkflowContext):
    raise ConnectionError("Network timeout")

@workflow
async def my_workflow(ctx: WorkflowContext):
    try:
        await flaky_operation(ctx)
    except RetryExhaustedError as e:
        print(f"RetryExhaustedError: {e}")
        # "Activity flaky_operation failed after 3 attempts: Max attempts (3) reached"

        print(f"Original error: {e.__cause__}")
        # ConnectionError("Network timeout")

        print(f"Error type: {type(e.__cause__).__name__}")
        # "ConnectionError"
```

### Handling RetryExhaustedError

Implement fallback logic when retries are exhausted:

```python
@workflow
async def resilient_workflow(ctx: WorkflowContext, order_id: str):
    try:
        # Attempt payment (retries automatically)
        payment = await process_payment(ctx, order_id)
        return {"status": "completed", "payment": payment}

    except RetryExhaustedError as e:
        # All retry attempts failed - execute fallback

        # 1. Log the failure
        await log_payment_failure(ctx, order_id, error=str(e))

        # 2. Notify support team
        await notify_support_team(ctx, order_id, error=str(e.__cause__))

        # 3. Mark order as payment_failed
        await update_order_status(ctx, order_id, status="payment_failed")

        # 4. Optionally re-raise to fail the workflow
        raise  # Workflow will be marked as "failed"
```

## Retry Metadata

Retry information is automatically recorded in workflow history for observability.

### Metadata Structure

```json
{
    "event_type": "ActivityCompleted",
    "activity_id": "process_payment:1",
    "event_data": {
        "activity_name": "process_payment",
        "result": {"transaction_id": "TXN-123"},
        "retry_metadata": {
            "total_attempts": 3,
            "total_duration_ms": 7200,
            "exhausted": false,
            "last_error": {
                "error_type": "ConnectionError",
                "message": "Payment gateway timeout"
            },
            "errors": [
                {
                    "attempt": 1,
                    "error_type": "ConnectionError",
                    "message": "Payment gateway timeout",
                    "timestamp_ms": 1699000000000
                },
                {
                    "attempt": 2,
                    "error_type": "ConnectionError",
                    "message": "Payment gateway timeout",
                    "timestamp_ms": 1699000001000
                }
            ]
        }
    }
}
```

### Metadata Fields

| Field | Type | Description |
|-------|------|-------------|
| `total_attempts` | `int` | Total number of attempts made (including successful) |
| `total_duration_ms` | `int` | Total time spent retrying (milliseconds) |
| `exhausted` | `bool` | `true` if max retries reached, `false` if succeeded |
| `last_error` | `dict` | Last error before success or exhaustion |
| `last_error.error_type` | `str` | Exception class name (e.g., `"ConnectionError"`) |
| `last_error.message` | `str` | Exception message |
| `errors` | `list[dict]` | Detailed info for each failed attempt |
| `errors[].attempt` | `int` | Attempt number (1-indexed) |
| `errors[].error_type` | `str` | Exception class name |
| `errors[].message` | `str` | Exception message |
| `errors[].timestamp_ms` | `int` | Unix timestamp (milliseconds) |

### Use Cases for Retry Metadata

#### 1. Monitoring and Alerting

```python
# Query workflow history and alert on high retry rates
history = await storage.get_history(instance_id)

for event in history:
    if event["event_type"] == "ActivityCompleted":
        metadata = event["event_data"].get("retry_metadata")
        if metadata and metadata["total_attempts"] > 3:
            # Alert: Activity required more than 3 attempts
            alert_ops_team(
                activity=event["event_data"]["activity_name"],
                attempts=metadata["total_attempts"],
                duration=metadata["total_duration_ms"]
            )
```

#### 2. Performance Analysis

```python
# Analyze average retry duration per activity
retry_durations = {}

for event in history:
    if "retry_metadata" in event["event_data"]:
        activity_name = event["event_data"]["activity_name"]
        duration = event["event_data"]["retry_metadata"]["total_duration_ms"]

        if activity_name not in retry_durations:
            retry_durations[activity_name] = []
        retry_durations[activity_name].append(duration)

# Calculate averages
for activity, durations in retry_durations.items():
    avg = sum(durations) / len(durations)
    print(f"{activity}: avg {avg}ms over {len(durations)} retries")
```

#### 3. Debugging Transient Failures

```python
# Inspect error patterns
for event in history:
    metadata = event["event_data"].get("retry_metadata")
    if metadata and not metadata["exhausted"]:
        # Activity succeeded after retries - investigate why
        print(f"Activity: {event['event_data']['activity_name']}")
        print(f"Attempts: {metadata['total_attempts']}")
        print(f"Errors:")
        for error in metadata["errors"]:
            print(f"  - Attempt {error['attempt']}: {error['message']}")
```

## Preset Retry Policies

Edda provides preset policies for common scenarios:

```python
from edda.retry import (
    DEFAULT_RETRY_POLICY,
    AGGRESSIVE_RETRY,
    CONSERVATIVE_RETRY,
    INFINITE_RETRY
)
```

### DEFAULT_RETRY_POLICY

```python
RetryPolicy(
    max_attempts=5,
    initial_interval=1.0,
    backoff_coefficient=2.0,
    max_interval=60.0,
    max_duration=300.0  # 5 minutes
)
```

**Use for**: General-purpose activities, most external API calls

### AGGRESSIVE_RETRY

```python
RetryPolicy(
    max_attempts=10,
    initial_interval=0.1,     # 100ms
    backoff_coefficient=1.5,
    max_interval=10.0,
    max_duration=60.0         # 1 minute
)
```

**Use for**: Low-latency services, critical operations, payment processing

### CONSERVATIVE_RETRY

```python
RetryPolicy(
    max_attempts=3,
    initial_interval=5.0,     # 5 seconds
    backoff_coefficient=2.0,
    max_interval=300.0,       # 5 minutes
    max_duration=900.0        # 15 minutes
)
```

**Use for**: Rate-limited APIs, batch operations, non-critical operations

### INFINITE_RETRY

```python
RetryPolicy(
    max_attempts=None,        # Infinite
    initial_interval=1.0,
    backoff_coefficient=2.0,
    max_interval=60.0,
    max_duration=None         # Infinite
)
```

⚠️ **Warning**: Use with extreme caution! Workflow may retry forever.

**Use for**: Truly critical operations that must succeed (e.g., financial transactions in regulated environments)

### Using Preset Policies

```python
from edda import activity
from edda.retry import AGGRESSIVE_RETRY

@activity(retry_policy=AGGRESSIVE_RETRY)
async def process_payment(ctx: WorkflowContext, amount: float):
    # Fast retries for payment API
    pass
```

## Retry vs. Crash Recovery

Understanding the difference between **retry** and **crash recovery** is crucial.

| Feature | Retry | Crash Recovery |
|---------|-------|----------------|
| **Trigger** | Activity failure (exception) | Process crash (infrastructure failure) |
| **Scope** | Single activity | Entire workflow |
| **Speed** | Immediate (seconds) | After lock timeout (5 minutes) |
| **Mechanism** | Retry loop with backoff | Deterministic replay from history |
| **Transaction** | Each attempt is new transaction | Replay skips completed activities |
| **Use case** | Transient errors (network timeout) | Infrastructure failures (container restart) |

### Example Scenario

```python
@workflow
async def order_workflow(ctx: WorkflowContext, order_id: str):
    # Step 1: Reserve inventory
    inventory = await reserve_inventory(ctx, order_id)

    # Step 2: Process payment (fails with network timeout)
    payment = await process_payment(ctx, inventory["total"])
    # ✅ RETRY: Immediate backoff (1s, 2s, 4s...) up to 5 attempts

    # 💥 Process crashes here (container killed)

    # Step 3: Ship order
    await ship_order(ctx, order_id)
    # ✅ CRASH RECOVERY: Workflow replays after 5 minutes
    # - Step 1 (reserve_inventory): Returns cached result (no re-execution)
    # - Step 2 (process_payment): Returns cached result (no re-execution)
    # - Step 3 (ship_order): Executes fresh (continues from checkpoint)
```

## Examples

See complete examples in:

- **[examples/retry_example.py](https://github.com/i2y/edda/blob/main/examples/retry_example.py)**: Comprehensive retry demonstrations
- **[examples/retry_with_compensation.py](https://github.com/i2y/edda/blob/main/examples/retry_with_compensation.py)**: Retry combined with compensation
- **[README.md](https://github.com/i2y/edda/blob/main/README.md)**: Quick start examples

## Next Steps

- **[Workflows and Activities](workflows-activities.md)**: Learn more about activity behavior
- **[Saga Pattern](saga-compensation.md)**: Automatic compensation on failure
- **[Durable Execution](durable-execution/replay.md)**: How Edda ensures workflows never lose progress
