# Core Concepts

This guide introduces the fundamental concepts of Edda: workflows, activities, durable execution, and the Saga pattern.

## Workflows vs. Activities

Edda separates orchestration logic (**workflows**) from business logic (**activities**).

### Activities

**Activity**: A unit of work that performs business logic.

```python
from edda import activity, WorkflowContext

@activity
async def send_email(ctx: WorkflowContext, email: str, message: str):
    """Business logic - sends an actual email"""
    # Call external email service
    response = await email_service.send(email, message)
    return {"sent": True, "message_id": response.id}
```

**Key characteristics:**

- ✅ Execute business logic (database writes, API calls, file I/O)
- ✅ Activity return values are automatically saved to history for deterministic replay
- ✅ On replay, return cached results from history (idempotent)
- ✅ Automatically transactional (by default)

### Workflows

**Workflow**: Orchestration logic that coordinates activities.

```python
from edda import workflow, WorkflowContext

@workflow
async def user_signup(ctx: WorkflowContext, email: str, name: str):
    """Orchestration logic - coordinates activities"""
    # Step 1: Create user account
    user = await create_account(ctx, email, name)

    # Step 2: Send welcome email
    await send_email(ctx, email, f"Welcome, {name}!")

    # Step 3: Initialize user settings
    await setup_default_settings(ctx, user["user_id"])

    return {"user_id": user["user_id"], "status": "active"}
```

**Key characteristics:**

- ✅ Coordinate activities (orchestration, not business logic)
- ✅ Can be replayed from history after crashes
- ✅ Deterministic replay - workflow replays the same execution path using saved activity results
- ✅ Resume from the last checkpoint automatically

## Durable Execution

Edda ensures workflow progress is never lost through **deterministic replay**.

### How It Works

```python
@workflow
async def process_order(ctx: WorkflowContext, order_id: str):
    # Step 1: Reserve inventory
    reservation = await reserve_inventory(ctx, order_id)  # ✅ Saved to history

    # 💥 Process crashes here!

    # Step 2: Charge payment
    payment = await charge_payment(ctx, order_id)

    return {"order_id": order_id, "status": "completed"}
```

**On crash recovery:**

1. **Workflow restarts** from the beginning
2. **Step 1 (reserve_inventory)**: Returns cached result from history (does NOT execute again)
3. **Step 2 (charge_payment)**: Executes fresh (continues from checkpoint)

### Key Guarantees

- ✅ Activities execute **exactly once** (results cached in history)
- ✅ Workflows survive **arbitrary crashes** (process restarts, container failures, etc.)
- ✅ No manual checkpoint management required
- ✅ Deterministic replay - predictable behavior

### Replay Example

```python
@workflow
async def long_running_workflow(ctx: WorkflowContext, user_id: str):
    print(f"Step 0: Starting workflow for {user_id}")

    # Step 1: Create user (1 second)
    result1 = await create_user(ctx, user_id)
    print("Step 1: User created")

    # Step 2: Send email (2 seconds)
    result2 = await send_welcome_email(ctx, result1["email"])
    print("Step 2: Email sent")

    # Step 3: Setup profile (1 second)
    result3 = await setup_profile(ctx, user_id)
    print("Step 3: Profile setup")

    return result3
```

**First run (crashes after Step 2):**

```
Step 0: Starting workflow for user_123
Step 1: User created
Step 2: Email sent
💥 Crash!
```

**Second run (replay):**

```
Step 0: Starting workflow for user_123
Step 2: Email sent  # Steps 0-1 replayed from history (instant)
Step 3: Profile setup  # Fresh execution
```

Steps 0 and 1 are **replayed from history** without re-executing the activities.

## Automatic Activity Retry

Edda automatically retries failed activities with exponential backoff, ensuring resilience against transient failures.

### Default Retry Behavior

By default, activities retry **5 times** with exponential backoff:

```python
from edda import activity, WorkflowContext

@activity  # Automatically retries 5 times (default)
async def call_external_api(ctx: WorkflowContext, url: str):
    response = await http_client.get(url)
    return {"data": response.json()}
```

**Default retry schedule:**
- Attempt 1: Immediate
- Attempt 2: 1 second delay
- Attempt 3: 2 seconds delay
- Attempt 4: 4 seconds delay
- Attempt 5: 8 seconds delay

### Retry vs. Crash Recovery

**Important distinction:**

| Feature | Retry | Crash Recovery |
|---------|-------|----------------|
| **Trigger** | Activity failure | Process crash |
| **Scope** | Single activity | Entire workflow |
| **Speed** | Immediate (seconds) | After lock timeout (5 minutes) |
| **Use case** | Transient errors | Infrastructure failures |

**Example:**

```python
@workflow
async def resilient_workflow(ctx: WorkflowContext, user_id: str):
    # Step 1: Call external API (retries automatically on network errors)
    user_data = await fetch_user_data(ctx, user_id)
    # ✅ Retry: Immediate exponential backoff (max 5 attempts)

    # 💥 Process crashes here

    # Step 2: Save to database
    await save_user(ctx, user_data)
    # ✅ Crash Recovery: Workflow replays from history (Step 1 cached)
```

### Custom Retry Policy

Configure retry behavior per activity:

```python
from edda import activity, RetryPolicy

@activity(retry_policy=RetryPolicy(
    max_attempts=10,           # More attempts for critical operations
    initial_interval=0.5,      # Faster initial retry
    backoff_coefficient=1.5,   # Slower exponential growth
    max_interval=30.0,         # Cap at 30 seconds
    max_duration=120.0         # Stop retrying after 2 minutes total
))
async def critical_payment_api(ctx: WorkflowContext, amount: float):
    # Retry aggressively for payment operations
    response = await payment_service.charge(amount)
    return {"transaction_id": response.id}
```

### Non-Retryable Errors

Use `TerminalError` for errors that should **not** be retried:

```python
from edda import activity, TerminalError, WorkflowContext

@activity
async def validate_user(ctx: WorkflowContext, user_id: str):
    user = await fetch_user(user_id)

    if not user:
        # Don't retry - user doesn't exist
        raise TerminalError(f"User {user_id} not found")

    return {"user_id": user_id, "email": user["email"]}
```

**When to use `TerminalError`:**
- Validation failures (invalid input)
- Business rule violations (insufficient funds)
- Permanent errors (resource not found)

### Retry Exhaustion

When all retry attempts are exhausted, `RetryExhaustedError` is raised:

```python
from edda import activity, RetryExhaustedError, WorkflowContext

@workflow
async def order_workflow(ctx: WorkflowContext, order_id: str):
    try:
        # Activity retries up to 5 times
        payment = await charge_payment(ctx, order_id)
    except RetryExhaustedError as e:
        # All 5 attempts failed - execute fallback logic
        await notify_payment_team(ctx, order_id)
        raise  # Re-raise to fail the workflow
```

**`RetryExhaustedError` contains:**
- Original exception via `__cause__`
- Retry metadata (total_attempts, total_duration_ms)
- All error messages from each attempt

### Retry Metadata

Retry information is automatically recorded in workflow history:

```json
{
    "event_type": "ActivityCompleted",
    "event_data": {
        "activity_name": "call_external_api",
        "result": {"data": "..."},
        "retry_metadata": {
            "total_attempts": 3,
            "total_duration_ms": 7200,
            "exhausted": false,
            "last_error": {
                "error_type": "ConnectionError",
                "message": "Network timeout"
            }
        }
    }
}
```

**Use retry metadata for:**
- Observability and monitoring
- Debugging transient failures
- Performance analysis
- Alerting on retry patterns

## Control Flow in Workflows

Workflows can use standard Python control flow (if statements, loops, etc.), but understanding **deterministic replay** is crucial.

### Deterministic Conditions (Recommended)

**Best practice**: Base conditions on **activity return values** for deterministic replay.

```python
@workflow
async def order_workflow(ctx: WorkflowContext, order_id: str, amount: float):
    # ✅ Condition based on activity result (deterministic)
    payment = await process_payment(ctx, amount, activity_id="process_payment:1")

    if payment["status"] == "approved":
        # This path will always be taken on replay
        await ship_order(ctx, order_id, activity_id="ship_order:1")
    else:
        # Or this path will always be taken on replay
        await refund(ctx, order_id, activity_id="refund:1")

    return {"status": payment["status"]}
```

**Why it works**: Activity results are saved in history, so replay follows the **same execution path**.

### Non-Deterministic Conditions (Use Carefully)

Conditions based on **external state** may take **different paths** on replay.

```python
@workflow
async def inventory_workflow(ctx: WorkflowContext, item_id: str):
    # ⚠️ Direct database read (non-deterministic on replay)
    stock = await db.query("SELECT stock FROM inventory WHERE id = ?", item_id)

    if stock > 0:
        # This condition may evaluate differently on replay!
        # (if stock changed between original run and replay)
        await reserve_item(ctx, item_id, activity_id="reserve:1")
```

**Why it's non-deterministic**: Database state may have changed between original execution and replay.

### Intentional Non-Determinism (Valid Use Case)

Sometimes you **want** to re-evaluate on replay:

```python
@workflow
async def fresh_inventory_check(ctx: WorkflowContext, item_id: str):
    # ✅ Intentionally check current stock on every execution (including replay)
    # Use case: Always respect current inventory levels
    current_stock = await check_current_inventory(
        ctx, item_id, activity_id="check_stock:1"
    )

    if current_stock["available"] > 0:
        # Decision based on CURRENT state, not historical state
        await create_order(ctx, item_id, activity_id="create_order:1")
    else:
        await notify_out_of_stock(ctx, item_id, activity_id="notify:1")
```

**Key difference**: Wrap external checks in an **activity** so they're executed fresh on replay.

### Loops in Workflows

Loops work naturally with activity IDs:

```python
@workflow
async def batch_workflow(ctx: WorkflowContext, item_ids: list[str]):
    results = []

    for i, item_id in enumerate(item_ids):
        # Use dynamic activity_id for each iteration
        result = await process_item(
            ctx, item_id, activity_id=f"process_item:{i+1}"
        )
        results.append(result)

    return {"processed": len(results)}
```

### Best Practices

1. **Default to deterministic**: Base conditions on activity results
2. **Intentional non-determinism**: When needed, wrap external checks in activities
3. **Document intent**: Add comments when using non-deterministic patterns
4. **Dynamic activity IDs**: Use loop indices or item identifiers for unique IDs

## WorkflowContext

The `WorkflowContext` object provides access to workflow operations.

### Common Methods

```python
@activity
async def my_activity(ctx: WorkflowContext, param: str):
    # Get current step number (read-only property)
    step = ctx.current_step

    # Check if replaying
    if ctx.is_replaying:
        print("Replaying from history")

    return {"step": step, "param": param}
```

### Key Properties

| Property/Method | Description |
|----------------|-------------|
| `ctx.instance_id` | Workflow instance ID |
| `ctx.workflow_name` | Name of the workflow function |
| `ctx.is_replaying` | `True` if replaying from history |
| `ctx.current_step` | Get current step number (read-only) |
| `ctx.transaction()` | Create transactional context |
| `ctx.in_transaction()` | Check if in transaction |
| `ctx.session` | Access Edda's managed database session |

## The Saga Pattern

When a workflow fails, Edda automatically executes **compensation functions** for already-executed activities in **reverse order**.

This implements the [Saga pattern](https://microservices.io/patterns/data/saga.html) for distributed transaction rollback.

### Basic Compensation

```python
from edda import activity, on_failure, compensation, workflow, WorkflowContext

@compensation
async def cancel_reservation(ctx: WorkflowContext, item_id: str):
    """Compensation function - runs on workflow failure"""
    print(f"❌ Cancelled reservation for {item_id}")
    return {"cancelled": True}

@activity
@on_failure(cancel_reservation)
async def reserve_inventory(ctx: WorkflowContext, item_id: str):
    print(f"✅ Reserved {item_id}")
    return {"reserved": True, "item_id": item_id}

@workflow
async def order_workflow(ctx: WorkflowContext, item1: str, item2: str):
    await reserve_inventory(ctx, item1)  # Step 1
    await reserve_inventory(ctx, item2)  # Step 2
    await charge_payment(ctx)            # Step 3: Fails!

    return {"status": "completed"}
```

**Execution:**

```
✅ Reserved item1
✅ Reserved item2
💥 charge_payment fails!
❌ Cancelled reservation for item2  # Reverse order
❌ Cancelled reservation for item1
```

### Compensation Rules

1. **Reverse Order**: Compensations run in **reverse order** of activity execution
2. **Already-Executed Only**: Only activities that **completed successfully** are compensated
3. **Automatic**: No manual trigger required - Edda handles it

### Example: Multi-Step Rollback

```python
# Compensation functions
@compensation
async def undo_a(ctx: WorkflowContext):
    print("A: Undo")

@compensation
async def undo_b(ctx: WorkflowContext):
    print("B: Undo")

# Activities with compensation links
@activity
@on_failure(undo_a)
async def step_a(ctx: WorkflowContext):
    print("A: Execute")
    return {"status": "done"}

@activity
@on_failure(undo_b)
async def step_b(ctx: WorkflowContext):
    print("B: Execute")
    return {"status": "done"}

@activity
async def step_c(ctx: WorkflowContext):
    print("C: Execute")
    raise Exception("C failed!")

@workflow
async def saga_workflow(ctx: WorkflowContext):
    await step_a(ctx)
    await step_b(ctx)
    await step_c(ctx)  # Fails!
    return {"status": "completed"}
```

**Output:**

```
A: Execute
B: Execute
C: Execute
💥 Exception: C failed!
B: Undo  # Reverse order
A: Undo
```

## AI Agent Workflows

Edda is well-suited for orchestrating AI agent workflows that involve long-running tasks:

### Why Edda for AI Agents?

- **🔄 Durable LLM Calls**: Long-running LLM inference with automatic retry on failure
- **🧠 Multi-Step Reasoning**: Coordinate multiple AI tasks (research → analysis → synthesis)
- **🛠️ Tool Usage Workflows**: Orchestrate AI agents calling external tools/APIs with crash recovery
- **↩️ Compensation on Failure**: Automatically rollback AI agent actions when workflows fail

### Example: Research Agent Workflow

```python
from edda import workflow, activity, WorkflowContext

@activity
async def research_topic(ctx: WorkflowContext, topic: str) -> dict:
    """Call LLM to research a topic (may take minutes)"""
    result = await llm_client.generate(f"Research: {topic}")
    return {"research": result}

@activity
async def analyze_research(ctx: WorkflowContext, research: str) -> dict:
    """Analyze research results with another LLM call"""
    analysis = await llm_client.generate(f"Analyze: {research}")
    return {"analysis": analysis}

@activity
async def synthesize_report(ctx: WorkflowContext, analysis: str) -> dict:
    """Create final report"""
    report = await llm_client.generate(f"Report: {analysis}")
    return {"report": report}

@workflow
async def ai_research_workflow(ctx: WorkflowContext, topic: str):
    """Multi-step AI research workflow with automatic crash recovery"""
    # Step 1: Research (may take 2-3 minutes)
    research = await research_topic(ctx, topic)

    # Step 2: Analyze (if crash happens here, Step 1 won't re-run)
    analysis = await analyze_research(ctx, research["research"])

    # Step 3: Synthesize
    report = await synthesize_report(ctx, analysis["analysis"])

    return report
```

**Key benefits**:

- If the workflow crashes during Step 2, Step 1 (research) won't re-run - cached results are used
- Each LLM call is automatically retried on transient failures
- Workflow state is persisted, allowing multi-hour AI workflows to survive restarts
- Compensation functions can undo AI agent actions (e.g., delete created resources) on failure

## Distributed Execution

Multiple workers can safely process workflows.

### Multi-Worker Setup

```python
# Worker 1, Worker 2, Worker 3 (same code on different machines)
from edda import EddaApp

app = EddaApp(
    db_url="postgresql://yourdbinstance/workflows",  # Shared database
    service_name="order-service"
)
```

**Features:**

- ✅ **Exclusive Execution**: Only one worker can execute a workflow instance at a time
- ✅ **Stale Lock Cleanup**: Automatic cleanup of locks from crashed workers (5-minute timeout)
- ✅ **Automatic Resume**: Crashed workflows resume on any available worker

### How It Works

```
Worker 1: Tries to acquire lock for workflow instance A
         → Lock acquired ✅
         → Executes workflow

Worker 2: Tries to acquire lock for same instance A
         → Lock already held by Worker 1 ❌
         → Skips, moves to next instance

Worker 1: Crashes during execution 💥

Cleanup Task: Detects stale lock (5 minutes old)
             → Releases lock
             → Marks workflow for resume

Worker 3: Acquires lock for instance A
         → Replays from history
         → Completes workflow ✅
```

## Type Safety with Pydantic

Edda integrates with Pydantic v2 for type-safe workflows.

```python
from pydantic import BaseModel, Field
from edda import workflow, WorkflowContext

class OrderInput(BaseModel):
    order_id: str = Field(..., pattern=r"^ORD-\d+$")
    customer_email: str
    amount: float = Field(..., gt=0)

class OrderResult(BaseModel):
    order_id: str
    status: str
    total: float

@workflow
async def process_order(
    ctx: WorkflowContext,
    input: OrderInput  # Pydantic model
) -> OrderResult:  # Pydantic model
    # Input is automatically validated
    total = input.amount * 1.1  # Add 10% tax

    return OrderResult(
        order_id=input.order_id,
        status="completed",
        total=total
    )
```

**Benefits:**

- ✅ Automatic input validation
- ✅ Type safety throughout workflow
- ✅ IDE autocomplete and type checking
- ✅ Viewer UI auto-generates input forms

## Next Steps

Now that you understand the core concepts:

- **[Your First Workflow](first-workflow.md)**: Build a complete workflow step-by-step
- **[Saga Pattern](../core-features/saga-compensation.md)**: Deep dive into compensation
- **[Durable Execution](../core-features/durable-execution/replay.md)**: Technical details of replay
- **[Examples](../examples/simple.md)**: Real-world examples
