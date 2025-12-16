# Deterministic Replay

Edda uses a **deterministic replay mechanism** to ensure workflows never lose progress. This document explains how workflows are resumed after interruption and how state is restored.

## Overview

Edda's replay mechanism has three key characteristics:

1. **Completed activities are skipped**: Already-executed activities return cached results from history
2. **Workflow code runs fully**: Control flow and calculations between activities execute every time
3. **State restoration from history**: Workflow state is restored from the persisted execution history

## How Replay Works

### Activity Execution Flow

When an activity is called during replay:

1. **Activity ID Resolution**: Auto-generated (`function_name:counter`) or explicitly provided
2. **Cache Check**: If replaying, check if result is cached for this `activity_id`
3. **Return Cached**: If found, return cached result **without executing the function**
4. **Execute**: If not cached, run the function and record the result
5. **Error Handling**: Failed activities are recorded with full error details

### Activity ID Patterns

Activities are identified by unique IDs in the format `function_name:counter`.

**Sequential Execution (Auto-generated IDs):**

```python
# First call: auto-generates "reserve_inventory:1"
inventory = await reserve_inventory(ctx, order_id)

# Second call: auto-generates "reserve_inventory:2"
backup_inventory = await reserve_inventory(ctx, backup_order_id)
```

**Conditional Execution (Auto-generated IDs):**

```python
# Execution order is deterministic, so auto-generated IDs work fine
if requires_approval:
    result = await approve_order(ctx, order_id)  # Auto: "approve_order:1"
else:
    result = await auto_approve(ctx, order_id)    # Auto: "auto_approve:1"
```

**Loop Execution (Auto-generated IDs):**

```python
# Execution order is deterministic (same order every replay)
for item in items:
    await process_item(ctx, item)  # Auto: "process_item:1", "process_item:2", ...
```

**Concurrent Execution (Manual IDs Required):**

```python
# Execution order is non-deterministic, so manual IDs are required
import asyncio

results = await asyncio.gather(
    process_a(ctx, data, activity_id="process_a:1"),
    process_b(ctx, data, activity_id="process_b:1"),
    process_c(ctx, data, activity_id="process_c:1"),
)
```

### How Replay Works Internally

When an activity is called:

1. **Resolve Activity ID**: Auto-generate or use explicit `activity_id` parameter
2. **Check Replay Mode**: If `ctx.is_replaying` is True, check cache
3. **Cache Lookup**: Look for cached result using `activity_id` as key
4. **Return or Execute**: Return cached result if found, otherwise execute function
5. **Record Result**: Save result to database with `activity_id` for future replay

### Example

```python
from edda import workflow, activity, WorkflowContext

@activity
async def reserve_inventory(ctx: WorkflowContext, order_id: str):
    # Business logic here
    return {"reservation_id": "R123", "status": "reserved"}

@activity
async def process_payment(ctx: WorkflowContext, order_id: str):
    # Business logic here
    return {"transaction_id": "T456", "status": "completed"}

@activity
async def arrange_shipping(ctx: WorkflowContext, order_id: str):
    # Business logic here
    return {"tracking_number": "TRACK789"}

@workflow
async def order_workflow(ctx: WorkflowContext, order_id: str):
    # Activity IDs are auto-generated for sequential calls
    inventory = await reserve_inventory(ctx, order_id)
    payment = await process_payment(ctx, order_id)
    shipping = await arrange_shipping(ctx, order_id)

    return {"status": "completed"}
```

**If workflow crashed after processing payment, during replay:**

- `reserve_inventory`: **Skipped** (cached result `{"reservation_id": "R123", ...}` returned)
- `process_payment`: **Skipped** (cached result `{"transaction_id": "T456", ...}` returned)
- `arrange_shipping`: **Executed** (no cache available, runs normally)

## What Gets Replayed

### ✅ Always Executed (Every Replay)

- Variable calculations and assignments
- Control flow (`if`, `for`, `while`, `match-case` statements)
- Function calls (non-activity)
- Local variable operations
- Workflow function code from start to finish

### ❌ Never Re-executed (Cached)

- Completed activity business logic

### Example

```python
@workflow
async def complex_workflow(ctx: WorkflowContext, amount: int):
    # This code executes every time (including replay)
    tax = amount * 0.1
    total = amount + tax
    print(f"Total calculated: {total}")  # Prints on every replay!

    # Activity is skipped during replay (cached)
    result1 = await check_balance(ctx, total)

    # This if statement is evaluated every time
    if result1["sufficient"]:
        # This activity is also skipped during replay
        result2 = await process_transaction(ctx, total)

        # This calculation executes every time
        final_amount = result2["amount"] - result2["fee"]

        # This activity is also skipped during replay
        await send_receipt(ctx, final_amount)
    else:
        # This branch is also evaluated every time
        await send_rejection(ctx, "Insufficient balance")

    return {"status": "completed"}
```

**During replay:**

1. `tax`, `total` calculations execute every time
2. `print()` executes every time (may appear multiple times in logs)
3. `check_balance()` skipped, `result1` from cache
4. `if result1["sufficient"]` evaluated every time
5. `process_transaction()` skipped, `result2` from cache
6. `final_amount` calculation executes every time
7. `send_receipt()` skipped

## History and Caching

### Data Flow

```
First execution:
    Activity executes → Result saved to DB → Available for replay

Replay:
    Load history from DB → Populate cache → Return cached results
```

### What Gets Stored

Edda persists all activity results to the `workflow_history` table:

| instance_id | activity_id | event_type | event_data |
|-------------|-------------|------------|------------|
| order-abc123 | reserve_inventory:1 | ActivityCompleted | `{"activity_name": "reserve_inventory", "result": {"reservation_id": "R123"}, "input": {...}}` |
| order-abc123 | process_payment:1 | ActivityCompleted | `{"activity_name": "process_payment", "result": {"transaction_id": "T456"}, "input": {...}}` |
| order-abc123 | wait_event_payment.completed:1 | EventReceived | `{"event_data": {...}}` |

**Event Types:**

- **ActivityCompleted**: Successful activity execution
- **ActivityFailed**: Activity raised an exception (includes error type and message)
- **EventReceived**: Event received via `wait_event()`
- **TimerExpired**: Timer expired via `sleep()`

### How Cache Works

On replay, Edda:

1. **Loads all history** from the database for this workflow instance
2. **Populates an in-memory cache** keyed by `activity_id`
3. **Returns cached results** without re-executing activities

**Example cache after loading history:**

```python
{
    "reserve_inventory:1": {"reservation_id": "R123", "status": "reserved"},
    "process_payment:1": {"transaction_id": "T456", "status": "completed"},
}
```

This ensures workflows resume exactly where they left off, even after crashes.

### ReceivedEvent Reconstruction

Events received via `wait_event()` are automatically reconstructed from stored data, preserving CloudEvents metadata (type, source, time, etc.).

## Determinism Guarantees

### ✅ Best Practices

**1. Hide non-deterministic operations in activities:**

```python
@activity
async def get_current_time(ctx: WorkflowContext) -> str:
    return datetime.now().isoformat()

@workflow
async def workflow(ctx: WorkflowContext):
    # Replay will use the same timestamp
    timestamp = await get_current_time(ctx)
```

**2. Random values should be activities:**

```python
@activity
async def generate_id(ctx: WorkflowContext) -> str:
    return str(uuid.uuid4())
```

**3. External API calls should be activities (recommended):**

```python
@activity
async def call_external_api(ctx: WorkflowContext) -> dict:
    async with httpx.AsyncClient() as client:
        response = await client.get("https://api.example.com/data")
        return response.json()

@workflow
async def workflow(ctx: WorkflowContext):
    # Benefits of making it an activity:
    # - Not re-executed on replay (definitely from cache)
    # - Easy to test (can be mocked)
    # - Recorded in history
    # - Better performance (network cost reduced)
    data = await call_external_api(ctx)
```

### ❌ Anti-Patterns

```python
@workflow
async def bad_workflow(ctx: WorkflowContext):
    # ❌ Direct time access in workflow (different on replay)
    timestamp = datetime.now()
    # First run: 2025-01-01 10:00:00
    # Replay: 2025-01-01 10:05:00 ← Different!

    # ❌ Random value generation in workflow (different on replay)
    request_id = str(uuid.uuid4())
    # First run: "abc-123"
    # Replay: "def-456" ← Different!

    # ❌ File write in workflow (duplicated on replay)
    with open("log.txt", "a") as f:
        f.write(f"Processing at {timestamp}\n")
    # Logs appended on every replay

    result = await some_activity(ctx, timestamp, request_id)
```

**Rule of thumb:** When in doubt, make it an activity. There's minimal downside and significant benefits.

## When Replay Happens

### 1. Event Waiting Resume (`wait_event()`)

The most common case is when a workflow resumes after waiting for an external event.

```python
@workflow
async def payment_workflow(ctx: WorkflowContext, order_id: str):
    # Step 1: Start payment
    payment = await start_payment(ctx, order_id)

    # Step 2: Wait for payment completion event
    # Workflow pauses here (status="waiting_for_event")
    event = await wait_event(
        ctx,
        event_type="payment.completed",
        timeout_seconds=300
    )

    # After event received, resume from here (replay happens)
    # Step 3: Complete order
    result = await complete_order(ctx, order_id, event)
    return result
```

**Replay behavior:**

1. `resume_workflow()` creates context with `ctx.is_replaying=True`
2. `load_history()` loads execution history
3. Workflow function runs from start
4. `start_payment()` - **Skipped** (cached result)
5. `wait_event()` - **Skipped** (cached event data)
6. `complete_order()` - **Executed** (new activity)

### 2. Explicit Resume Call

Developers can manually resume workflows:

```python
# Admin API endpoint
@app.post("/admin/workflows/{instance_id}/resume")
async def resume_workflow_endpoint(instance_id: str):
    """Manually resume a workflow"""
    instance = await app.storage.get_instance(instance_id)
    workflow_name = instance["workflow_name"]

    # Get corresponding Saga instance
    from edda.workflow import get_all_workflows
    saga_instance = get_all_workflows()[workflow_name]

    # Start replay
    await saga_instance.resume(instance_id)

    return {"status": "resumed", "instance_id": instance_id}
```

### 3. Crash Recovery (Automatic)

Edda automatically recovers from crashes in two stages:

#### 3-1. Stale Lock Cleanup

When a worker process crashes, its locks become "stale." Edda automatically cleans these up:

```python
async def cleanup_stale_locks_periodically(
    storage: StorageProtocol,
    interval: int = 60,  # Check every 60 seconds
) -> None:
    """Background task to clean up stale locks"""
    while True:
        await asyncio.sleep(interval)

        # Clean up stale locks (uses lock_expires_at column)
        workflows_to_resume = await storage.cleanup_stale_locks()

        if len(workflows_to_resume) > 0:
            print(f"Cleaned up {len(workflows_to_resume)} stale locks")
```

This background task starts automatically when `EddaApp` launches.

**How it works:**

1. **Every 60 seconds**, check for stale locks
2. **Expired locks** are detected (based on `lock_expires_at` column set at lock acquisition)
3. Release those locks (`locked_by=NULL`)
4. Return list of workflows that need to be resumed

**Return value structure:**

```python
[
    {
        "instance_id": str,
        "workflow_name": str,
        "source_hash": str,  # Hash of workflow definition
        "status": str        # "running" or "compensating"
    },
    ...
]
```

The `status` field indicates whether the workflow was running normally (`"running"`) or executing compensations (`"compensating"`) when it crashed.

#### 3-2. Automatic Workflow Resume

After cleaning stale locks, Edda automatically resumes workflows with `status="running"` or `status="compensating"`:

```python
async def auto_resume_stale_workflows_periodically(
    storage: StorageProtocol,
    replay_engine: Any,
    interval: int = 60,
) -> None:
    """Stale lock cleanup + automatic resume"""
    while True:
        await asyncio.sleep(interval)

        # Clean up stale locks and get workflows to resume
        workflows_to_resume = await storage.cleanup_stale_locks()

        if len(workflows_to_resume) > 0:
            # Auto-resume workflows
            for workflow in workflows_to_resume:
                instance_id = workflow["instance_id"]
                workflow_name = workflow["workflow_name"]
                try:
                    print(f"Auto-resuming: {workflow_name} ({instance_id})")
                    await replay_engine.resume_by_name(instance_id, workflow_name)
                except Exception as e:
                    print(f"Failed to resume {instance_id}: {e}")
```

**Special handling for different workflow states:**

1. **Running workflows** (`status="running"`):
   - Resume normally via `replay_engine.resume_by_name()`
   - Full workflow function execution with replay

2. **Compensating workflows** (`status="compensating"`):
   - Resume via `replay_engine.resume_compensating_workflow()`
   - Only re-execute incomplete compensations (not the workflow function)
   - Ensures compensation transactions complete even after crashes

**Source hash verification (Safety mechanism):**

Before auto-resuming, Edda verifies that the workflow definition hasn't changed:

```python
# Check if workflow definition matches current registry
current_hash = saga_instance.source_hash
stored_hash = workflow["source_hash"]

if current_hash != stored_hash:
    # Workflow code has changed - skip auto-resume
    logger.warning(f"Source hash mismatch for {workflow_name}")
    continue
```

This prevents incompatible code from executing and ensures crash recovery is safe.

**Why this works:**

When a worker crashes, workflows with `status="running"` **always** hold a stale lock:

| Workflow Status | Lock Held | On Crash | Auto-Resume Strategy |
|----------------|-----------|----------|---------------------|
| `status="running"` | YES (inside `workflow_lock`) | Becomes stale | ✅ Normal resume |
| `status="compensating"` | YES (inside compensation execution) | Becomes stale | ✅ Compensation resume |
| `status="waiting_for_event"` | NO (after lock released) | No stale lock | ❌ Event-driven resume |
| `status="waiting_for_timer"` | NO (after lock released) | No stale lock | ❌ Timer-driven resume |
| `status="completed"` | NO | No stale lock | N/A |
| `status="failed"` | NO | No stale lock | N/A |
| `status="cancelled"` | NO | No stale lock | N/A |

Therefore, cleaning stale locks and resuming `status="running"` and `status="compensating"` workflows ensures **no resume leakage**.

### 4. Deployment & Scale-Out

Edda supports distributed execution, so workflows continue during deployment:

**Scenario:**

1. Worker A executing a workflow
2. Worker B newly deployed
3. Worker A shutdown
4. Waiting workflows are taken over by Worker B (resume via replay)

**Database-based exclusive control guarantee:**

Edda's database-based exclusive control prevents multiple workers from executing the same workflow instance simultaneously:

```python
async with workflow_lock(storage, instance_id, worker_id):
    # Only execute while lock held
    ctx = WorkflowContext(instance_id=instance_id, is_replaying=True, ...)
    await ctx.load_history()
    result = await workflow_func(ctx, **input_data)
```

## Complete Replay Flow

### Initial Execution (Completed Steps 1-2, Crashed at Step 3)

```python
@workflow
async def order_workflow(ctx: WorkflowContext, order_id: str):
    # Activity 1 (auto-generated ID: "reserve_inventory:1")
    inventory = await reserve_inventory(ctx, order_id)
    # → DB saved: activity_id="reserve_inventory:1", result={"reservation_id": "R123"}

    # Activity 2 (auto-generated ID: "process_payment:1")
    payment = await process_payment(ctx, order_id)
    # → DB saved: activity_id="process_payment:1", result={"transaction_id": "T456"}

    # Activity 3: Exception occurs (e.g., network error)
    shipping = await arrange_shipping(ctx, order_id)
    # → Exception thrown, workflow interrupted
```

**DB State:**

- `workflow_instances.status = "running"`
- `workflow_instances.current_activity_id = "process_payment:1"`
- `workflow_history` has 2 records

### Replay Execution (Resume)

```python
# 1. ReplayEngine.resume_workflow() called
await replay_engine.resume_workflow(instance_id, order_workflow)

# 2. Create WorkflowContext (is_replaying=True)
ctx = WorkflowContext(
    instance_id=instance_id,
    is_replaying=True,  # Replay mode
    ...
)

# 3. Load history
await ctx.load_history()
# → _history_cache = {"reserve_inventory:1": {...}, "process_payment:1": {...}}

# 4. Execute workflow function from start
result = await order_workflow(ctx, order_id="123")

# 5. Activity: reserve_inventory:1
#    - ctx.is_replaying == True
#    - Cache has activity_id="reserve_inventory:1"
#    - Don't execute function, return {"reservation_id": "R123"} from cache

# 6. Activity: process_payment:1
#    - ctx.is_replaying == True
#    - Cache has activity_id="process_payment:1"
#    - Don't execute function, return {"transaction_id": "T456"} from cache

# 7. Activity: arrange_shipping:1
#    - ctx.is_replaying == True
#    - No cache for activity_id="arrange_shipping:1"
#    - Execute function (new processing)
#    - Save result to DB on success

# 8. Workflow complete
await ctx.update_status("completed", {"result": result})
```

## Safety Mechanisms

Edda includes several safety mechanisms to ensure reliable execution:

### Source Hash Verification

Before auto-resuming crashed workflows, Edda verifies workflow definition hasn't changed:

- Each workflow has a source code hash (`source_hash`)
- Stored in database when workflow starts
- Compared with current registry during auto-resume
- Incompatible code is skipped (prevents unsafe execution)

This prevents:

- Resuming workflows with outdated logic
- Schema mismatches after deployment
- Data corruption from incompatible code changes

### Exclusive Control Guarantees

Edda's database-based exclusive control prevents concurrent execution:

```python
async with workflow_lock(storage, instance_id, worker_id, timeout_seconds=300):
    # Only one worker can hold this lock
    # Other workers wait or skip
```

Features:

- **5-minute timeout** by default (prevents indefinite locks)
- **Worker ID tracking** (know which worker holds the lock)
- **Stale lock cleanup** (automatic recovery after crashes)

### Transactional History Recording

All history recording is transactional:

- Activity completion + history save in single transaction
- Rollback on failure (ensures consistency)
- No orphaned history records
- Deterministic replay guaranteed

### Compensating Workflow Recovery

Special handling for workflows that crash during compensation:

- `status="compensating"` detected during cleanup
- Only incomplete compensations are re-executed
- Workflow function is NOT re-executed
- Ensures compensation transactions complete even after multiple crashes

## Summary

Edda's replay mechanism characteristics:

| Item | Behavior |
|------|----------|
| **Completed activities** | Skipped (result from cache) |
| **Workflow function code** | Runs from start every time |
| **Control flow (if/for/while/match-case)** | Evaluated every time |
| **Variable calculations** | Executed every time |
| **State restoration** | Load history from DB → Populate memory cache |
| **Determinism guarantee** | Non-deterministic operations hidden in activities |

This mechanism ensures workflows can resume accurately after process crashes, deployments, or scale-outs.

## Next Steps

- **[Crash Recovery](crash-recovery.md)**: Learn about automatic recovery from failures
- **[Saga Pattern](../saga-compensation.md)**: Automatic compensation on workflow failure
- **[Event Handling](../events/wait-event.md)**: Wait for external events in workflows
