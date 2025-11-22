# Quick Start

Get started with Edda in 5 minutes! This guide will walk you through creating your first durable workflow.

## Prerequisites

Before starting, make sure you have Edda installed:

```bash
# Install Edda from PyPI
uv add edda-framework
```

If you haven't installed uv yet, see the [Installation Guide](installation.md).

## Step 1: Create a Simple Workflow

Create a new file `my_first_workflow.py`:

```python
import asyncio
from edda import EddaApp, workflow, activity, WorkflowContext

@activity
async def send_welcome_email(ctx: WorkflowContext, email: str):
    """Send welcome email (simulated)"""
    print(f"📧 Sending welcome email to {email}")
    return {"sent": True, "email": email}

@activity
async def create_user_profile(ctx: WorkflowContext, user_id: str, email: str):
    """Create user profile (simulated)"""
    print(f"👤 Creating profile for user {user_id}")
    return {"user_id": user_id, "email": email, "created": True}

@workflow
async def user_onboarding(ctx: WorkflowContext, user_id: str, email: str):
    """Complete user onboarding workflow"""
    # Step 1: Create profile
    profile = await create_user_profile(ctx, user_id, email)

    # Step 2: Send welcome email
    email_result = await send_welcome_email(ctx, email)

    return {
        "status": "completed",
        "user_id": profile["user_id"],
        "email_sent": email_result["sent"]
    }

async def main():
    # Create EddaApp with SQLite database
    app = EddaApp(
        service_name="onboarding-service",
        db_url="sqlite:///onboarding.db",
    )
    await app.initialize()

    try:
        # Start the workflow
        instance_id = await user_onboarding.start(
            user_id="user_123",
            email="newuser@example.com"
        )

        print(f"✅ Workflow started with ID: {instance_id}")

        # Get workflow result
        instance = await app.storage.get_instance(instance_id)
        result = instance['output_data']['result']
        print(f"📊 Result: {result}")

    finally:
        # Cleanup resources
        await app.shutdown()

if __name__ == "__main__":
    asyncio.run(main())
```

## Step 2: Run the Workflow

```bash
uv run python my_first_workflow.py
```

**Output:**

```
👤 Creating profile for user user_123
📧 Sending welcome email to newuser@example.com
✅ Workflow started with ID: <instance_id>
📊 Result: {'status': 'completed', 'user_id': 'user_123', 'email_sent': True}
```

## Step 3: Understanding Crash Recovery

Edda's durable execution ensures workflows survive crashes through **deterministic replay**. When a workflow crashes:

1. ✅ **Activity results are saved** to the database before execution continues
2. ✅ **Workflow state is preserved** (current step, history, locks)
3. ✅ **Automatic recovery** detects and resumes stale workflows

### How Automatic Recovery Works

In production environments with long-running EddaApp instances:

- **Crash detection**: Edda's background task checks for stale locks every 60 seconds
- **Auto-resume**: Crashed workflows are automatically resumed when their lock timeout expires
  - Both normal execution and rollback execution are automatically resumed
  - Default timeout: 5 minutes (300 seconds)
  - Customizable at 3 levels: runtime (`start(lock_timeout_seconds=X)`), decorator (`@workflow(lock_timeout_seconds=Y)`), or global default
  - See [Lock Timeout Customization](../core-features/workflows-activities.md#lock-timeout-customization) for details
  - Workflows resume from their last checkpoint using deterministic replay
- **Deterministic replay**: Previously executed activities return cached results from history
- **Resume from checkpoint**: Only remaining activities execute fresh

### Workflows Waiting for Events or Timers

Workflows in special waiting states are handled differently:

- **Waiting for Events**: Resumed immediately when the awaited event arrives (not on a fixed schedule)
- **Waiting for Timers**: Checked every 10 seconds and resumed when the timer expires
- These workflows are **not** included in the 60-second crash recovery cycle

### Automatic Recovery Mechanisms

| Workflow State | Recovery Check Interval | When Resumed |
|----------------|------------------------|--------------|
| Normal execution or rollback | Every 60 seconds | When lock timeout expires (default: 5 min) |
| Waiting for event | Immediate (event-driven) | When event arrives |
| Waiting for timer | Every 10 seconds | When timer expires |

### Production Behavior

In production (use PostgreSQL or MySQL for distributed systems):

```python
# Long-running ASGI application
from edda import EddaApp

# For distributed systems (K8s, Docker Compose with multiple replicas)
# Use PostgreSQL or MySQL (NOT SQLite)
app = EddaApp(
    service_name="onboarding-service",
    db_url="postgresql://user:password@localhost/edda_workflows",
)

# Background tasks automatically handle:
# - Stale lock cleanup
# - Workflow auto-resume
# - Deterministic replay
```

**Important**: For distributed execution (multiple worker pods/containers), you **must** use PostgreSQL or MySQL. SQLite's single-writer limitation makes it unsuitable for multi-pod deployments.

**When a crash occurs:**

1. Worker process crashes mid-workflow
2. Lock remains in database (marks workflow as "in-progress")
3. After 5 minutes, another worker detects the stale lock
4. Workflow automatically resumes from last checkpoint
5. Previously executed activities skip (cached from history)
6. Remaining activities execute fresh

This is **deterministic replay** - Edda's core feature for durable execution.

## Key Concepts Demonstrated

### Activities

```python
@activity
async def send_welcome_email(ctx: WorkflowContext, email: str):
    # Business logic here
    return {"sent": True}
```

- Activities perform actual work (database writes, API calls, etc.)
- Activity results are **automatically saved in history**
- On replay, activities return cached results

### Workflows

```python
@workflow
async def user_onboarding(ctx: WorkflowContext, user_id: str, email: str):
    # Orchestration logic here
    result1 = await activity1(ctx, ...)
    result2 = await activity2(ctx, ...)
    return result
```

- Workflows orchestrate activities
- Workflows can be replayed after crashes
- Workflows resume from the last checkpoint

### WorkflowContext

```python
async def my_activity(ctx: WorkflowContext, ...):
    step = ctx.current_step  # Get current step number (property)
    # ... use ctx for workflow operations
```

- `ctx` provides workflow operations
- Automatically manages history and replay

## Next Steps

Now that you've created your first workflow, learn more about:

- **[Core Concepts](concepts.md)**: Deep dive into workflows, activities, and durable execution
- **[Your First Workflow](first-workflow.md)**: Build a complete order processing workflow step-by-step
- **[Examples](../examples/simple.md)**: See more real-world examples
- **[Saga Pattern](../core-features/saga-compensation.md)**: Learn about compensation and rollback
