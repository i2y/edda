# Simple Workflow Example

This example demonstrates the basics of creating a workflow with activities in Edda.

## What This Example Shows

- ✅ Defining activities with `@activity`
- ✅ Defining a workflow with `@workflow`
- ✅ Starting a workflow
- ✅ Basic workflow execution

## Code Overview

### Define Activities

```python
from edda import activity, WorkflowContext

@activity
async def greet_user(ctx: WorkflowContext, name: str) -> dict:
    """Simple activity that greets a user."""
    print(f"[Activity] Greeting user: {name}")
    return {"message": f"Hello, {name}!"}

@activity
async def process_data(ctx: WorkflowContext, data: str) -> dict:
    """Simple activity that processes some data."""
    print(f"[Activity] Processing data: {data}")
    processed = data.upper()
    return {"processed": processed, "length": len(processed)}

@activity
async def finalize(ctx: WorkflowContext, result: dict) -> dict:
    """Final activity that finalizes the workflow."""
    print(f"[Activity] Finalizing with result: {result}")
    return {"status": "completed", "final_result": result}
```

### Define Workflow

```python
from edda import workflow

@workflow
async def simple_workflow(ctx: WorkflowContext, name: str, data: str) -> dict:
    """
    Simple workflow that coordinates multiple activities.

    Note: Edda automatically generates activity IDs for sequential execution.
    You don't need to specify activity_id unless using concurrent execution
    (asyncio.gather, async for, etc.).
    """
    print(f"[Workflow] Starting simple_workflow for {name}")

    # Step 1: Greet the user (auto-generated ID: "greet_user:1")
    greeting_result = await greet_user(ctx, name)
    print(f"[Workflow] Step 1 completed: {greeting_result}")

    # Step 2: Process data (auto-generated ID: "process_data:1")
    process_result = await process_data(ctx, data)
    print(f"[Workflow] Step 2 completed: {process_result}")

    # Step 3: Finalize (auto-generated ID: "finalize:1")
    final_result = await finalize(
        ctx,
        {"greeting": greeting_result, "processing": process_result}
    )
    print(f"[Workflow] Step 3 completed: {final_result}")

    print("[Workflow] Workflow completed successfully!")
    return final_result
```

### Run the Workflow

```python
from edda import EddaApp

async def main():
    # Create Edda app
    app = EddaApp(
        db_url="sqlite:///demo.db",
        service_name="example-service",
    )

    await app.initialize()

    try:
        # Start the workflow
        instance_id = await simple_workflow.start(
            name="Alice",
            data="hello world from edda"
        )

        print(f"Workflow started with ID: {instance_id}")

    finally:
        await app.shutdown()
```

## Running the Example

Create a file named `simple_workflow.py` with the complete code (see [Complete Code](#complete-code) section below), then run:

```bash
# Install Edda if you haven't already
uv add edda-framework

# Run your workflow
uv run python simple_workflow.py
```

## Expected Output

```
============================================================
Edda Framework - Simple Workflow Example
============================================================

>>> Starting workflow...

[Workflow] Starting simple_workflow for Alice
[Activity] Greeting user: Alice
[Workflow] Step 1 completed: {'message': 'Hello, Alice!'}
[Activity] Processing data: hello world from edda
[Workflow] Step 2 completed: {'processed': 'HELLO WORLD FROM EDDA', 'length': 21}
[Activity] Finalizing with result: {...}
[Workflow] Step 3 completed: {'status': 'completed', 'final_result': {...}}
[Workflow] Workflow completed successfully!

>>> Workflow started with instance ID: <instance_id>
```

## Complete Code

See a reference implementation in [examples/simple_workflow.py](https://github.com/i2y/edda/blob/main/examples/simple_workflow.py) in the Edda repository.

## Activity ID Auto-Generation

Edda automatically generates activity IDs for sequential execution. You only need to specify `activity_id` manually for concurrent execution.

### Sequential Execution (Auto-Generated IDs - Recommended)

```python
@workflow
async def sequential_workflow(ctx: WorkflowContext, data: str) -> dict:
    # Activities called sequentially - IDs are auto-generated
    result1 = await my_activity(ctx, data)  # Auto: "my_activity:1"
    result2 = await my_activity(ctx, data)  # Auto: "my_activity:2"
    result3 = await another_activity(ctx, result1)  # Auto: "another_activity:1"

    return {"results": [result1, result2, result3]}
```

### Concurrent Execution (Manual IDs - Required)

```python
import asyncio

@workflow
async def concurrent_workflow(ctx: WorkflowContext, items: list) -> dict:
    # Activities called concurrently - you MUST specify activity_id manually
    results = await asyncio.gather(
        my_activity(ctx, items[0], activity_id="my_activity:1"),
        my_activity(ctx, items[1], activity_id="my_activity:2"),
        my_activity(ctx, items[2], activity_id="my_activity:3"),
    )

    return {"results": results}
```

**Why manual IDs for concurrent execution?**

- Concurrent execution order is non-deterministic
- Edda needs explicit IDs to match activities during replay
- Manual IDs ensure deterministic replay even with concurrent execution

## What You Learned

- ✅ **Activities** perform business logic and are recorded in history
- ✅ **Workflows** orchestrate activities
- ✅ **WorkflowContext** (`ctx`) is automatically provided to workflows and activities
- ✅ **EddaApp** manages the workflow engine and database
- ✅ **`.start()`** method begins workflow execution

## Next Steps

- **[E-commerce Example](ecommerce.md)**: Learn about Pydantic integration
- **[Saga Pattern](saga.md)**: Understand compensation and rollback
- **[Event Waiting](events.md)**: Wait for external events in workflows
- **[Core Concepts](../getting-started/concepts.md)**: Deep dive into workflows and activities
