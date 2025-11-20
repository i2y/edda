"""
Simple workflow example for Edda framework.

This example demonstrates:
- Defining activities with @activity
- Defining a workflow with @workflow
- Starting a workflow
- Basic workflow execution

Run with:
    uv run python -m examples.simple_workflow
"""

import asyncio

from edda import EddaApp, WorkflowContext, activity, workflow


# Define activities
@activity
async def greet_user(_ctx: WorkflowContext, name: str) -> dict:
    """Simple activity that greets a user."""
    print(f"[Activity] Greeting user: {name}")
    return {"message": f"Hello, {name}!"}


@activity
async def process_data(_ctx: WorkflowContext, data: str) -> dict:
    """Simple activity that processes some data."""
    print(f"[Activity] Processing data: {data}")
    processed = data.upper()
    return {"processed": processed, "length": len(processed)}


@activity
async def finalize(_ctx: WorkflowContext, result: dict) -> dict:
    """Final activity that finalizes the workflow."""
    print(f"[Activity] Finalizing with result: {result}")
    return {"status": "completed", "final_result": result}


# Define a workflow
@workflow
async def simple_workflow(ctx: WorkflowContext, name: str, data: str) -> dict:
    """
    Simple workflow that coordinates multiple activities.

    Note: Edda automatically generates activity IDs for sequential execution.
    You don't need to specify activity_id unless using concurrent execution.

    Args:
        ctx: Workflow context (automatically provided)
        name: User name
        data: Data to process

    Returns:
        Final workflow result
    """
    print(f"\n[Workflow] Starting simple_workflow for {name}")

    # Step 1: Greet the user (Activity ID auto-generated: "greet_user:1")
    greeting_result = await greet_user(ctx, name)
    print(f"[Workflow] Step 1 completed: {greeting_result}")

    # Step 2: Process data (Activity ID auto-generated: "process_data:1")
    process_result = await process_data(ctx, data)
    print(f"[Workflow] Step 2 completed: {process_result}")

    # Step 3: Finalize (Activity ID auto-generated: "finalize:1")
    final_result = await finalize(ctx, {"greeting": greeting_result, "processing": process_result})
    print(f"[Workflow] Step 3 completed: {final_result}")

    print("[Workflow] Workflow completed successfully!")
    return final_result


async def main():
    """Main function to demonstrate the workflow."""
    print("=" * 60)
    print("Edda Framework - Simple Workflow Example")
    print("=" * 60)

    # Create Edda app
    app = EddaApp(
        service_name="example-service",
        db_url="sqlite:///demo.db",
    )

    # Initialize the app
    await app.initialize()

    try:
        # Start the workflow
        print("\n>>> Starting workflow...")
        instance_id = await simple_workflow.start(name="Alice", data="hello world from edda")

        print(f"\n>>> Workflow started with instance ID: {instance_id}")
        print(">>> Check the database to see the workflow history!")

        # In a real application, the workflow would be resumed by events
        # For this simple example, we just show how to start it

    finally:
        # Cleanup
        await app.shutdown()
        print("\n" + "=" * 60)
        print("Example completed!")
        print("=" * 60)


if __name__ == "__main__":
    asyncio.run(main())
