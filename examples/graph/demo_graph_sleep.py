"""Demo script for durable graph with Sleep marker."""

import asyncio
from dataclasses import dataclass

from pydantic_graph import BaseNode, End, Graph

from edda import EddaApp, WorkflowContext, workflow
from edda.integrations.graph import (
    DurableGraph,
    DurableGraphContext,
    Sleep,
)


# =============================================================================
# State Definition
# =============================================================================


@dataclass
class RetryState:
    """State for retry workflow."""

    attempt: int = 0
    max_attempts: int = 3
    success: bool = False


# =============================================================================
# Node Definitions
# =============================================================================


@dataclass
class TryOperationNode(BaseNode[RetryState, None, dict]):
    """Try an operation that might fail."""

    async def run(self, ctx: DurableGraphContext) -> "RetryNode":
        ctx.state.attempt += 1
        print(f"  [TryOperationNode] Attempt {ctx.state.attempt}/{ctx.state.max_attempts}")

        # Simulate failure on first 2 attempts
        if ctx.state.attempt < 3:
            print(f"  [TryOperationNode] Operation failed, will retry after sleep")
            ctx.state.success = False
        else:
            print(f"  [TryOperationNode] Operation succeeded!")
            ctx.state.success = True

        return RetryNode()


@dataclass
class RetryNode(BaseNode[RetryState, None, dict]):
    """Check if retry is needed and sleep before retrying."""

    async def run(self, ctx: DurableGraphContext) -> "TryOperationNode | End[dict]":
        if ctx.state.success:
            print(f"  [RetryNode] Success! Completing workflow.")
            return End({
                "status": "success",
                "attempts": ctx.state.attempt,
            })

        if ctx.state.attempt >= ctx.state.max_attempts:
            print(f"  [RetryNode] Max attempts reached. Failing.")
            return End({
                "status": "failed",
                "attempts": ctx.state.attempt,
            })

        # Sleep before retrying - this is durable!
        print(f"  [RetryNode] Sleeping 1 second before retry...")
        return Sleep(  # type: ignore[return-value]
            seconds=1,
            next_node=TryOperationNode(),
        )


# =============================================================================
# Graph Setup
# =============================================================================

retry_graph = Graph(nodes=[TryOperationNode, RetryNode])
durable_retry_graph = DurableGraph(retry_graph)


# =============================================================================
# Workflow Definition
# =============================================================================


@workflow
async def retry_workflow(ctx: WorkflowContext) -> dict:
    """Retry an operation with sleep between attempts using durable graph."""
    print(f"\n=== Starting retry workflow ===")

    result = await durable_retry_graph.run(
        ctx,
        start_node=TryOperationNode(),
        state=RetryState(),
    )

    print(f"=== Retry workflow completed: {result} ===\n")
    return result


# =============================================================================
# Main
# =============================================================================


async def main():
    """Run the demo."""
    print("=" * 60)
    print("Durable Graph with Sleep Demo")
    print("=" * 60)

    # Create and initialize the app
    # Background tasks (including timer check) start automatically on initialize()
    app = EddaApp(
        service_name="retry-service",
        db_url="sqlite+aiosqlite:///:memory:",
    )
    await app.initialize()

    try:
        # Start the retry workflow
        print("\n[Starting retry workflow...]")

        instance_id = await retry_workflow.start()
        print(f"Workflow instance: {instance_id}")

        # Wait for the workflow to complete
        # Timer check runs every 10 seconds, so we need to wait enough time
        # 3 attempts with 1 second sleep each, checked every 10 seconds = ~30 seconds max
        print("\n[Waiting for workflow to complete (timer check every 10 seconds)...]")
        for i in range(40):  # Check every 1 second for up to 40 seconds
            await asyncio.sleep(1)
            instance = await app.storage.get_instance(instance_id)
            if i % 5 == 0:  # Print status every 5 seconds
                print(f"  Status at {i+1}s: {instance['status']}")
            if instance["status"] == "completed":
                break

        # Check final status
        instance = await app.storage.get_instance(instance_id)
        print(f"\n[Final Result]")
        print(f"Status: {instance['status']}")
        print(f"Result: {instance['output_data']}")

        # Show execution history
        print("\n[Execution History]")
        history = await app.storage.get_history(instance_id)
        for event in history:
            if event["event_type"] == "ActivityCompleted":
                activity_name = event["event_data"].get("activity_name", "unknown")
                print(f"  - {activity_name}")
            elif event["event_type"] == "TimerFired":
                print(f"  - (timer fired)")

    finally:
        await app.shutdown()

    print("\n" + "=" * 60)
    print("Demo completed successfully!")
    print("=" * 60)


if __name__ == "__main__":
    asyncio.run(main())
