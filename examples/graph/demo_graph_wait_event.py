"""Demo script for durable graph with WaitForEvent marker."""

import asyncio
from dataclasses import dataclass

from pydantic_graph import BaseNode, End, Graph

from edda import EddaApp, WorkflowContext, workflow
from edda.channels import publish
from edda.integrations.graph import (
    DurableGraph,
    DurableGraphContext,
    WaitForEvent,
)


# =============================================================================
# State Definition
# =============================================================================


@dataclass
class ApprovalState:
    """State for approval workflow."""

    request_id: str | None = None
    requester: str | None = None
    approved: bool = False
    approver: str | None = None


# =============================================================================
# Node Definitions
# =============================================================================


@dataclass
class SubmitRequestNode(BaseNode[ApprovalState, None, dict]):
    """Submit a request for approval."""

    request_id: str
    requester: str

    async def run(self, ctx: DurableGraphContext) -> "ProcessApprovalNode":
        print(f"  [SubmitRequestNode] Submitting request {self.request_id} from {self.requester}")
        ctx.state.request_id = self.request_id
        ctx.state.requester = self.requester

        # Return WaitForEvent marker - DurableGraph will handle the actual wait
        # at the workflow level (outside the activity)
        # Note: Return type annotation is ProcessApprovalNode for pydantic-graph,
        # but we actually return WaitForEvent which DurableGraph handles specially
        return WaitForEvent(  # type: ignore[return-value]
            event_type=f"approval.{ctx.state.request_id}",
            next_node=ProcessApprovalNode(),
            timeout_seconds=30,
        )


@dataclass
class ProcessApprovalNode(BaseNode[ApprovalState, None, dict]):
    """Process the approval result."""

    async def run(self, ctx: DurableGraphContext) -> End[dict]:
        # Access the event that was received via WaitForEvent
        event = ctx.last_event
        print(f"  [ProcessApprovalNode] Received event: {event}")

        if event:
            ctx.state.approved = event.data.get("approved", False)
            ctx.state.approver = event.data.get("approver", "unknown")

        if ctx.state.approved:
            print(f"  [ProcessApprovalNode] Request APPROVED by {ctx.state.approver}")
            return End({
                "request_id": ctx.state.request_id,
                "status": "approved",
                "approver": ctx.state.approver,
            })
        else:
            print(f"  [ProcessApprovalNode] Request REJECTED by {ctx.state.approver}")
            return End({
                "request_id": ctx.state.request_id,
                "status": "rejected",
                "approver": ctx.state.approver,
            })


# =============================================================================
# Graph Setup
# =============================================================================

approval_graph = Graph(nodes=[SubmitRequestNode, ProcessApprovalNode])
durable_approval_graph = DurableGraph(approval_graph)


# =============================================================================
# Workflow Definition
# =============================================================================


@workflow
async def approval_workflow(
    ctx: WorkflowContext,
    request_id: str,
    requester: str,
) -> dict:
    """Process an approval request using durable graph with WaitForEvent."""
    print(f"\n=== Starting approval workflow for {request_id} ===")

    result = await durable_approval_graph.run(
        ctx,
        start_node=SubmitRequestNode(request_id=request_id, requester=requester),
        state=ApprovalState(),
    )

    print(f"=== Approval workflow completed: {result} ===\n")
    return result


# =============================================================================
# Main
# =============================================================================


async def main():
    """Run the demo."""
    print("=" * 60)
    print("Durable Graph with WaitForEvent Demo")
    print("=" * 60)

    # Create and initialize the app
    app = EddaApp(
        service_name="approval-service",
        db_url="sqlite+aiosqlite:///:memory:",
    )
    await app.initialize()

    try:
        # Start the approval workflow
        print("\n[Step 1] Starting approval workflow...")
        request_id = "REQ-001"

        instance_id = await approval_workflow.start(
            request_id=request_id,
            requester="alice@example.com",
        )
        print(f"Workflow instance: {instance_id}")

        # Wait a bit for the workflow to reach WaitForEvent
        await asyncio.sleep(0.5)

        # Check status - should be waiting
        instance = await app.storage.get_instance(instance_id)
        print(f"Current status: {instance['status']}")

        # Now simulate an external approval event
        print("\n[Step 2] Sending approval event...")
        await publish(
            app.storage,
            f"approval.{request_id}",
            {"approved": True, "approver": "bob@example.com"},
            worker_id="external-approver",
        )

        # Wait for the workflow to complete automatically
        # The background channel delivery task will resume the workflow
        print("\n[Waiting for workflow to complete (automatic resume via background task)...]")
        for i in range(20):  # Check every 0.5 second for up to 10 seconds
            await asyncio.sleep(0.5)
            instance = await app.storage.get_instance(instance_id)
            if i % 4 == 0:  # Print status every 2 seconds
                print(f"  Status at {(i+1)*0.5}s: {instance['status']}")
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
            elif event["event_type"] == "ChannelMessageReceived":
                print(f"  - (received approval event)")

    finally:
        await app.shutdown()

    print("\n" + "=" * 60)
    print("Demo completed successfully!")
    print("=" * 60)


if __name__ == "__main__":
    asyncio.run(main())
