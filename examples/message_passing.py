"""
Message Passing Example for Edda.

This example demonstrates:
1. Direct messaging between workflows (wait_message / send_message_to)
2. Group messaging for fan-out patterns (join_group / publish_to_group)

Run with:
    uv run uvicorn examples.message_passing:app --reload --port 8002

Test the workflows:
    # Start the receiver workflow (will wait for a message)
    curl -X POST http://localhost:8002/ \
        -H "Content-Type: application/cloudevents+json" \
        -d '{
            "specversion": "1.0",
            "type": "approval_workflow",
            "source": "examples",
            "id": "evt-001",
            "data": {"request_id": "req-001"}
        }'

    # Start the sender workflow (will send a message to the receiver)
    curl -X POST http://localhost:8002/ \
        -H "Content-Type: application/cloudevents+json" \
        -d '{
            "specversion": "1.0",
            "type": "approver_workflow",
            "source": "examples",
            "id": "evt-002",
            "data": {"target_id": "approval_workflow:req-001", "decision": true}
        }'

    # Start a notification service (joins a group and waits for broadcasts)
    curl -X POST http://localhost:8002/ \
        -H "Content-Type: application/cloudevents+json" \
        -d '{
            "specversion": "1.0",
            "type": "notification_service",
            "source": "examples",
            "id": "evt-003",
            "data": {"service_id": "svc-001"}
        }'

    # Broadcast a message to all notification services
    curl -X POST http://localhost:8002/ \
        -H "Content-Type: application/cloudevents+json" \
        -d '{
            "specversion": "1.0",
            "type": "order_processor",
            "source": "examples",
            "id": "evt-004",
            "data": {"order_id": "order-001"}
        }'
"""

from pydantic import BaseModel, Field

from edda import EddaApp, activity, publish, receive, send_to, subscribe, workflow
from edda.context import WorkflowContext

# =============================================================================
# Pydantic Models
# =============================================================================


class ApprovalRequest(BaseModel):
    """Input for approval workflow."""

    request_id: str = Field(..., description="Unique request ID")


class ApprovalDecision(BaseModel):
    """Input for approver workflow."""

    target_id: str = Field(..., description="Target workflow instance ID")
    decision: bool = Field(..., description="Approval decision")


class NotificationServiceInput(BaseModel):
    """Input for notification service."""

    service_id: str = Field(..., description="Service instance ID")


class OrderInput(BaseModel):
    """Input for order processor."""

    order_id: str = Field(..., description="Order ID")


class ApprovalResult(BaseModel):
    """Result of approval workflow."""

    request_id: str
    approved: bool
    approver: str | None = None


class ProcessResult(BaseModel):
    """Generic process result."""

    status: str
    message: str


# =============================================================================
# Activities
# =============================================================================


@activity
async def log_activity(_ctx: WorkflowContext, message: str) -> dict:
    """Log a message (simulating some work)."""
    print(f"[LOG] {message}")
    return {"logged": True, "message": message}


@activity
async def send_notification(_ctx: WorkflowContext, notification: dict) -> dict:
    """Send a notification (simulated)."""
    print(f"[NOTIFICATION] {notification}")
    return {"sent": True, "notification": notification}


# =============================================================================
# Direct Messaging Example
# =============================================================================


@workflow(event_handler=True)
async def approval_workflow(
    ctx: WorkflowContext, input: ApprovalRequest
) -> ApprovalResult:
    """
    Workflow that waits for an approval message.

    This demonstrates the receiver side of direct messaging.
    The workflow will pause until a message arrives on the "approval" channel.
    """
    # Log that we're waiting
    await log_activity(ctx, f"Request {input.request_id} waiting for approval")

    # Wait for approval message on the "approval" channel
    # This will pause the workflow until a message arrives
    msg = await receive(ctx, channel="approval", timeout_seconds=300)

    # Process the approval decision
    approved = msg.data.get("approved", False)
    approver = msg.metadata.get("source_instance_id", "unknown")

    await log_activity(
        ctx, f"Request {input.request_id} {'approved' if approved else 'rejected'}"
    )

    return ApprovalResult(
        request_id=input.request_id,
        approved=approved,
        approver=approver,
    )


@workflow(event_handler=True)
async def approver_workflow(
    ctx: WorkflowContext, input: ApprovalDecision
) -> ProcessResult:
    """
    Workflow that sends an approval message to another workflow.

    This demonstrates the sender side of direct messaging.
    """
    # Send approval message to the target workflow
    delivered = await send_to(
        ctx,
        instance_id=input.target_id,
        data={"approved": input.decision, "reason": "Manager approved"},
        channel="approval",
    )

    if delivered:
        return ProcessResult(
            status="success",
            message=f"Approval decision sent to {input.target_id}",
        )
    else:
        return ProcessResult(
            status="not_delivered",
            message=f"No workflow waiting at {input.target_id}",
        )


# =============================================================================
# Group Messaging Example
# =============================================================================


@workflow(event_handler=True)
async def notification_service(
    ctx: WorkflowContext, input: NotificationServiceInput
) -> ProcessResult:
    """
    Notification service that joins a group and waits for broadcasts.

    This demonstrates group membership and receiving broadcast messages.
    Multiple instances of this workflow can join the same group.
    """
    # Subscribe to order.created channel (all services receive all messages)
    await subscribe(ctx, "order.created", mode="broadcast")

    await log_activity(ctx, f"Service {input.service_id} joined notification group")

    # Wait for a notification message
    # All instances subscribed to this channel receive the same broadcast
    msg = await receive(ctx, channel="order.created", timeout_seconds=600)

    # Send the notification
    await send_notification(
        ctx, {"service": input.service_id, "order": msg.data.get("order_id")}
    )

    return ProcessResult(
        status="notified",
        message=f"Service {input.service_id} processed order notification",
    )


@workflow(event_handler=True)
async def order_processor(ctx: WorkflowContext, input: OrderInput) -> ProcessResult:
    """
    Order processor that broadcasts to all notification services.

    This demonstrates publishing to a group.
    The message will be delivered to all workflows in the "order_notifications" group.
    """
    await log_activity(ctx, f"Processing order {input.order_id}")

    # Broadcast to all notification services subscribed to order.created
    message_id = await publish(
        ctx,
        channel="order.created",
        data={"order_id": input.order_id, "status": "created"},
    )

    await log_activity(ctx, f"Published order notification {message_id}")

    return ProcessResult(
        status="completed",
        message=f"Order {input.order_id} processed, notification published",
    )


# =============================================================================
# Application Setup
# =============================================================================

app = EddaApp(
    db_url="sqlite:///message_passing_example.db",
)

if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="127.0.0.1", port=8002)
