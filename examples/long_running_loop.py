"""
Long-Running Loop Example for Edda.

This example demonstrates:
1. Using recur() for Erlang-style tail recursion
2. Processing items in batches to prevent history growth
3. Maintaining state across recur() calls
4. Re-joining groups after recur()

The recur() pattern is essential for workflows that:
- Run indefinitely (event listeners, background processors)
- Process many items (batch processing, queue consumers)
- Need to prevent history table from growing unbounded

Run with:
    uv run uvicorn examples.long_running_loop:app --reload --port 8003

Start the batch processor:
    curl -X POST http://localhost:8003/ \
        -H "Content-Type: application/cloudevents+json" \
        -d '{
            "specversion": "1.0",
            "type": "batch_processor",
            "source": "examples",
            "id": "evt-001",
            "data": {"total_items": 50, "batch_size": 10}
        }'

Start the event listener (runs indefinitely):
    curl -X POST http://localhost:8003/ \
        -H "Content-Type: application/cloudevents+json" \
        -d '{
            "specversion": "1.0",
            "type": "event_listener",
            "source": "examples",
            "id": "evt-002",
            "data": {"listener_id": "listener-001"}
        }'

Send an event to the listener:
    curl -X POST http://localhost:8003/ \
        -H "Content-Type: application/cloudevents+json" \
        -d '{
            "specversion": "1.0",
            "type": "event_publisher",
            "source": "examples",
            "id": "evt-003",
            "data": {"message": "Hello from publisher!"}
        }'
"""

from pydantic import BaseModel, Field

from edda import EddaApp, activity, publish, receive, subscribe, workflow
from edda.context import WorkflowContext
from edda.workflow import recur

# =============================================================================
# Pydantic Models
# =============================================================================


class BatchProcessorInput(BaseModel):
    """Input for batch processor."""

    total_items: int = Field(..., description="Total number of items to process")
    batch_size: int = Field(default=10, description="Items per batch")
    processed_count: int = Field(default=0, description="Already processed count")


class EventListenerInput(BaseModel):
    """Input for event listener."""

    listener_id: str = Field(..., description="Listener instance ID")
    events_processed: int = Field(default=0, description="Number of events processed")
    max_events_before_recur: int = Field(
        default=100, description="Recur after this many events"
    )


class EventPublisherInput(BaseModel):
    """Input for event publisher."""

    message: str = Field(..., description="Message to publish")


class ProcessResult(BaseModel):
    """Result of processing."""

    status: str
    message: str
    processed_count: int = 0


# =============================================================================
# Activities
# =============================================================================


@activity
async def process_batch(
    _ctx: WorkflowContext, start: int, end: int, total: int
) -> dict:
    """Process a batch of items."""
    print(f"[BATCH] Processing items {start} to {end} of {total}")
    # Simulate batch processing
    return {
        "processed": list(range(start, min(end, total))),
        "count": min(end, total) - start,
    }


@activity
async def handle_event(_ctx: WorkflowContext, event_data: dict) -> dict:
    """Handle an incoming event."""
    print(f"[EVENT] Handling event: {event_data}")
    # Simulate event handling
    return {"handled": True, "event": event_data}


# =============================================================================
# Batch Processor Example
# =============================================================================


@workflow(event_handler=True)
async def batch_processor(
    ctx: WorkflowContext, input: BatchProcessorInput
) -> ProcessResult:
    """
    Batch processor that uses recur() to prevent history growth.

    This workflow processes items in batches. After each batch, it calls
    recur() to:
    1. Archive the current history
    2. Clean up subscriptions
    3. Start a fresh instance with updated state

    Without recur(), the history table would grow indefinitely as each
    process_batch() call adds a new history entry.
    """
    # Check if we're done
    if input.processed_count >= input.total_items:
        return ProcessResult(
            status="completed",
            message=f"All {input.total_items} items processed",
            processed_count=input.processed_count,
        )

    # Calculate batch range
    start = input.processed_count
    end = min(start + input.batch_size, input.total_items)

    # Process this batch
    result = await process_batch(ctx, start, end, input.total_items)
    new_count = input.processed_count + result["count"]

    print(f"[BATCH] Progress: {new_count}/{input.total_items}")

    # Check if done
    if new_count >= input.total_items:
        return ProcessResult(
            status="completed",
            message=f"All {input.total_items} items processed",
            processed_count=new_count,
        )

    # Not done yet - recur with updated state
    # This archives the current history and starts a fresh instance
    await recur(
        ctx,
        total_items=input.total_items,
        batch_size=input.batch_size,
        processed_count=new_count,
    )

    # This line is never reached (recur raises RecurException)
    return ProcessResult(status="recurred", message="", processed_count=new_count)


# =============================================================================
# Infinite Event Listener Example
# =============================================================================


@workflow(event_handler=True)
async def event_listener(
    ctx: WorkflowContext, input: EventListenerInput
) -> ProcessResult:
    """
    Event listener that runs indefinitely using recur().

    This workflow:
    1. Joins a message group
    2. Waits for events
    3. Processes events
    4. After N events, recurs to reset history

    Note: After recur(), the workflow must re-join groups as
    cleanup_instance_subscriptions() removes all subscriptions.
    """
    # Subscribe to events channel (must be done each time, including after recur)
    await subscribe(ctx, "app.event", mode="broadcast")

    print(
        f"[LISTENER {input.listener_id}] "
        f"Waiting for events (processed so far: {input.events_processed})"
    )

    # Wait for an event
    msg = await receive(ctx, channel="app.event", timeout_seconds=3600)

    # Handle the event
    await handle_event(ctx, msg.data)
    new_count = input.events_processed + 1

    print(f"[LISTENER {input.listener_id}] Processed event #{new_count}")

    # Check if we should recur (to prevent history growth)
    if new_count >= input.max_events_before_recur:
        print(f"[LISTENER {input.listener_id}] Recurring after {new_count} events")
        await recur(
            ctx,
            listener_id=input.listener_id,
            events_processed=0,  # Reset count after recur
            max_events_before_recur=input.max_events_before_recur,
        )

    # Continue listening (recur to wait for next event)
    await recur(
        ctx,
        listener_id=input.listener_id,
        events_processed=new_count,
        max_events_before_recur=input.max_events_before_recur,
    )

    # Never reached
    return ProcessResult(
        status="listening", message="", processed_count=new_count
    )


@workflow(event_handler=True)
async def event_publisher(
    ctx: WorkflowContext, input: EventPublisherInput
) -> ProcessResult:
    """
    Publishes an event to all listeners.
    """
    message_id = await publish(
        ctx,
        channel="app.event",
        data={"message": input.message, "timestamp": "now"},
    )

    return ProcessResult(
        status="published",
        message=f"Event published with ID {message_id}",
        processed_count=1,
    )


# =============================================================================
# Application Setup
# =============================================================================

app = EddaApp(
    db_url="sqlite:///long_running_loop_example.db",
)

if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="127.0.0.1", port=8003)
