# LlamaIndex Workflow Integration

Edda provides integration with [LlamaIndex Workflow](https://docs.llamaindex.ai/en/stable/module_guides/workflow/), making your LlamaIndex Workflows **durable**. Each workflow step is automatically recorded as an Edda Activity, enabling crash recovery and deterministic replay.

## Overview

LlamaIndex Workflow is an event-driven orchestration framework for building complex AI pipelines. Edda's integration adds:

- **Crash Recovery**: If your workflow crashes, completed steps are replayed from cache
- **Durable Timers**: Use `DurableSleepEvent` for timers that survive crashes
- **Durable Event Waiting**: Use `DurableWaitEvent` to wait for external events durably
- **Step-level Durability**: Each `@step` becomes a durable Edda Activity

## Installation

```bash
pip install 'edda-framework[llamaindex]'

# Or using uv
uv add edda-framework --extra llamaindex
```

## Quick Start

```python
from llama_index.core.workflow import Workflow, step, StartEvent, StopEvent, Event
from edda import EddaApp, workflow, WorkflowContext
from edda.integrations.llamaindex import DurableWorkflowRunner

# Define events
class OrderReceivedEvent(Event):
    order_id: str
    amount: float

class ProcessingCompleteEvent(Event):
    order_id: str
    status: str

# Define LlamaIndex Workflow
class OrderWorkflow(Workflow):
    @step
    async def receive_order(self, ctx, ev: StartEvent) -> OrderReceivedEvent:
        print(f"Received order: {ev.order_id}")
        return OrderReceivedEvent(order_id=ev.order_id, amount=ev.amount)

    @step
    async def process_order(self, ctx, ev: OrderReceivedEvent) -> ProcessingCompleteEvent:
        print(f"Processing order {ev.order_id}...")
        return ProcessingCompleteEvent(order_id=ev.order_id, status="processed")

    @step
    async def complete_order(self, ctx, ev: ProcessingCompleteEvent) -> StopEvent:
        return StopEvent(result={
            "order_id": ev.order_id,
            "status": ev.status,
            "message": "Order completed successfully",
        })

# Create durable runner
runner = DurableWorkflowRunner(OrderWorkflow)

# Wrap in Edda workflow
@workflow
async def order_workflow(ctx: WorkflowContext, order_id: str, amount: float) -> dict:
    result = await runner.run(ctx, order_id=order_id, amount=amount)
    return result

# Run the workflow
async def main():
    app = EddaApp(service_name="order-service", db_url="sqlite:///app.db")
    await app.initialize()

    instance_id = await order_workflow.start(order_id="ORD-001", amount=99.99)
    print(f"Started workflow: {instance_id}")

    await app.shutdown()
```

## How It Works

```
LlamaIndex Workflow Steps
        │
        ▼
┌──────────────────────┐
│ DurableWorkflowRunner│  ← Wraps your Workflow class
└──────────┬───────────┘
           │
           ▼
┌──────────────────────┐
│   For each @step:    │
│   - Serialize event  │
│   - Run as Activity  │
│   - Deserialize result│
└──────────┬───────────┘
           │
           ▼
┌──────────────────────┐
│   Edda @activity     │  ← Each step is durable
│   (_run_step)        │
└──────────────────────┘
```

**On Replay**: If the workflow crashes and restarts, completed steps return cached results from the Edda history. Steps are not re-executed.

## Durable Sleep

Use `DurableSleepEvent` for timers that survive crashes:

```python
from edda.integrations.llamaindex import DurableSleepEvent

class RateLimitedWorkflow(Workflow):
    @step
    async def check_rate_limit(self, ctx, ev: StartEvent) -> DurableSleepEvent | ProcessEvent:
        if is_rate_limited():
            # Sleep for 60 seconds (durable timer)
            return DurableSleepEvent(
                seconds=60,
                resume_data={"retry_count": ev.retry_count + 1}
            )
        return ProcessEvent(data=ev.data)

    @step
    async def handle_resume(self, ctx, ev: ResumeEvent) -> ProcessEvent:
        # Called after DurableSleepEvent completes
        retry_count = ev.data.get("retry_count", 0)
        return ProcessEvent(data={"retried": retry_count})
```

## Durable Wait for External Events

Use `DurableWaitEvent` to wait for external events:

```python
from edda.integrations.llamaindex import DurableWaitEvent, ResumeEvent

class ApprovalWorkflow(Workflow):
    @step
    async def request_approval(self, ctx, ev: StartEvent) -> DurableWaitEvent:
        # Wait for approval event (up to 1 hour)
        return DurableWaitEvent(
            event_type=f"approval.{ev.request_id}",
            timeout_seconds=3600,
        )

    @step
    async def process_approval(self, ctx, ev: ResumeEvent) -> StopEvent:
        # Called when external event arrives
        approved = ev.data.get("approved", False)
        return StopEvent(result={"approved": approved})
```

To send the external event:

```python
from edda import send_event

await send_event(
    event_type="approval.REQ-123",
    source="approval-service",
    data={"approved": True},
)
```

## API Reference

### DurableWorkflowRunner

```python
from edda.integrations.llamaindex import DurableWorkflowRunner

runner = DurableWorkflowRunner(MyWorkflow)
result = await runner.run(ctx, **start_event_kwargs)
```

| Parameter | Type | Description |
|-----------|------|-------------|
| `workflow_class` | `type[Workflow]` | LlamaIndex Workflow class (not instance) |

### DurableSleepEvent

```python
DurableSleepEvent(
    seconds: float,           # Duration to sleep
    resume_data: dict = None, # Data passed to ResumeEvent
)
```

### DurableWaitEvent

```python
DurableWaitEvent(
    event_type: str,              # Event type to wait for
    timeout_seconds: float = None, # Optional timeout
)
```

### ResumeEvent

Returned after `DurableSleepEvent` or `DurableWaitEvent` completes:

```python
class ResumeEvent:
    data: dict  # Contains resume_data or received event data
```

## Related Documentation

- [Workflows and Activities](../core-features/workflows-activities.md) - Core concepts of durable execution
- [Event Waiting](../core-features/events/wait-event.md) - wait_event() function
- [LlamaIndex Workflow Docs](https://docs.llamaindex.ai/en/stable/module_guides/workflow/) - Official LlamaIndex documentation
- [Examples](https://github.com/i2y/edda/tree/main/examples/llamaindex) - Complete working examples
