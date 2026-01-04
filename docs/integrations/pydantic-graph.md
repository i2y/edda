# pydantic-graph Integration (Experimental)

> **Status: Experimental**
> This integration is functional but has known limitations with type hints. The API may change in future versions.

Edda provides integration with [pydantic-graph](https://ai.pydantic.dev/pydantic-graph/), making graph-based workflows **durable**. Each node execution is automatically recorded as an Edda Activity, enabling crash recovery and deterministic replay.

## Overview

pydantic-graph is a graph-based workflow library that uses dataclass nodes and type-safe transitions. Edda's integration adds:

- **Crash Recovery**: If your workflow crashes, completed nodes are replayed from cache
- **Durable Timers**: Use `Sleep` marker for timers that survive crashes
- **Durable Event Waiting**: Use `WaitForEvent` marker to wait for external events durably
- **Node-level Durability**: Each node execution becomes a durable Edda Activity

## Known Limitations

**Type hint limitation**: The `Sleep` and `WaitForEvent` markers cannot be included in node return type annotations due to pydantic-graph's strict type validation. You need to use `# type: ignore` comments:

```python
# This works at runtime but requires type: ignore
async def run(self, ctx: DurableGraphContext) -> "NextNode":  # type: ignore[override]
    return Sleep(seconds=60, next_node=NextNode())
```

If you need clean type hints for graph visualization, consider the [LlamaIndex Workflow integration](./llamaindex.md) instead.

## Installation

```bash
pip install 'edda-framework[graph]'

# Or using uv
uv add edda-framework --extra graph
```

## Quick Start

```python
from dataclasses import dataclass
from pydantic_graph import BaseNode, End, Graph
from edda import EddaApp, workflow, WorkflowContext
from edda.integrations.graph import DurableGraph, DurableGraphContext

# Define state
@dataclass
class OrderState:
    order_id: str | None = None
    total: float = 0.0
    status: str = "pending"

# Define nodes
@dataclass
class ValidateOrderNode(BaseNode[OrderState, None, dict]):
    order_id: str
    amount: float

    async def run(self, ctx: DurableGraphContext) -> "ProcessPaymentNode":
        ctx.state.order_id = self.order_id
        ctx.state.total = self.amount
        ctx.state.status = "validated"
        return ProcessPaymentNode()

@dataclass
class ProcessPaymentNode(BaseNode[OrderState, None, dict]):
    async def run(self, ctx: DurableGraphContext) -> End[dict]:
        ctx.state.status = "paid"
        return End({
            "order_id": ctx.state.order_id,
            "total": ctx.state.total,
            "status": ctx.state.status,
        })

# Create graph and durable wrapper
graph = Graph(nodes=[ValidateOrderNode, ProcessPaymentNode])
durable_graph = DurableGraph(graph)

# Wrap in Edda workflow
@workflow
async def order_workflow(ctx: WorkflowContext, order_id: str, amount: float) -> dict:
    return await durable_graph.run(
        ctx,
        start_node=ValidateOrderNode(order_id=order_id, amount=amount),
        state=OrderState(),
    )

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
pydantic-graph Nodes
        │
        ▼
┌──────────────────────┐
│    DurableGraph      │  ← Wraps your Graph
└──────────┬───────────┘
           │
           ▼
┌──────────────────────┐
│   For each node:     │
│   - Serialize state  │
│   - Run as Activity  │
│   - Deserialize result│
└──────────┬───────────┘
           │
           ▼
┌──────────────────────┐
│   Edda @activity     │  ← Each node is durable
│   (_run_graph_node)  │
└──────────────────────┘
```

**On Replay**: If the workflow crashes and restarts, completed nodes return cached results from the Edda history. Nodes are not re-executed.

## Durable Sleep

Use the `Sleep` marker for timers that survive crashes:

```python
from edda.integrations.graph import Sleep

@dataclass
class RateLimitNode(BaseNode[MyState, None, str]):
    async def run(self, ctx: DurableGraphContext) -> "RetryNode":  # type: ignore[override]
        if is_rate_limited():
            # Sleep for 60 seconds (durable timer)
            return Sleep(seconds=60, next_node=RetryNode())
        return RetryNode()
```

## Durable Wait for External Events

Use the `WaitForEvent` marker to wait for external events:

```python
from edda.integrations.graph import WaitForEvent, ReceivedEvent

@dataclass
class WaitForApprovalNode(BaseNode[OrderState, None, str]):
    async def run(self, ctx: DurableGraphContext) -> "ProcessApprovalNode":  # type: ignore[override]
        return WaitForEvent(
            event_type=f"approval.{ctx.state.order_id}",
            next_node=ProcessApprovalNode(),
            timeout_seconds=3600,
        )

@dataclass
class ProcessApprovalNode(BaseNode[OrderState, None, str]):
    async def run(self, ctx: DurableGraphContext) -> End[str]:
        # Access the received event via ctx.last_event
        event: ReceivedEvent = ctx.last_event
        if event.data.get("approved"):
            return End("approved")
        return End("rejected")
```

To send the external event:

```python
from edda import send_event

await send_event(
    event_type="approval.ORD-123",
    source="approval-service",
    data={"approved": True},
)
```

## DurableGraphContext

The `DurableGraphContext` provides access to state, dependencies, and received events:

```python
@dataclass
class MyNode(BaseNode[MyState, MyDeps, str]):
    async def run(self, ctx: DurableGraphContext) -> End[str]:
        # Access state (mutable)
        ctx.state.counter += 1

        # Access dependencies (read-only)
        api_key = ctx.deps.api_key

        # Access last received event (after WaitForEvent)
        if ctx.last_event:
            data = ctx.last_event.data

        # Access Edda WorkflowContext for advanced operations
        edda_ctx = ctx.workflow_ctx

        return End("done")
```

## API Reference

### DurableGraph

```python
from edda.integrations.graph import DurableGraph

durable = DurableGraph(graph)
result = await durable.run(ctx, start_node=MyNode(), state=MyState(), deps=MyDeps())
```

| Parameter | Type | Description |
|-----------|------|-------------|
| `graph` | `Graph` | pydantic-graph Graph instance |

### Sleep

```python
Sleep(
    seconds: int,      # Duration to sleep
    next_node: NextT,  # Node to continue with after sleep
)
```

### WaitForEvent

```python
WaitForEvent(
    event_type: str,               # Event type to wait for
    next_node: NextT,              # Node to continue with after event
    timeout_seconds: int | None,   # Optional timeout
)
```

### ReceivedEvent

Available via `ctx.last_event` after `WaitForEvent` completes:

```python
@dataclass
class ReceivedEvent:
    event_type: str              # The event type that was received
    data: dict[str, Any]         # Event payload
    metadata: dict[str, Any]     # Event metadata
```

## Related Documentation

- [Workflows and Activities](../core-features/workflows-activities.md) - Core concepts of durable execution
- [Event Waiting](../core-features/events/wait-event.md) - wait_event() function
- [LlamaIndex Integration](./llamaindex.md) - Alternative with better type hint support
- [pydantic-graph Documentation](https://ai.pydantic.dev/pydantic-graph/) - Official pydantic-graph docs
- [Examples](https://github.com/i2y/edda/tree/main/examples/graph) - Complete working examples
