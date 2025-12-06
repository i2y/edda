# OpenTelemetry Integration

Edda provides official integration with [OpenTelemetry](https://opentelemetry.io/), enabling distributed tracing and optional metrics for your durable workflows.

## Overview

OpenTelemetry is an industry-standard observability framework. Edda's OpenTelemetry integration provides:

- **Distributed Tracing**: Workflow and activity spans with parent-child relationships
- **Optional Metrics**: Counters for workflow/activity execution, histograms for duration
- **W3C Trace Context**: Propagate traces across service boundaries via CloudEvents
- **Automatic Context Inheritance**: Inherit from ASGI/WSGI middleware or CloudEvents headers

## Installation

Install Edda with OpenTelemetry support:

```bash
pip install edda-framework[opentelemetry]

# Or using uv
uv add edda-framework --extra opentelemetry
```

## Quick Start

```python
from edda import EddaApp, workflow, activity, WorkflowContext
from edda.integrations.opentelemetry import OpenTelemetryHooks

# Create hooks (console exporter for development)
hooks = OpenTelemetryHooks(
    service_name="order-service",
    otlp_endpoint=None,  # Use console exporter
    enable_metrics=False,
)

# Or with OTLP exporter for production (Jaeger, Tempo, etc.)
hooks = OpenTelemetryHooks(
    service_name="order-service",
    otlp_endpoint="http://localhost:4317",
    enable_metrics=True,
)

app = EddaApp(
    service_name="order-service",
    db_url="sqlite:///workflow.db",
    hooks=hooks,
)

@activity
async def reserve_inventory(ctx: WorkflowContext, order_id: str):
    return {"reserved": True}

@workflow
async def order_workflow(ctx: WorkflowContext, order_id: str):
    await reserve_inventory(ctx, order_id)
    return {"status": "completed"}

async def main():
    await app.initialize()
    await order_workflow.start(order_id="ORD-123")
```

## Span Hierarchy

Edda creates a hierarchical span structure:

```
workflow:order_workflow (parent)
├── activity:reserve_inventory:1 (child)
├── activity:process_payment:1 (child)
└── activity:ship_order:1 (child)
```

## Span Attributes

**Workflow Spans**:

- `edda.workflow.instance_id`
- `edda.workflow.name`
- `edda.workflow.cancelled` (when cancelled)

**Activity Spans**:

- `edda.activity.id` (e.g., "reserve_inventory:1")
- `edda.activity.name`
- `edda.activity.is_replaying`
- `edda.activity.cache_hit`

## Metrics (Optional)

When `enable_metrics=True`:

| Metric | Type | Description |
|--------|------|-------------|
| `edda.workflow.started` | Counter | Workflows started |
| `edda.workflow.completed` | Counter | Workflows completed |
| `edda.workflow.failed` | Counter | Workflows failed |
| `edda.workflow.duration` | Histogram | Workflow execution time |
| `edda.activity.executed` | Counter | Activities executed |
| `edda.activity.cache_hit` | Counter | Activity cache hits |
| `edda.activity.duration` | Histogram | Activity execution time |

## Trace Context Propagation

### Automatic Context Inheritance

OpenTelemetryHooks automatically inherits trace context from multiple sources, with the following priority:

1. **Explicit `_trace_context` in input_data** (highest priority)

   - Extracted from CloudEvents extension attributes
   - Useful for cross-service trace propagation

2. **Current active span** (e.g., from ASGI/WSGI middleware)

   - Automatically detected using `trace.get_current_span()`
   - Works with OpenTelemetry instrumentation middleware

3. **New root span** (if no parent context is found)

### CloudEvents Integration

Inject trace context when sending events:

```python
from edda.integrations.opentelemetry import inject_trace_context

event_data = {"order_id": "ORD-123"}
event_data = inject_trace_context(hooks, ctx.instance_id, event_data)
await send_event_transactional(ctx, "order.shipped", "orders", event_data)
```

When a CloudEvent contains W3C Trace Context extension attributes (`traceparent`, `tracestate`), they are automatically extracted and used as the parent context:

```bash
# CloudEvent with trace context
curl -X POST http://localhost:8001/ \
  -H "Content-Type: application/json" \
  -H "ce-specversion: 1.0" \
  -H "ce-type: order.created" \
  -H "ce-source: external-service" \
  -H "ce-id: event-123" \
  -H "ce-traceparent: 00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01" \
  -d '{"order_id": "ORD-123"}'
```

### ASGI/WSGI Middleware

OpenTelemetryHooks automatically inherits from the current active span:

```python
from opentelemetry.instrumentation.asgi import OpenTelemetryMiddleware

# Middleware creates parent span for each HTTP request
app = OpenTelemetryMiddleware(edda_app)

# Workflow spans automatically inherit from the request span
```

### Existing TracerProvider Reuse

If a TracerProvider is already configured (e.g., by ASGI middleware or your application), OpenTelemetryHooks will reuse it instead of creating a new one:

```python
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider

# Configure your own provider
provider = TracerProvider(resource=my_resource)
trace.set_tracer_provider(provider)

# OpenTelemetryHooks will use the existing provider
hooks = OpenTelemetryHooks(service_name="my-service")
# No new provider is created!
```

## Related Documentation

- [Lifecycle Hooks](../core-features/hooks.md) - Detailed hooks documentation
- [Example](https://github.com/i2y/edda/blob/main/examples/observability_with_opentelemetry.py) - Complete working example
- [OpenTelemetry Documentation](https://opentelemetry.io/docs/)
