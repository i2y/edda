# Lifecycle Hooks

Edda provides a Protocol-based hook system that allows you to integrate custom observability and monitoring tools without coupling the framework to specific vendors.

## Overview

The hook system enables you to:

- ✅ Add distributed tracing (Logfire, Jaeger, etc.)
- ✅ Send metrics to monitoring systems (Datadog, Prometheus)
- ✅ Track errors (Sentry, custom logging)
- ✅ Audit workflow execution
- ✅ Combine multiple observability backends

**Design Philosophy:** Edda focuses on workflow orchestration, while observability is delegated to users through a flexible hook system.

## Quick Start

### 1. Implement WorkflowHooks

```python
from edda import HooksBase
import logfire

class LogfireHooks(HooksBase):
    """Logfire integration using Edda hooks."""

    async def on_workflow_start(self, instance_id, workflow_name, input_data):
        logfire.info("workflow.start",
            instance_id=instance_id,
            workflow_name=workflow_name)

    async def on_activity_complete(self, instance_id, activity_id, activity_name, result, cache_hit):
        logfire.info("activity.complete",
            instance_id=instance_id,
            activity_id=activity_id,
            activity_name=activity_name,
            cache_hit=cache_hit
        )
```

### 2. Pass Hooks to EddaApp

```python
from edda import EddaApp

app = EddaApp(
    service_name="my-service",
    db_url="sqlite:///workflow.db",
    hooks=LogfireHooks(),  # <-- Your hook implementation
)
await app.initialize()  # Initialize before starting workflows
```

### 3. Run Your Workflows

All lifecycle events are automatically captured:

- ✅ Workflow start, complete, failure, cancellation
- ✅ Activity execution (with cache hit/miss tracking)
- ✅ Event send/receive

## Available Hooks

The `WorkflowHooks` Protocol defines these methods (all optional):

| Hook Method | Parameters | Description |
|-------------|------------|-------------|
| `on_workflow_start` | `instance_id`, `workflow_name`, `input_data` | Called when a workflow starts execution |
| `on_workflow_complete` | `instance_id`, `workflow_name`, `result` | Called when a workflow completes successfully |
| `on_workflow_failed` | `instance_id`, `workflow_name`, `error` | Called when a workflow fails with an exception |
| `on_workflow_cancelled` | `instance_id`, `workflow_name` | Called when a workflow is cancelled |
| `on_activity_start` | `instance_id`, `activity_id`, `activity_name`, `is_replaying` | Called before an activity executes |
| `on_activity_complete` | `instance_id`, `activity_id`, `activity_name`, `result`, `cache_hit` | Called after an activity completes successfully |
| `on_activity_failed` | `instance_id`, `activity_id`, `activity_name`, `error` | Called when an activity fails with an exception |
| `on_event_sent` | `event_type`, `event_source`, `event_data` | Called when an event is sent (transactional outbox) |
| `on_event_received` | `instance_id`, `event_type`, `event_data` | Called when a workflow receives an awaited event |

## Best Practices

### 1. Scrub Sensitive Data

Always remove sensitive information from logs:

```python
from edda import HooksBase

SENSITIVE_FIELDS = {"password", "api_key", "token", "ssn", "credit_card"}

def scrub_data(data: dict) -> dict:
    """Remove sensitive fields from data."""
    return {
        k: "***REDACTED***" if k.lower() in SENSITIVE_FIELDS else v
        for k, v in data.items()
    }

class SecureHooks(HooksBase):
    async def on_workflow_start(self, instance_id, workflow_name, input_data):
        safe_data = scrub_data(input_data)
        logfire.info("workflow.start", input_data=safe_data)
```

### 2. Handle Hook Errors Gracefully

Don't let hook failures break your workflows:

```python
from edda import HooksBase

class RobustHooks(HooksBase):
    async def on_workflow_start(self, instance_id, workflow_name, input_data):
        try:
            await send_metrics(...)
        except Exception as e:
            # Log but don't raise (workflow should continue)
            print(f"Hook error: {e}")
```

### 3. Use Sampling in Production

For high-throughput systems, sample traces:

```python
from edda import HooksBase
import random

class SampledHooks(HooksBase):
    def __init__(self, sample_rate=0.1):
        self.sample_rate = sample_rate

    async def on_workflow_start(self, instance_id, workflow_name, input_data):
        if random.random() < self.sample_rate:
            logfire.info("workflow.start", instance_id=instance_id)

    async def on_workflow_failed(self, instance_id, workflow_name, error):
        # Always log errors (100% sampling)
        logfire.error("workflow.failed", error=str(error))
```

## Integration Examples

### Pydantic Logfire

```python
from edda import HooksBase
import logfire

class LogfireHooks(HooksBase):
    async def on_workflow_start(self, instance_id, workflow_name, input_data):
        logfire.info("workflow.start",
            instance_id=instance_id,
            workflow_name=workflow_name)

    async def on_activity_complete(self, instance_id, activity_id, activity_name, result, cache_hit):
        logfire.info("activity.complete",
            activity_id=activity_id,
            activity_name=activity_name,
            cache_hit=cache_hit)

app = EddaApp(service_name="my-service", db_url="...", hooks=LogfireHooks())
await app.initialize()  # Initialize before starting workflows
```

**What you get:**

- Distributed tracing across workflows
- Activity execution spans with cache hit/miss
- Automatic SQLite query instrumentation
- OpenTelemetry-compatible (works with Jaeger, Grafana, etc.)

### Datadog

```python
from edda import HooksBase
from datadog import statsd
from ddtrace import tracer

class DatadogHooks(HooksBase):
    async def on_workflow_start(self, instance_id, workflow_name, input_data):
        statsd.increment('edda.workflow.started', tags=[f'workflow:{workflow_name}'])

        with tracer.trace("workflow.start", service="edda") as span:
            span.set_tag("workflow.name", workflow_name)
            span.set_tag("instance.id", instance_id)

    async def on_activity_complete(self, instance_id, activity_id, activity_name, result, cache_hit):
        statsd.increment('edda.activity.completed',
            tags=[f'activity:{activity_name}', f'cache_hit:{cache_hit}'])
```

### Prometheus

```python
from edda import HooksBase
from prometheus_client import Counter, Histogram

workflow_started = Counter('edda_workflow_started_total', 'Total workflows started', ['workflow_name'])
activity_executed = Counter('edda_activity_executed_total', 'Activities executed', ['activity_name', 'cache_hit'])

class PrometheusHooks(HooksBase):
    async def on_workflow_start(self, instance_id, workflow_name, input_data):
        workflow_started.labels(workflow_name=workflow_name).inc()

    async def on_activity_complete(self, instance_id, activity_id, activity_name, result, cache_hit):
        activity_executed.labels(activity_name=activity_name, cache_hit=str(cache_hit)).inc()
```

### Sentry Error Tracking

```python
from edda import HooksBase
import sentry_sdk

class SentryHooks(HooksBase):
    async def on_workflow_failed(self, instance_id, workflow_name, error):
        with sentry_sdk.push_scope() as scope:
            scope.set_context("workflow", {
                "instance_id": instance_id,
                "workflow_name": workflow_name,
            })
            sentry_sdk.capture_exception(error)

    async def on_activity_failed(self, instance_id, activity_id, activity_name, error):
        with sentry_sdk.push_scope() as scope:
            scope.set_context("activity", {
                "instance_id": instance_id,
                "activity_id": activity_id,
                "activity_name": activity_name,
            })
            sentry_sdk.capture_exception(error)
```

### OpenTelemetry (Official Integration)

Edda provides an official OpenTelemetry integration with full tracing, optional metrics, and W3C Trace Context propagation.

```python
from edda import EddaApp
from edda.integrations.opentelemetry import OpenTelemetryHooks

hooks = OpenTelemetryHooks(
    service_name="order-service",
    otlp_endpoint="http://localhost:4317",  # Optional
    enable_metrics=True,  # Optional
)

app = EddaApp(
    service_name="order-service",
    db_url="sqlite:///workflow.db",
    hooks=hooks,
)
```

**Features:**

- ✅ Distributed tracing with parent-child span relationships
- ✅ Optional metrics (counters, histograms)
- ✅ W3C Trace Context propagation via CloudEvents
- ✅ Automatic context inheritance from ASGI/WSGI middleware

👉 **See [OpenTelemetry Integration](../integrations/opentelemetry.md) for full documentation.**

## See Also

- **[OpenTelemetry Integration](../integrations/opentelemetry.md)**: Official OpenTelemetry integration with full documentation
- **[Complete Logfire Example](https://github.com/i2y/edda/blob/main/examples/observability_with_logfire.py)**: Full implementation with multiple workflows
- **[Complete OpenTelemetry Example](https://github.com/i2y/edda/blob/main/examples/observability_with_opentelemetry.py)**: Full implementation with tracing, optional metrics, and CloudEvents context propagation
- **[Observability Guide](https://github.com/i2y/edda/blob/main/examples/README_observability.md)**: Detailed guide with more integration examples
- **[API Reference](https://github.com/i2y/edda/blob/main/edda/hooks.py)**: WorkflowHooks Protocol definition
