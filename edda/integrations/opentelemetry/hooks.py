"""
OpenTelemetry hooks implementation for Edda workflows.

This module provides the OpenTelemetryHooks class that integrates OpenTelemetry
tracing and optional metrics with Edda's workflow execution.
"""

from __future__ import annotations

import time
from typing import TYPE_CHECKING, Any

from edda.hooks import HooksBase

if TYPE_CHECKING:
    from opentelemetry.context import Context
    from opentelemetry.trace import Span, Tracer

# Check if OpenTelemetry is available
try:
    from opentelemetry import trace
    from opentelemetry.context import Context
    from opentelemetry.sdk.resources import Resource
    from opentelemetry.sdk.trace import TracerProvider
    from opentelemetry.sdk.trace.export import (
        BatchSpanProcessor,
        ConsoleSpanExporter,
    )
    from opentelemetry.trace import Span, Status, StatusCode, Tracer
    from opentelemetry.trace.propagation.tracecontext import (
        TraceContextTextMapPropagator,
    )

    _OPENTELEMETRY_AVAILABLE = True
except ImportError:
    _OPENTELEMETRY_AVAILABLE = False


class OpenTelemetryHooks(HooksBase):
    """
    OpenTelemetry tracing and metrics integration for Edda workflows.

    Creates distributed traces with:
    - Workflow spans as parent spans
    - Activity spans as child spans
    - Error recording and status propagation
    - Retry event tracking
    - Optional metrics (counters, histograms)

    Span Hierarchy::

        workflow:order_workflow (parent)
        ├── activity:reserve_inventory (child)
        │   └── [event: retry] (if retry occurs)
        ├── activity:process_payment (child)
        └── activity:ship_order (child)
            └── [event: event_received] (if wait_event used)

    Example::

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

    Attributes:
        service.name: Service name for resource identification
        service.version: Service version (default: "1.0.0")
        edda.framework: Always "true" to identify Edda workflows

    Installation::

        pip install edda-framework[opentelemetry]
    """

    def __init__(
        self,
        service_name: str = "edda",
        otlp_endpoint: str | None = None,
        enable_metrics: bool = False,
    ) -> None:
        """
        Initialize OpenTelemetry hooks.

        Args:
            service_name: Service name for resource identification
            otlp_endpoint: OTLP endpoint URL (e.g., "http://localhost:4317").
                          If None, uses ConsoleSpanExporter for local development.
            enable_metrics: Enable OpenTelemetry metrics (counters, histograms)

        Raises:
            ImportError: If OpenTelemetry packages are not installed
        """
        if not _OPENTELEMETRY_AVAILABLE:
            raise ImportError(
                "OpenTelemetry packages are not installed. "
                "Install them with: pip install edda-framework[opentelemetry]"
            )

        self._tracer = self._setup_tracing(service_name, otlp_endpoint)
        self._propagator = TraceContextTextMapPropagator()

        # Span lifecycle management
        self._workflow_spans: dict[str, Span] = {}
        self._activity_spans: dict[str, Span] = {}
        self._workflow_start_times: dict[str, float] = {}
        self._activity_start_times: dict[str, float] = {}

        # Optional metrics
        self._enable_metrics = enable_metrics
        if enable_metrics:
            self._setup_metrics(service_name, otlp_endpoint)

    def _setup_tracing(self, service_name: str, otlp_endpoint: str | None) -> Tracer:
        """Configure OpenTelemetry tracing.

        If a TracerProvider is already configured (e.g., by ASGI/WSGI middleware),
        it will be reused instead of creating a new one. This enables trace context
        propagation from external sources.
        """
        from opentelemetry.trace import NoOpTracerProvider

        # Check if a TracerProvider is already configured
        existing_provider = trace.get_tracer_provider()

        # Only create new provider if none exists (NoOpTracerProvider is the default)
        if isinstance(existing_provider, NoOpTracerProvider):
            resource = Resource.create(
                {
                    "service.name": service_name,
                    "service.version": "1.0.0",
                    "edda.framework": "true",
                }
            )

            provider = TracerProvider(resource=resource)

            if otlp_endpoint:
                # Production: OTLP exporter
                try:
                    from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import (
                        OTLPSpanExporter,
                    )

                    exporter = OTLPSpanExporter(endpoint=otlp_endpoint, insecure=True)
                    provider.add_span_processor(BatchSpanProcessor(exporter))
                except ImportError:
                    # Fallback to console if OTLP exporter not installed
                    provider.add_span_processor(BatchSpanProcessor(ConsoleSpanExporter()))
            else:
                # Development: Console exporter
                provider.add_span_processor(BatchSpanProcessor(ConsoleSpanExporter()))

            trace.set_tracer_provider(provider)

        # Always get tracer from current provider (whether new or existing)
        return trace.get_tracer("edda.opentelemetry", "1.0.0")

    def _setup_metrics(self, service_name: str, otlp_endpoint: str | None) -> None:
        """Configure OpenTelemetry metrics (optional)."""
        try:
            from opentelemetry import metrics
            from opentelemetry.sdk.metrics import MeterProvider
            from opentelemetry.sdk.metrics.export import (
                ConsoleMetricExporter,
                MetricExporter,
                PeriodicExportingMetricReader,
            )

            exporter: MetricExporter
            if otlp_endpoint:
                try:
                    from opentelemetry.exporter.otlp.proto.grpc.metric_exporter import (
                        OTLPMetricExporter,
                    )

                    exporter = OTLPMetricExporter(endpoint=otlp_endpoint, insecure=True)
                except ImportError:
                    exporter = ConsoleMetricExporter()
            else:
                exporter = ConsoleMetricExporter()

            reader = PeriodicExportingMetricReader(exporter, export_interval_millis=10000)
            resource = Resource.create({"service.name": service_name})
            provider = MeterProvider(resource=resource, metric_readers=[reader])
            metrics.set_meter_provider(provider)

            meter = metrics.get_meter("edda.opentelemetry", "1.0.0")

            # Counters
            self._workflow_started_counter = meter.create_counter(
                "edda.workflow.started",
                description="Number of workflows started",
                unit="1",
            )
            self._workflow_completed_counter = meter.create_counter(
                "edda.workflow.completed",
                description="Number of workflows completed",
                unit="1",
            )
            self._workflow_failed_counter = meter.create_counter(
                "edda.workflow.failed",
                description="Number of workflows failed",
                unit="1",
            )
            self._activity_executed_counter = meter.create_counter(
                "edda.activity.executed",
                description="Number of activities executed (not cache hit)",
                unit="1",
            )
            self._activity_cache_hit_counter = meter.create_counter(
                "edda.activity.cache_hit",
                description="Number of activity cache hits (replay)",
                unit="1",
            )

            # Histograms
            self._workflow_duration_histogram = meter.create_histogram(
                "edda.workflow.duration",
                description="Workflow execution duration",
                unit="s",
            )
            self._activity_duration_histogram = meter.create_histogram(
                "edda.activity.duration",
                description="Activity execution duration",
                unit="s",
            )
        except ImportError:
            self._enable_metrics = False

    # =========================================================================
    # Workflow Hooks
    # =========================================================================

    async def on_workflow_start(
        self, instance_id: str, workflow_name: str, input_data: dict[str, Any]
    ) -> None:
        """Start a workflow span (parent for all activities).

        Trace context is inherited in the following priority:
        1. Explicit _trace_context in input_data (e.g., from CloudEvents)
        2. Current active span (e.g., from ASGI/WSGI middleware)
        3. None (creates a new root span)
        """
        # Priority 1: Extract trace context from input_data (CloudEvents, manual)
        parent_context = self._extract_trace_context(input_data)

        # Priority 2: Inherit from current active span (ASGI/WSGI middleware)
        if parent_context is None:
            current_span = trace.get_current_span()
            if current_span.is_recording():
                parent_context = trace.set_span_in_context(current_span)

        span = self._tracer.start_span(
            name=f"workflow:{workflow_name}",
            context=parent_context,
            attributes={
                "edda.workflow.instance_id": instance_id,
                "edda.workflow.name": workflow_name,
                "edda.workflow.input_keys": str(list(input_data.keys())),
            },
        )
        self._workflow_spans[instance_id] = span
        self._workflow_start_times[instance_id] = time.time()

        # Metrics
        if self._enable_metrics:
            self._workflow_started_counter.add(1, {"workflow_name": workflow_name})

    async def on_workflow_complete(
        self, instance_id: str, workflow_name: str, result: Any  # noqa: ARG002
    ) -> None:
        """End workflow span with success status."""
        span = self._workflow_spans.pop(instance_id, None)
        if span:
            span.set_status(Status(StatusCode.OK))
            span.end()

        # Always cleanup start time
        start_time = self._workflow_start_times.pop(instance_id, None)

        # Metrics
        if self._enable_metrics:
            self._workflow_completed_counter.add(1, {"workflow_name": workflow_name})
            if start_time:
                duration = time.time() - start_time
                self._workflow_duration_histogram.record(
                    duration, {"workflow_name": workflow_name, "status": "completed"}
                )

    async def on_workflow_failed(
        self, instance_id: str, workflow_name: str, error: Exception
    ) -> None:
        """End workflow span with error status."""
        span = self._workflow_spans.pop(instance_id, None)
        if span:
            span.set_status(Status(StatusCode.ERROR, str(error)))
            span.record_exception(error)
            span.end()

        # Always cleanup start time
        start_time = self._workflow_start_times.pop(instance_id, None)

        # Metrics
        if self._enable_metrics:
            self._workflow_failed_counter.add(
                1,
                {"workflow_name": workflow_name, "error_type": type(error).__name__},
            )
            if start_time:
                duration = time.time() - start_time
                self._workflow_duration_histogram.record(
                    duration, {"workflow_name": workflow_name, "status": "failed"}
                )

    async def on_workflow_cancelled(
        self, instance_id: str, workflow_name: str  # noqa: ARG002
    ) -> None:
        """End workflow span with cancelled status."""
        span = self._workflow_spans.pop(instance_id, None)
        if span:
            span.set_attribute("edda.workflow.cancelled", True)
            span.set_status(Status(StatusCode.OK, "Cancelled"))
            span.end()

        self._workflow_start_times.pop(instance_id, None)

    # =========================================================================
    # Activity Hooks
    # =========================================================================

    async def on_activity_start(
        self,
        instance_id: str,
        activity_id: str,
        activity_name: str,
        is_replaying: bool,
    ) -> None:
        """Start an activity span as child of workflow span."""
        parent_span = self._workflow_spans.get(instance_id)

        # Create activity span with parent context
        if parent_span:
            ctx = trace.set_span_in_context(parent_span)
            span = self._tracer.start_span(
                name=f"activity:{activity_name}",
                context=ctx,
                attributes={
                    "edda.activity.id": activity_id,
                    "edda.activity.name": activity_name,
                    "edda.activity.is_replaying": is_replaying,
                    "edda.workflow.instance_id": instance_id,
                },
            )
        else:
            # No parent workflow span (edge case)
            span = self._tracer.start_span(
                name=f"activity:{activity_name}",
                attributes={
                    "edda.activity.id": activity_id,
                    "edda.activity.name": activity_name,
                    "edda.activity.is_replaying": is_replaying,
                    "edda.workflow.instance_id": instance_id,
                },
            )

        key = f"{instance_id}:{activity_id}"
        self._activity_spans[key] = span
        self._activity_start_times[key] = time.time()

    async def on_activity_complete(
        self,
        instance_id: str,
        activity_id: str,
        activity_name: str,
        result: Any,  # noqa: ARG002
        cache_hit: bool,
    ) -> None:
        """End activity span with success status."""
        key = f"{instance_id}:{activity_id}"
        span = self._activity_spans.pop(key, None)
        if span:
            span.set_attribute("edda.activity.cache_hit", cache_hit)
            span.set_status(Status(StatusCode.OK))
            span.end()

        # Metrics
        if self._enable_metrics:
            if cache_hit:
                self._activity_cache_hit_counter.add(1, {"activity_name": activity_name})
            else:
                self._activity_executed_counter.add(1, {"activity_name": activity_name})
                start_time = self._activity_start_times.pop(key, None)
                if start_time:
                    duration = time.time() - start_time
                    self._activity_duration_histogram.record(
                        duration, {"activity_name": activity_name}
                    )

    async def on_activity_failed(
        self,
        instance_id: str,
        activity_id: str,
        activity_name: str,  # noqa: ARG002
        error: Exception,
    ) -> None:
        """End activity span with error status."""
        key = f"{instance_id}:{activity_id}"
        span = self._activity_spans.pop(key, None)
        if span:
            span.set_status(Status(StatusCode.ERROR, str(error)))
            span.record_exception(error)
            span.end()

        self._activity_start_times.pop(key, None)

    async def on_activity_retry(
        self,
        instance_id: str,
        activity_id: str,
        activity_name: str,  # noqa: ARG002
        error: Exception,
        attempt: int,
        delay: float,
    ) -> None:
        """Record retry event on current activity span."""
        key = f"{instance_id}:{activity_id}"
        span = self._activity_spans.get(key)
        if span:
            span.add_event(
                "retry",
                attributes={
                    "edda.retry.attempt": attempt,
                    "edda.retry.delay_seconds": delay,
                    "edda.retry.error": str(error),
                    "edda.retry.error_type": type(error).__name__,
                },
            )

    # =========================================================================
    # Event Hooks
    # =========================================================================

    async def on_event_sent(
        self,
        event_type: str,
        event_source: str,
        event_data: dict[str, Any],  # noqa: ARG002
    ) -> None:
        """Record event sent as a short-lived span."""
        with self._tracer.start_as_current_span(
            name=f"event:send:{event_type}",
            attributes={
                "edda.event.type": event_type,
                "edda.event.source": event_source,
            },
        ) as span:
            span.set_status(Status(StatusCode.OK))

    async def on_event_received(
        self,
        instance_id: str,
        event_type: str,
        event_data: dict[str, Any],  # noqa: ARG002
    ) -> None:
        """Record event received as an event on workflow span."""
        parent_span = self._workflow_spans.get(instance_id)
        if parent_span:
            parent_span.add_event(
                "event_received",
                attributes={
                    "edda.event.type": event_type,
                },
            )

    # =========================================================================
    # Trace Context Propagation
    # =========================================================================

    def _extract_trace_context(self, data: dict[str, Any]) -> Context | None:
        """Extract W3C Trace Context from data dict."""
        carrier: dict[str, str] = {}

        # Check _trace_context nested dict (recommended)
        if "_trace_context" in data:
            tc = data["_trace_context"]
            if isinstance(tc, dict):
                carrier.update({k: v for k, v in tc.items() if k in ("traceparent", "tracestate")})

        # Also check top-level keys
        if "traceparent" in data:
            carrier["traceparent"] = str(data["traceparent"])
        if "tracestate" in data:
            carrier["tracestate"] = str(data["tracestate"])

        return self._propagator.extract(carrier) if carrier else None

    def get_trace_context(self, instance_id: str) -> dict[str, str]:
        """
        Get W3C Trace Context for a workflow instance.

        Use this to propagate trace context to external services or CloudEvents.

        Args:
            instance_id: Workflow instance ID

        Returns:
            dict with 'traceparent' and optionally 'tracestate' keys
        """
        carrier: dict[str, str] = {}
        span = self._workflow_spans.get(instance_id)
        if span:
            ctx = trace.set_span_in_context(span)
            self._propagator.inject(carrier, context=ctx)
        return carrier


# =============================================================================
# Trace Context Propagation Helpers
# =============================================================================


def inject_trace_context(
    hooks: OpenTelemetryHooks, instance_id: str, event_data: dict[str, Any]
) -> dict[str, Any]:
    """
    Inject W3C Trace Context into event data for CloudEvents propagation.

    Use this before calling send_event_transactional() to propagate trace
    context across service boundaries.

    Example::

        from edda.integrations.opentelemetry import inject_trace_context
        from edda.outbox.transactional import send_event_transactional

        event_data = {"order_id": "ORD-123", "amount": 99.99}
        event_data = inject_trace_context(hooks, ctx.instance_id, event_data)
        await send_event_transactional(ctx, "payment.completed", "payment-service", event_data)

    Args:
        hooks: OpenTelemetryHooks instance
        instance_id: Workflow instance ID
        event_data: Event data dict to inject trace context into

    Returns:
        Updated event_data with _trace_context key
    """
    trace_context = hooks.get_trace_context(instance_id)
    if trace_context:
        event_data["_trace_context"] = trace_context
    return event_data


def extract_trace_context(event_data: dict[str, Any]) -> Context | None:
    """
    Extract W3C Trace Context from event data.

    This is called automatically by OpenTelemetryHooks.on_workflow_start(),
    but can also be used manually if needed.

    Args:
        event_data: Event data dict containing _trace_context

    Returns:
        OpenTelemetry Context or None if no trace context found
    """
    if not _OPENTELEMETRY_AVAILABLE:
        return None

    if "_trace_context" in event_data:
        propagator = TraceContextTextMapPropagator()
        return propagator.extract(event_data["_trace_context"])
    return None
