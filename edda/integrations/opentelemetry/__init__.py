"""
Edda OpenTelemetry Integration.

Provides OpenTelemetry tracing and optional metrics for Edda workflows.

Example:
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

Installation:
    ```bash
    pip install edda-framework[opentelemetry]

    # Or using uv
    uv add edda-framework --extra opentelemetry
    ```
"""

from edda.integrations.opentelemetry.hooks import (
    OpenTelemetryHooks,
    extract_trace_context,
    inject_trace_context,
)

__all__ = ["OpenTelemetryHooks", "inject_trace_context", "extract_trace_context"]
