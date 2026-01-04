"""DurableWorkflow - makes LlamaIndex Workflow execution durable via Edda.

This module provides integration between LlamaIndex Workflow and Edda's durable
execution framework, making workflow execution crash-recoverable and supporting
durable wait operations.
"""

from __future__ import annotations

import importlib
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, TypeVar

from edda.activity import activity
from edda.pydantic_utils import to_json_dict

from .events import DurableSleepEvent, DurableWaitEvent, ResumeEvent
from .exceptions import WorkflowExecutionError

if TYPE_CHECKING:
    from edda.context import WorkflowContext

T = TypeVar("T")


def _import_llamaindex_workflow() -> Any:
    """Import llama_index.core.workflow with helpful error message."""
    try:
        from llama_index.core import workflow  # type: ignore[import-not-found]

        return workflow
    except ImportError as e:
        msg = (
            "llama-index-core is not installed. Install with:\n"
            "  pip install llama-index-core\n"
            "or\n"
            "  pip install 'edda-framework[llamaindex]'"
        )
        raise ImportError(msg) from e


def _serialize_event(event: Any) -> dict[str, Any]:
    """Serialize a LlamaIndex Event to a dictionary."""
    if isinstance(event, DurableSleepEvent):
        return event.to_dict()
    if isinstance(event, DurableWaitEvent):
        return event.to_dict()
    if isinstance(event, ResumeEvent):
        return event.to_dict()

    # For LlamaIndex events, use model_dump if available (Pydantic)
    if hasattr(event, "model_dump"):
        data = event.model_dump()
    elif hasattr(event, "__dict__"):
        data = {k: v for k, v in event.__dict__.items() if not k.startswith("_")}
    else:
        data = {}

    return {
        "_type": f"{event.__class__.__module__}:{event.__class__.__qualname__}",
        "_data": to_json_dict(data),
    }


def _deserialize_event(data: dict[str, Any]) -> Any:
    """Deserialize a dictionary to a LlamaIndex Event."""
    event_type = data.get("_type", "")

    # Handle our special events
    if event_type == "DurableSleepEvent":
        return DurableSleepEvent.from_dict(data)
    if event_type == "DurableWaitEvent":
        return DurableWaitEvent.from_dict(data)
    if event_type == "ResumeEvent":
        return ResumeEvent.from_dict(data)

    # Handle LlamaIndex events
    if ":" in event_type:
        module_path, class_name = event_type.rsplit(":", 1)
        module = importlib.import_module(module_path)
        event_class = getattr(module, class_name)
        event_data = data.get("_data", {})

        # Use model_validate for Pydantic models
        if hasattr(event_class, "model_validate"):
            return event_class.model_validate(event_data)
        return event_class(**event_data)

    raise ValueError(f"Unknown event type: {event_type}")


@dataclass
class StepResult:
    """Result of a step execution."""

    event: Any
    step_name: str


@activity
async def _run_step(
    ctx: WorkflowContext,  # noqa: ARG001 - Used by @activity decorator
    workflow_class_path: str,
    step_name: str,
    event_data: dict[str, Any],
    context_data: dict[str, Any],
) -> dict[str, Any]:
    """
    Execute a single workflow step as a durable activity.

    This activity is the core of DurableWorkflow - it runs one step and returns
    the serialized result event.
    """
    # Import the workflow class
    module_path, class_name = workflow_class_path.rsplit(":", 1)
    module = importlib.import_module(module_path)
    workflow_class = getattr(module, class_name)

    # Create workflow instance
    workflow_instance = workflow_class()

    # Deserialize the input event
    input_event = _deserialize_event(event_data)

    # Get the step method
    step_method = getattr(workflow_instance, step_name, None)
    if step_method is None:
        raise WorkflowExecutionError(f"Step '{step_name}' not found", step_name)

    # Create a minimal context for the step
    # Note: LlamaIndex Context is created per-run, we create a simple mock

    # Execute the step
    try:
        # The step method signature is: async def step(self, ctx, event) -> Event
        # We need to provide a context - use a simple object with store
        @dataclass
        class SimpleContext:
            store: dict[str, Any]

        simple_ctx = SimpleContext(store=context_data.get("store", {}))
        result_event = await step_method(simple_ctx, input_event)
    except Exception as e:
        raise WorkflowExecutionError(f"Step '{step_name}' failed: {e}", step_name) from e

    # Serialize the result
    return {
        "event": _serialize_event(result_event),
        "context_store": simple_ctx.store,
    }


class DurableWorkflowRunner:
    """
    Runner that executes a LlamaIndex Workflow with Edda durability.

    This class wraps a LlamaIndex Workflow and executes it step-by-step,
    recording each step as an Edda Activity for crash recovery.

    Example:
        from llama_index.core.workflow import Workflow, step, StartEvent, StopEvent

        class MyWorkflow(Workflow):
            @step
            async def process(self, ctx: Context, ev: StartEvent) -> StopEvent:
                return StopEvent(result="done")

        runner = DurableWorkflowRunner(MyWorkflow)

        @workflow
        async def my_edda_workflow(ctx: WorkflowContext) -> str:
            result = await runner.run(ctx, input_data="hello")
            return result
    """

    def __init__(self, workflow_class: type) -> None:
        """
        Initialize DurableWorkflowRunner.

        Args:
            workflow_class: A LlamaIndex Workflow class (not instance)
        """
        self._workflow_class = workflow_class
        self._class_path = f"{workflow_class.__module__}:{workflow_class.__qualname__}"

        # Validate it's a Workflow subclass
        llamaindex_workflow = _import_llamaindex_workflow()
        if not issubclass(workflow_class, llamaindex_workflow.Workflow):
            raise TypeError(f"Expected a Workflow subclass, got {type(workflow_class).__name__}")

    async def run(
        self,
        ctx: WorkflowContext,
        **kwargs: Any,
    ) -> Any:
        """
        Execute the workflow durably with Edda crash recovery.

        Args:
            ctx: Edda WorkflowContext
            **kwargs: Arguments passed to StartEvent

        Returns:
            The result from StopEvent
        """
        from edda.channels import sleep as edda_sleep
        from edda.channels import wait_event as edda_wait_event

        llamaindex_workflow = _import_llamaindex_workflow()

        # Create a workflow instance to analyze its steps
        workflow_instance = self._workflow_class()

        # Build step registry from the workflow
        step_registry = self._build_step_registry(workflow_instance)

        # Start with StartEvent
        start_event_class = llamaindex_workflow.StartEvent
        current_event = start_event_class(**kwargs)
        context_store: dict[str, Any] = {}

        # Main execution loop
        while True:
            # Find the step that handles this event type
            event_type = type(current_event)
            step_name = self._find_step_for_event(step_registry, event_type)

            if step_name is None:
                # No step found - check if it's a stop event
                if isinstance(current_event, llamaindex_workflow.StopEvent):
                    return current_event.result
                raise WorkflowExecutionError(f"No step found for event type: {event_type.__name__}")

            # Execute the step as an activity
            result = await _run_step(  # type: ignore[misc,call-arg]
                ctx,  # type: ignore[arg-type]
                self._class_path,
                step_name,
                _serialize_event(current_event),
                {"store": context_store},
            )

            # Update context store
            context_store = result.get("context_store", {})

            # Deserialize the result event
            result_event = _deserialize_event(result["event"])

            # Handle special durable events
            if isinstance(result_event, DurableSleepEvent):
                # Durable sleep
                await edda_sleep(ctx, int(result_event.seconds))
                # Resume with ResumeEvent containing the sleep's resume_data
                current_event = ResumeEvent(data=result_event.resume_data)

            elif isinstance(result_event, DurableWaitEvent):
                # Durable wait for external event
                received = await edda_wait_event(
                    ctx,
                    result_event.event_type,
                    timeout_seconds=(
                        int(result_event.timeout_seconds) if result_event.timeout_seconds else None
                    ),
                )
                # Resume with ResumeEvent containing the received data
                current_event = ResumeEvent(data=received.data if hasattr(received, "data") else {})

            elif isinstance(result_event, llamaindex_workflow.StopEvent):
                # Workflow completed
                return result_event.result

            else:
                # Normal event transition
                current_event = result_event

    def _build_step_registry(self, workflow_instance: Any) -> dict[str, list[type]]:
        """Build a registry mapping step names to their input event types."""
        registry: dict[str, list[type]] = {}

        # Look for methods decorated with @step in the class
        workflow_class = type(workflow_instance)
        for name in dir(workflow_class):
            if name.startswith("_"):
                continue

            # Get the raw function from the class (not bound method)
            method = getattr(workflow_class, name, None)
            if method is None or not callable(method):
                continue

            # Check if it's a step (has _step_config attribute from @step decorator)
            if hasattr(method, "_step_config"):
                step_config = method._step_config
                # Get accepted event types directly from step config
                if hasattr(step_config, "accepted_events"):
                    registry[name] = list(step_config.accepted_events)

        return registry

    def _find_step_for_event(self, registry: dict[str, list[type]], event_type: type) -> str | None:
        """Find the step that handles the given event type."""
        for step_name, accepted_types in registry.items():
            for accepted_type in accepted_types:
                if issubclass(event_type, accepted_type):
                    return step_name
        return None
