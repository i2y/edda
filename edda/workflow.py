"""
Workflow module for Edda framework.

This module provides the @workflow decorator for defining workflow functions
and managing workflow instances.
"""

import functools
import inspect
from collections.abc import Callable
from typing import Any, TypeVar, cast

from edda.pydantic_utils import to_json_dict

F = TypeVar("F", bound=Callable[..., Any])


class RecurException(Exception):
    """
    Exception raised to signal that a workflow should recur (restart with fresh history).

    This is similar to Erlang's tail recursion pattern - it prevents unbounded history
    growth in long-running loops by completing the current workflow instance and
    starting a new one with the provided arguments.

    The workflow's history is archived (not deleted) and a new instance is created
    with a reference to the previous instance (continued_from).

    Note:
        This exception should not be caught by user code. It is handled internally
        by the ReplayEngine.

    Example:
        >>> @workflow
        ... async def notification_service(ctx: WorkflowContext, processed_count: int = 0):
        ...     await join_group(ctx, group="order_watchers")
        ...
        ...     count = 0
        ...     while True:
        ...         msg = await wait_message(ctx, channel="order.completed")
        ...         await send_notification(ctx, msg.data, activity_id=f"notify:{msg.id}")
        ...
        ...         count += 1
        ...         if count >= 1000:
        ...             # Reset history by recurring with new state
        ...             await ctx.recur(processed_count=processed_count + count)
    """

    def __init__(self, kwargs: dict[str, Any]):
        """
        Initialize RecurException.

        Args:
            kwargs: Keyword arguments to pass to the new workflow instance
        """
        self.kwargs = kwargs
        super().__init__("Workflow recur requested")


# Global registry of workflow instances (will be set by EddaApp)
_replay_engine: Any = None

# Global registry of all @workflow decorated workflows
_workflow_registry: dict[str, "Workflow"] = {}


def set_replay_engine(engine: Any) -> None:
    """
    Set the global replay engine.

    This is called by EddaApp during initialization.

    Args:
        engine: ReplayEngine instance
    """
    global _replay_engine
    _replay_engine = engine


def get_all_workflows() -> dict[str, "Workflow"]:
    """
    Get all registered workflow definitions.

    Returns:
        Dictionary mapping workflow names to Workflow instances
    """
    return _workflow_registry.copy()


class Workflow:
    """
    Wrapper class for workflow functions.

    Provides methods for starting and managing workflow instances.
    """

    def __init__(
        self,
        func: Callable[..., Any],
        event_handler: bool = False,
        lock_timeout_seconds: int | None = None,
    ):
        """
        Initialize workflow wrapper.

        Args:
            func: The async function to wrap as a workflow
            event_handler: Whether to auto-register as CloudEvent handler
            lock_timeout_seconds: Default lock timeout for this workflow (None = global default 300s)
        """
        self.func = func
        self.name = func.__name__
        self.event_handler = event_handler
        self.lock_timeout_seconds = lock_timeout_seconds
        functools.update_wrapper(self, func)

        # Register in global workflow registry for auto-discovery
        _workflow_registry[self.name] = self

    async def start(self, lock_timeout_seconds: int | None = None, **kwargs: Any) -> str:
        """
        Start a new workflow instance.

        Pydantic models in kwargs are automatically converted to JSON-compatible dicts
        for storage. During execution, they will be restored back to Pydantic models
        based on the workflow function's type hints.

        Args:
            lock_timeout_seconds: Override lock timeout for this specific execution
                                (None = use decorator default or global default 300s)
            **kwargs: Input parameters for the workflow (can include Pydantic models)

        Returns:
            Instance ID of the started workflow

        Raises:
            RuntimeError: If replay engine not initialized
        """
        if _replay_engine is None:
            raise RuntimeError(
                "Replay engine not initialized. "
                "Ensure EddaApp is properly initialized before starting workflows."
            )

        # Convert Pydantic models and Enums to JSON-compatible values for storage
        processed_kwargs = {k: to_json_dict(v) for k, v in kwargs.items()}

        # Determine actual lock timeout (priority: runtime > decorator > global default)
        actual_timeout = lock_timeout_seconds or self.lock_timeout_seconds

        instance_id: str = await _replay_engine.start_workflow(
            workflow_name=self.name,
            workflow_func=self.func,
            input_data=processed_kwargs,
            lock_timeout_seconds=actual_timeout,
        )
        return instance_id

    async def resume(self, instance_id: str, event: Any = None) -> None:
        """
        Resume an existing workflow instance.

        Args:
            instance_id: Workflow instance ID
            event: Optional event that triggered the resume

        Raises:
            RuntimeError: If replay engine not initialized
        """
        if _replay_engine is None:
            raise RuntimeError(
                "Replay engine not initialized. "
                "Ensure EddaApp is properly initialized before resuming workflows."
            )

        await _replay_engine.resume_workflow(
            instance_id=instance_id, workflow_func=self.func, _event=event
        )

    async def __call__(self, *args: Any, **kwargs: Any) -> Any:
        """
        Direct call to the workflow function.

        This is typically used during replay by the replay engine.

        Args:
            *args: Positional arguments
            **kwargs: Keyword arguments

        Returns:
            Workflow result
        """
        return await self.func(*args, **kwargs)


def workflow(
    func: F | None = None,
    *,
    event_handler: bool = False,
    lock_timeout_seconds: int | None = None,
) -> F | Callable[[F], F]:
    """
    Decorator for defining workflows.

    Workflows are the top-level orchestration functions that coordinate
    multiple activities. They support deterministic replay and can wait for
    external events.

    By default, workflows are NOT automatically registered as CloudEvent handlers.
    Set event_handler=True to enable automatic CloudEvent handling.

    Example:
        >>> # Basic workflow (manual event handling)
        >>> @workflow
        ... async def order_workflow(ctx: WorkflowContext, order_id: str, amount: int):
        ...     inventory = await reserve_inventory(ctx, order_id)
        ...     payment = await process_payment(ctx, order_id, amount)
        ...     return {"status": "completed"}
        ...
        ... # Start the workflow manually
        ... instance_id = await order_workflow.start(order_id="123", amount=100)
        ...
        >>> # Workflow with automatic CloudEvent handling
        >>> @workflow(event_handler=True)
        ... async def auto_workflow(ctx: WorkflowContext, **kwargs):
        ...     # This will automatically handle CloudEvents with type="auto_workflow"
        ...     pass
        ...
        >>> # Workflow with custom lock timeout
        >>> @workflow(lock_timeout_seconds=600)
        ... async def long_running_workflow(ctx: WorkflowContext, **kwargs):
        ...     # This workflow will use a 10-minute lock timeout instead of the default 5 minutes
        ...     pass

    Args:
        func: Async function to wrap as a workflow
        event_handler: If True, automatically register as CloudEvent handler
        lock_timeout_seconds: Default lock timeout for this workflow (None = global default 300s)

    Returns:
        Decorated Workflow instance
    """

    def decorator(f: F) -> F:
        # Verify the function is async
        if not inspect.iscoroutinefunction(f):
            raise TypeError(f"Workflow {f.__name__} must be an async function")

        # Create the Workflow wrapper
        workflow_wrapper = Workflow(
            f, event_handler=event_handler, lock_timeout_seconds=lock_timeout_seconds
        )
        return cast(F, workflow_wrapper)

    # Support both @workflow and @workflow(...) patterns
    if func is None:
        # Called with parameters: @workflow(event_handler=True, lock_timeout_seconds=600)
        return decorator
    else:
        # Called without parameters: @workflow
        return decorator(func)
