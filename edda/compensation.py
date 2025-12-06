"""
Compensation (Saga compensation) module for Edda framework.

This module provides compensation transaction support for implementing
the Saga pattern with automatic rollback on failure.
"""

import logging
from collections.abc import Callable
from typing import TYPE_CHECKING, Any, TypeVar

logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    from edda.context import WorkflowContext

F = TypeVar("F", bound=Callable[..., Any])

# Global registry for compensation functions
_COMPENSATION_REGISTRY: dict[str, Callable[..., Any]] = {}


class CompensationAction:
    """
    Represents a compensation action that should be executed on rollback.

    Compensation actions are stored in LIFO order (stack) and executed
    in reverse order when a workflow fails.
    """

    def __init__(
        self,
        func: Callable[..., Any],
        args: tuple[Any, ...],
        kwargs: dict[str, Any],
        name: str,
    ):
        """
        Initialize a compensation action.

        Args:
            func: The compensation function to execute
            args: Positional arguments for the function
            kwargs: Keyword arguments for the function
            name: Human-readable name for this compensation
        """
        self.func = func
        self.args = args
        self.kwargs = kwargs
        self.name = name

    async def execute(self) -> None:
        """
        Execute the compensation action.

        Raises:
            Exception: Any exception raised by the compensation function
        """
        await self.func(*self.args, **self.kwargs)

    def __repr__(self) -> str:
        """String representation of the compensation action."""
        return f"CompensationAction(name={self.name!r})"


async def register_compensation(
    ctx: "WorkflowContext",
    compensation_func: Callable[..., Any],
    *args: Any,
    activity_id: str | None = None,
    **kwargs: Any,
) -> None:
    """
    Register a compensation action to be executed if the workflow fails.

    Compensation actions are stored in LIFO order (like a stack) and will be
    executed in reverse order during rollback.

    Args:
        ctx: Workflow context
        compensation_func: The async function to call for compensation
        *args: Positional arguments to pass to the compensation function
        activity_id: Activity ID to associate with this compensation (optional)
        **kwargs: Keyword arguments to pass to the compensation function

    Example:
        >>> @saga
        ... async def order_workflow(ctx: WorkflowContext, order_id: str) -> dict:
        ...     # Execute activity
        ...     result = await reserve_inventory(ctx, order_id, activity_id="reserve:1")
        ...
        ...     # Register compensation AFTER activity execution
        ...     await register_compensation(
        ...         ctx,
        ...         release_inventory,
        ...         activity_id="reserve:1",
        ...         reservation_id=result["reservation_id"]
        ...     )
        ...
        ...     return result
    """
    # Get function name for logging
    func_name = getattr(compensation_func, "__name__", str(compensation_func))

    # Register function in global registry for later execution
    _COMPENSATION_REGISTRY[func_name] = compensation_func

    action = CompensationAction(
        func=compensation_func,
        args=args,
        kwargs=kwargs,
        name=func_name,
    )

    # Generate activity_id if not provided
    if activity_id is None:
        activity_id = ctx._generate_activity_id(func_name)

    # Store in workflow's compensation stack
    await ctx._push_compensation(action, activity_id)


def compensation(func: F) -> F:
    """
    Decorator to register a compensation function in the global registry.

    This automatically registers the function when the module is imported,
    making it available for execution across all worker processes in a
    multi-process environment (e.g., tsuno, gunicorn).

    Usage:
        >>> @compensation
        ... async def cancel_reservation(ctx: WorkflowContext, reservation_id: str):
        ...     await cancel_api(reservation_id)

    Args:
        func: The compensation function to register

    Returns:
        The same function (unmodified)
    """
    func_name = getattr(func, "__name__", str(func))
    _COMPENSATION_REGISTRY[func_name] = func
    return func


def on_failure(compensation_func: Callable[..., Any]) -> Callable[[F], F]:
    """
    Decorator to automatically register a compensation function.

    This decorator wraps an activity and automatically registers a compensation
    action when the activity completes successfully.

    Args:
        compensation_func: The compensation function to register

    Returns:
        Decorator function

    Example:
        >>> @activity
        ... @on_failure(release_inventory)
        ... async def reserve_inventory(ctx: WorkflowContext, order_id: str) -> dict:
        ...     reservation_id = await make_reservation(order_id)
        ...     return {"reservation_id": reservation_id}
        ...
        ... @activity
        ... async def release_inventory(ctx: WorkflowContext, reservation_id: str) -> None:
        ...     await cancel_reservation(reservation_id)
    """

    def decorator(func: F) -> F:
        # Mark the function to indicate it has compensation
        func._compensation_func = compensation_func  # type: ignore[attr-defined]
        func._has_compensation = True  # type: ignore[attr-defined]
        return func

    return decorator


async def execute_compensations(ctx: "WorkflowContext") -> None:
    """
    Execute all registered compensation actions in LIFO order.

    This is called automatically when a workflow fails and needs to rollback.
    Compensations are executed in reverse order (LIFO/stack semantics).

    This function sets status to "compensating" before execution to enable
    crash recovery. The caller is responsible for setting the final status
    (failed/cancelled) after compensations complete.

    Args:
        ctx: Workflow context

    Raises:
        Exception: If any compensation fails (logged but not propagated)
    """
    # Get all compensations from storage
    compensations = await ctx._get_compensations()

    # If no compensations, nothing to do
    if not compensations:
        logger.debug("No compensations to execute for %s", ctx.instance_id)
        return

    # Mark as compensating BEFORE execution for crash recovery
    # This allows auto-resume to detect and restart incomplete compensation
    logger.debug("Starting compensation execution for %s", ctx.instance_id)
    await ctx._update_status("compensating", {"started_at": None})

    # Get already executed compensations to avoid duplicate execution
    history = await ctx.storage.get_history(ctx.instance_id)
    executed_compensation_ids = {
        event.get("event_data", {}).get("compensation_id")
        for event in history
        if event.get("event_type") == "CompensationExecuted"
    }

    # Execute in reverse order (LIFO - most recent first)
    for compensation_data in compensations:  # Already reversed by get_compensations()
        compensation_id = compensation_data.get("id")
        activity_name = compensation_data.get("activity_name")
        args_data = compensation_data.get("args", {})

        # Skip if already executed (idempotency)
        if compensation_id in executed_compensation_ids:
            logger.debug(
                "Skipping already executed compensation: %s (id=%s)",
                activity_name,
                compensation_id,
            )
            continue

        # Extract args and kwargs
        comp_args = args_data.get("args", [])
        comp_kwargs = args_data.get("kwargs", {})

        # Skip if activity_name is None or not a string
        if not isinstance(activity_name, str):
            logger.warning("Invalid activity_name: %s. Skipping.", activity_name)
            continue

        # Log compensation execution
        logger.info("Executing compensation: %s (id=%s)", activity_name, compensation_id)

        try:
            # Look up compensation function from registry
            compensation_func = _COMPENSATION_REGISTRY.get(activity_name)

            if compensation_func is None:
                logger.warning("Function '%s' not found in registry. Skipping.", activity_name)
                continue

            # Execute the compensation function directly
            # Compensation functions should NOT have @activity decorator
            await compensation_func(ctx, *comp_args, **comp_kwargs)

            # Record compensation execution in history (with idempotency)
            try:
                compensation_activity_id = f"compensation_{compensation_id}"
                await ctx.storage.append_history(
                    instance_id=ctx.instance_id,
                    activity_id=compensation_activity_id,
                    event_type="CompensationExecuted",
                    event_data={
                        "activity_name": activity_name,
                        "compensation_id": compensation_id,
                        "args": comp_args,
                        "kwargs": comp_kwargs,
                    },
                )
            except Exception as record_error:
                # UNIQUE constraint error means another process already recorded this compensation
                # This is expected in concurrent cancellation scenarios - silently ignore
                error_msg = str(record_error)
                if "UNIQUE constraint" in error_msg or "UNIQUE" in error_msg:
                    logger.debug(
                        "%s already recorded by another process, skipping duplicate record",
                        activity_name,
                    )
                else:
                    # Other errors should be logged but not break the compensation flow
                    logger.warning("Failed to record %s execution: %s", activity_name, record_error)

            logger.info("Successfully executed compensation: %s", activity_name)

        except Exception as error:
            # Log but don't fail the rollback
            logger.error(
                "Failed to execute compensation %s: %s", activity_name, error, exc_info=True
            )

            # Record compensation failure in history
            try:
                compensation_activity_id = f"compensation_{compensation_id}"
                await ctx.storage.append_history(
                    instance_id=ctx.instance_id,
                    activity_id=compensation_activity_id,
                    event_type="CompensationFailed",
                    event_data={
                        "activity_name": activity_name,
                        "compensation_id": compensation_id,
                        "error_type": type(error).__name__,
                        "error_message": str(error),
                    },
                )
            except Exception as record_error:
                # UNIQUE constraint error means another process already recorded this failure
                error_msg = str(record_error)
                if "UNIQUE constraint" in error_msg or "UNIQUE" in error_msg:
                    logger.debug("%s failure already recorded by another process", activity_name)
                else:
                    logger.warning("Failed to record compensation failure: %s", record_error)


async def clear_compensations(ctx: "WorkflowContext") -> None:
    """
    Clear all registered compensation actions.

    This is called when a workflow completes successfully and no longer
    needs the registered compensations.

    Args:
        ctx: Workflow context
    """
    await ctx._clear_compensations()
