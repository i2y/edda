"""
Hook system for extending Edda with custom observability and monitoring.

This module provides a Protocol-based hook system that allows users to integrate
their own observability tools (Logfire, Datadog, Jaeger, etc.) without coupling
the framework to any specific tool.

Example:
    >>> from edda.hooks import WorkflowHooks
    >>>
    >>> class MyHooks(WorkflowHooks):
    ...     async def on_workflow_start(self, instance_id, workflow_name, input_data):
    ...         print(f"Workflow {workflow_name} started: {instance_id}")
    ...
    ...     async def on_activity_complete(self, instance_id, activity_id, activity_name, result, cache_hit):
    ...         print(f"Activity {activity_name} ({activity_id}) completed (cache_hit={cache_hit})")
    >>>
    >>> app = EddaApp(service_name="my-service", db_url="...", hooks=MyHooks())
"""

from __future__ import annotations

from abc import ABC
from typing import Any, Protocol


class WorkflowHooks(Protocol):
    """
    Protocol for workflow lifecycle hooks.

    Users can implement this protocol to add custom observability, logging,
    or monitoring to their workflows. All methods are optional - implement
    only the ones you need.

    The framework will check if a hook method exists before calling it, so
    partial implementations are fully supported.
    """

    async def on_workflow_start(
        self, instance_id: str, workflow_name: str, input_data: dict[str, Any]
    ) -> None:
        """
        Called when a workflow starts execution.

        Args:
            instance_id: Unique workflow instance ID
            workflow_name: Name of the workflow function
            input_data: Input parameters passed to the workflow
        """
        ...

    async def on_workflow_complete(self, instance_id: str, workflow_name: str, result: Any) -> None:
        """
        Called when a workflow completes successfully.

        Args:
            instance_id: Unique workflow instance ID
            workflow_name: Name of the workflow function
            result: Return value from the workflow
        """
        ...

    async def on_workflow_failed(
        self, instance_id: str, workflow_name: str, error: Exception
    ) -> None:
        """
        Called when a workflow fails with an exception.

        Args:
            instance_id: Unique workflow instance ID
            workflow_name: Name of the workflow function
            error: Exception that caused the failure
        """
        ...

    async def on_workflow_cancelled(self, instance_id: str, workflow_name: str) -> None:
        """
        Called when a workflow is cancelled.

        Args:
            instance_id: Unique workflow instance ID
            workflow_name: Name of the workflow function
        """
        ...

    async def on_activity_start(
        self,
        instance_id: str,
        activity_id: str,
        activity_name: str,
        is_replaying: bool,
    ) -> None:
        """
        Called before an activity executes.

        Args:
            instance_id: Unique workflow instance ID
            activity_id: Activity ID (e.g., "reserve_inventory:1")
            activity_name: Name of the activity function
            is_replaying: True if this is a replay (cached result)
        """
        ...

    async def on_activity_complete(
        self,
        instance_id: str,
        activity_id: str,
        activity_name: str,
        result: Any,
        cache_hit: bool,
    ) -> None:
        """
        Called after an activity completes successfully.

        Args:
            instance_id: Unique workflow instance ID
            activity_id: Activity ID (e.g., "reserve_inventory:1")
            activity_name: Name of the activity function
            result: Return value from the activity
            cache_hit: True if result was retrieved from cache (replay)
        """
        ...

    async def on_activity_failed(
        self,
        instance_id: str,
        activity_id: str,
        activity_name: str,
        error: Exception,
    ) -> None:
        """
        Called when an activity fails with an exception.

        Args:
            instance_id: Unique workflow instance ID
            activity_id: Activity ID (e.g., "reserve_inventory:1")
            activity_name: Name of the activity function
            error: Exception that caused the failure
        """
        ...

    async def on_activity_retry(
        self,
        instance_id: str,
        activity_id: str,
        activity_name: str,
        error: Exception,
        attempt: int,
        delay: float,
    ) -> None:
        """
        Called when an activity is about to be retried after a failure.

        This hook is called BEFORE the retry delay (asyncio.sleep), allowing
        observability tools to track retry attempts in real-time.

        Args:
            instance_id: Unique workflow instance ID
            activity_id: Activity ID (e.g., "my_activity:1")
            activity_name: Name of the activity function
            error: Exception that caused the failure
            attempt: Current attempt number (1-indexed, before retry)
            delay: Backoff delay in seconds before the next retry
        """
        ...

    async def on_event_sent(
        self,
        event_type: str,
        event_source: str,
        event_data: dict[str, Any],
    ) -> None:
        """
        Called when an event is sent (transactional outbox).

        Args:
            event_type: CloudEvents type
            event_source: CloudEvents source
            event_data: Event payload
        """
        ...

    async def on_event_received(
        self,
        instance_id: str,
        event_type: str,
        event_data: dict[str, Any],
    ) -> None:
        """
        Called when a workflow receives an awaited event.

        Args:
            instance_id: Unique workflow instance ID
            event_type: CloudEvents type
            event_data: Event payload
        """
        ...


# Base class for convenient partial implementations
class HooksBase(WorkflowHooks, ABC):
    """
    Abstract base class for WorkflowHooks implementations.

    This can be used as a base class for partial implementations,
    so you don't have to implement all methods.

    Example:
        >>> class MyHooks(HooksBase):
        ...     async def on_workflow_start(self, instance_id, workflow_name, input_data):
        ...         print(f"Workflow started: {workflow_name}")
        ...     # Other methods are no-ops (inherited from HooksBase)
    """

    async def on_workflow_start(
        self, instance_id: str, workflow_name: str, input_data: dict[str, Any]
    ) -> None:
        pass

    async def on_workflow_complete(self, instance_id: str, workflow_name: str, result: Any) -> None:
        pass

    async def on_workflow_failed(
        self, instance_id: str, workflow_name: str, error: Exception
    ) -> None:
        pass

    async def on_workflow_cancelled(self, instance_id: str, workflow_name: str) -> None:
        pass

    async def on_activity_start(
        self,
        instance_id: str,
        activity_id: str,
        activity_name: str,
        is_replaying: bool,
    ) -> None:
        pass

    async def on_activity_complete(
        self,
        instance_id: str,
        activity_id: str,
        activity_name: str,
        result: Any,
        cache_hit: bool,
    ) -> None:
        pass

    async def on_activity_failed(
        self,
        instance_id: str,
        activity_id: str,
        activity_name: str,
        error: Exception,
    ) -> None:
        pass

    async def on_activity_retry(
        self,
        instance_id: str,
        activity_id: str,
        activity_name: str,
        error: Exception,
        attempt: int,
        delay: float,
    ) -> None:
        pass

    async def on_event_sent(
        self,
        event_type: str,
        event_source: str,
        event_data: dict[str, Any],
    ) -> None:
        pass

    async def on_event_received(
        self,
        instance_id: str,
        event_type: str,
        event_data: dict[str, Any],
    ) -> None:
        pass
