"""
Replay engine for Edda framework.

This module implements the deterministic replay mechanism with activity result caching,
allowing workflows to resume from where they left off by replaying their
execution history.
"""

import hashlib
import inspect
import logging
import uuid
from collections.abc import Callable
from typing import Any

from edda.channels import WaitForChannelMessageException, WaitForTimerException
from edda.compensation import execute_compensations
from edda.context import WorkflowContext
from edda.locking import workflow_lock
from edda.pydantic_utils import (
    enum_value_to_enum,
    extract_enum_from_annotation,
    extract_pydantic_model_from_annotation,
    from_json_dict,
    to_json_dict,
)
from edda.storage.protocol import StorageProtocol
from edda.workflow import RecurException

logger = logging.getLogger(__name__)


class ReplayEngine:
    """
    Engine for executing and replaying workflows with deterministic behavior.

    The replay engine orchestrates workflow execution, handles lock acquisition,
    loads history for replay, and manages workflow lifecycle.
    """

    def __init__(
        self,
        storage: StorageProtocol,
        service_name: str,
        worker_id: str,
        hooks: Any = None,
        default_retry_policy: Any = None,
    ):
        """
        Initialize the replay engine.

        Args:
            storage: Storage backend
            service_name: Name of the service (e.g., "order-service")
            worker_id: Unique worker ID for this process
            hooks: Optional WorkflowHooks implementation for observability
            default_retry_policy: Default retry policy for all activities (RetryPolicy or None)
        """
        self.storage = storage
        self.service_name = service_name
        self.worker_id = worker_id
        self.hooks = hooks
        self.default_retry_policy = default_retry_policy

    def _prepare_workflow_input(
        self,
        workflow_func: Callable[..., Any],
        input_data: dict[str, Any],
    ) -> dict[str, Any]:
        """
        Prepare workflow input by converting JSON values to Pydantic models and Enums based on type hints.

        Supports:
        - Pydantic models: User
        - Enums: OrderStatus
        - List of Pydantic models: list[OrderItem]

        Args:
            workflow_func: The workflow function
            input_data: Input data from storage (JSON-compatible dicts)

        Returns:
            Processed input data with Pydantic models and Enums restored
        """
        from typing import get_args, get_origin

        sig = inspect.signature(workflow_func)
        processed_input: dict[str, Any] = {}

        # Detect single Pydantic model parameter pattern
        # Count non-ctx parameters
        workflow_params = [
            (name, param) for name, param in sig.parameters.items() if name not in ("ctx", "self")
        ]

        # If there's only one parameter and it's a Pydantic model,
        # and input_data doesn't have that parameter name as a key,
        # assume input_data IS the model data (CloudEvents case)
        if len(workflow_params) == 1:
            param_name, param = workflow_params[0]
            model = extract_pydantic_model_from_annotation(param.annotation)
            if model is not None and param_name not in input_data:
                # input_data = {"order_id": "...", "amount": ...}
                # → processed_input = {"input": PaymentWorkflowInput(...)}
                processed_input[param_name] = from_json_dict(input_data, model)
                return processed_input

        for param_name, param in sig.parameters.items():
            # Skip 'ctx' parameter (WorkflowContext)
            if param_name == "ctx" or param_name == "self":
                continue

            if param_name not in input_data:
                # Parameter not provided in input_data (may have default value)
                continue

            value = input_data[param_name]

            # Check if parameter has Pydantic model type hint
            model = extract_pydantic_model_from_annotation(param.annotation)
            if model is not None and isinstance(value, dict):
                # Restore Pydantic model from JSON dict
                value = from_json_dict(value, model)
            # Check if parameter has Enum type hint
            elif (enum_class := extract_enum_from_annotation(param.annotation)) is not None:
                # Only convert if not already an Enum instance (defensive programming)
                from enum import Enum

                if not isinstance(value, Enum):
                    value = enum_value_to_enum(value, enum_class)
            # Check if parameter is list[PydanticModel]
            elif get_origin(param.annotation) is list:
                args = get_args(param.annotation)
                if args and len(args) == 1:
                    # Check if list element is Pydantic model
                    element_model = extract_pydantic_model_from_annotation(args[0])
                    if element_model is not None and isinstance(value, list):
                        # Convert each dict to Pydantic model
                        value = [
                            from_json_dict(item, element_model) if isinstance(item, dict) else item
                            for item in value
                        ]

            processed_input[param_name] = value

        return processed_input

    async def start_workflow(
        self,
        workflow_name: str,
        workflow_func: Callable[..., Any],
        input_data: dict[str, Any],
        lock_timeout_seconds: int | None = None,
    ) -> str:
        """
        Start a new workflow instance.

        Args:
            workflow_name: Name of the workflow
            workflow_func: The workflow function to execute
            input_data: Input parameters for the workflow
            lock_timeout_seconds: Lock timeout for this workflow (None = global default 300s)

        Returns:
            Instance ID of the started workflow
        """
        # Generate instance ID
        instance_id = f"{workflow_name}-{uuid.uuid4().hex}"

        # Extract source code for visualization
        try:
            source_code = inspect.getsource(workflow_func)
        except (OSError, TypeError) as e:
            # inspect.getsource can fail for lambdas, built-ins, REPL functions, etc.
            logger.warning(
                f"Could not extract source code for workflow '{workflow_name}': {e}. "
                "Hybrid diagram visualization will not be available."
            )
            source_code = f"# Source code not available\n# Workflow: {workflow_name}\n# Error: {e}"

        # Calculate source code hash
        source_hash = hashlib.sha256(source_code.encode("utf-8")).hexdigest()

        # Store workflow definition (idempotent)
        await self.storage.upsert_workflow_definition(
            workflow_name=workflow_name,
            source_hash=source_hash,
            source_code=source_code,
        )

        # Create workflow instance in storage
        await self.storage.create_instance(
            instance_id=instance_id,
            workflow_name=workflow_name,
            source_hash=source_hash,
            owner_service=self.service_name,
            input_data=input_data,
            lock_timeout_seconds=lock_timeout_seconds,
        )

        # Execute the workflow with distributed lock
        async with workflow_lock(self.storage, instance_id, self.worker_id):
            # Create context for new execution
            ctx = WorkflowContext(
                instance_id=instance_id,
                workflow_name=workflow_name,
                storage=self.storage,
                worker_id=self.worker_id,
                is_replaying=False,
                hooks=self.hooks,
            )
            # Set default retry policy for activity resolution
            ctx._app_retry_policy = self.default_retry_policy

            try:
                # Call hook: workflow start
                if self.hooks and hasattr(self.hooks, "on_workflow_start"):
                    await self.hooks.on_workflow_start(instance_id, workflow_name, input_data)

                # Prepare input: convert JSON dicts to Pydantic models based on type hints
                processed_input = self._prepare_workflow_input(workflow_func, input_data)

                # Execute workflow function
                result = await workflow_func(ctx, **processed_input)

                # Before marking as completed, check if workflow was cancelled
                instance = await ctx.storage.get_instance(instance_id)
                if instance and instance.get("status") == "cancelled":
                    from edda.exceptions import WorkflowCancelledException

                    raise WorkflowCancelledException(
                        f"Workflow {instance_id} was cancelled"
                    ) from None

                # Convert Pydantic model result to JSON dict for storage
                result_dict = to_json_dict(result)

                # Mark as completed
                await ctx._update_status("completed", {"result": result_dict})

                # Call hook: workflow complete
                if self.hooks and hasattr(self.hooks, "on_workflow_complete"):
                    await self.hooks.on_workflow_complete(instance_id, workflow_name, result)

                return instance_id

            except WaitForTimerException as exc:
                # Workflow is waiting for a timer
                # Before marking as waiting_for_timer, check if workflow was cancelled
                instance = await ctx.storage.get_instance(instance_id)
                if instance and instance.get("status") == "cancelled":
                    from edda.exceptions import WorkflowCancelledException

                    raise WorkflowCancelledException(
                        f"Workflow {instance_id} was cancelled"
                    ) from None

                # Atomically register timer subscription and release lock (distributed coroutines)
                # This ensures subscription is registered and lock is released in a single transaction
                # so ANY worker can resume the workflow when the timer expires
                # Use the expires_at from the exception (calculated at wait_timer() call time)
                # This ensures deterministic replay: the timer expiration time never changes
                await self.storage.register_timer_subscription_and_release_lock(
                    instance_id=instance_id,
                    worker_id=self.worker_id,
                    timer_id=exc.timer_id,
                    expires_at=exc.expires_at,
                    activity_id=exc.activity_id,
                )

                # Status is updated to 'waiting_for_timer' atomically
                # by register_timer_subscription_and_release_lock()
                return instance_id

            except WaitForChannelMessageException as exc:
                # Workflow is waiting for a message on a channel
                # Before marking as waiting_for_message, check if workflow was cancelled
                instance = await ctx.storage.get_instance(instance_id)
                if instance and instance.get("status") == "cancelled":
                    from edda.exceptions import WorkflowCancelledException

                    raise WorkflowCancelledException(
                        f"Workflow {instance_id} was cancelled"
                    ) from None

                # Atomically register channel receive and release lock (distributed coroutines)
                # This ensures subscription is registered and lock is released in a single transaction
                # so ANY worker can resume the workflow when the message arrives
                await self.storage.register_channel_receive_and_release_lock(
                    instance_id=instance_id,
                    worker_id=self.worker_id,
                    channel=exc.channel,
                    activity_id=exc.activity_id,
                    timeout_seconds=exc.timeout_seconds,
                )

                # Status is updated to 'waiting_for_message' atomically
                # by register_channel_receive_and_release_lock()
                return instance_id

            except RecurException as exc:
                # Workflow is recurring (Erlang-style tail recursion pattern)
                # This resets history growth in long-running loops by:
                # 1. Completing the current instance (marking as "recurred")
                # 2. Archiving the current history
                # 3. Cleaning up subscriptions
                # 4. Starting a new instance with the provided arguments
                # 5. Linking new instance to old via `continued_from`

                logger.info(f"Workflow {instance_id} recurring with args: {exc.kwargs}")

                # Mark current workflow as "recurred"
                await ctx._update_status("recurred", {"recur_kwargs": exc.kwargs})

                # Archive history (move to archive table)
                archived_count = await self.storage.archive_history(instance_id)
                logger.info(f"Archived {archived_count} history entries for {instance_id}")

                # Clean up all subscriptions (event/timer/message)
                # This prevents old subscriptions from receiving events meant for the new instance
                await self.storage.cleanup_instance_subscriptions(instance_id)

                # Clear compensations (fresh start)
                await ctx._clear_compensations()

                # Create and start a new workflow instance
                new_instance_id = await self._start_recurred_workflow(
                    workflow_name=workflow_name,
                    workflow_func=workflow_func,
                    input_data=exc.kwargs,
                    continued_from=instance_id,
                    lock_timeout_seconds=lock_timeout_seconds,
                )

                logger.info(f"Workflow {instance_id} recurred to {new_instance_id}")
                return new_instance_id

            except Exception as error:
                # Check if this is a cancellation exception
                from edda.exceptions import WorkflowCancelledException

                if isinstance(error, WorkflowCancelledException):
                    # Workflow was cancelled during execution
                    logger.info("Workflow %s was cancelled during execution", instance_id)

                    # Execute compensations (idempotent - already executed ones will be skipped)
                    # This ensures all compensations are executed, even if some were already
                    # executed by cancel_workflow() in a concurrent process
                    logger.debug("Executing compensations for %s", instance_id)
                    await execute_compensations(ctx)

                    # Ensure status is "cancelled"
                    await ctx._update_status("cancelled", {"reason": "Workflow cancelled by user"})

                    # Call hook: workflow cancelled
                    if self.hooks and hasattr(self.hooks, "on_workflow_cancelled"):
                        await self.hooks.on_workflow_cancelled(instance_id, workflow_name)

                    return instance_id

                # Execute compensations before marking as failed
                await execute_compensations(ctx)

                # Capture error details for debugging
                import traceback

                stack_trace = "".join(
                    traceback.format_exception(type(error), error, error.__traceback__)
                )

                error_data = {
                    "error_message": str(error),
                    "error_type": type(error).__name__,
                    "stack_trace": stack_trace,
                }

                await self.storage.append_history(
                    instance_id=instance_id,
                    activity_id="workflow_failed",
                    event_type="WorkflowFailed",
                    event_data=error_data,
                )

                # Mark as failed with detailed error information
                await ctx._update_status("failed", error_data)

                # Call hook: workflow failed
                if self.hooks and hasattr(self.hooks, "on_workflow_failed"):
                    await self.hooks.on_workflow_failed(instance_id, workflow_name, error)

                raise

    async def resume_workflow(
        self,
        instance_id: str,
        workflow_func: Callable[..., Any],
        _event: Any = None,
        already_locked: bool = False,
    ) -> None:
        """
        Resume a workflow instance (with replay).

        This method performs deterministic replay of the workflow execution
        up to the point where it was paused, then continues execution.

        Args:
            instance_id: Workflow instance ID
            workflow_func: The workflow function to replay/execute
            event: Optional event that triggered the resume (for wait_event)
            already_locked: If True, assumes the lock is already held by the caller
                           (used in distributed coroutine event delivery)

        Raises:
            ValueError: If instance not found or already completed
        """
        # Get instance metadata
        instance = await self.storage.get_instance(instance_id)
        if instance is None:
            raise ValueError(f"Workflow instance {instance_id} not found")

        if instance["status"] == "completed":
            # Already completed, nothing to do
            return

        if instance["status"] == "failed":
            # Cannot resume failed workflow
            raise ValueError(f"Cannot resume failed workflow {instance_id}")

        # Execute the workflow logic with or without lock acquisition
        if already_locked:
            # Lock already held by caller (distributed coroutine pattern)
            await self._execute_workflow_logic(instance, instance_id, workflow_func)
        else:
            # Acquire lock for this workflow
            async with workflow_lock(self.storage, instance_id, self.worker_id):
                await self._execute_workflow_logic(instance, instance_id, workflow_func)

    async def _execute_workflow_logic(
        self,
        instance: dict[str, Any],
        instance_id: str,
        workflow_func: Callable[..., Any],
    ) -> None:
        """
        Execute workflow logic (factored out to support both locked and unlocked execution).

        Args:
            instance: Workflow instance metadata
            instance_id: Workflow instance ID
            workflow_func: The workflow function to execute
        """
        # Create context for replay
        ctx = WorkflowContext(
            instance_id=instance_id,
            workflow_name=instance["workflow_name"],
            storage=self.storage,
            worker_id=self.worker_id,
            is_replaying=True,
            hooks=self.hooks,
        )
        # Set default retry policy for activity resolution
        ctx._app_retry_policy = self.default_retry_policy

        # Load history for replay
        await ctx._load_history()

        try:
            # Replay and continue execution
            input_data = instance["input_data"]

            # Prepare input: convert JSON dicts to Pydantic models based on type hints
            processed_input = self._prepare_workflow_input(workflow_func, input_data)

            result = await workflow_func(ctx, **processed_input)

            # Before marking as completed, check if workflow was cancelled
            instance_check = await ctx.storage.get_instance(instance_id)
            if instance_check and instance_check.get("status") == "cancelled":
                from edda.exceptions import WorkflowCancelledException

                raise WorkflowCancelledException(f"Workflow {instance_id} was cancelled")

            # Convert Pydantic model result to JSON dict for storage
            result_dict = to_json_dict(result)

            # Mark as completed
            await ctx._update_status("completed", {"result": result_dict})

        except WaitForTimerException as exc:
            # Workflow is waiting for a timer (again)
            # Atomically register timer subscription and release lock (distributed coroutines)
            # Use the expires_at from the exception (calculated at wait_timer() call time)
            # This ensures deterministic replay: the timer expiration time never changes
            await self.storage.register_timer_subscription_and_release_lock(
                instance_id=instance_id,
                worker_id=self.worker_id,
                timer_id=exc.timer_id,
                expires_at=exc.expires_at,
                activity_id=exc.activity_id,
            )

            # Status is updated to 'waiting_for_timer' atomically
            # by register_timer_subscription_and_release_lock()

        except WaitForChannelMessageException as exc:
            # Workflow is waiting for a message on a channel (again)
            # Atomically register channel receive and release lock (distributed coroutines)
            await self.storage.register_channel_receive_and_release_lock(
                instance_id=instance_id,
                worker_id=self.worker_id,
                channel=exc.channel,
                activity_id=exc.activity_id,
                timeout_seconds=exc.timeout_seconds,
            )

            # Status is updated to 'waiting_for_message' atomically
            # by register_channel_receive_and_release_lock()

        except RecurException as exc:
            # Workflow is recurring (Erlang-style tail recursion pattern)
            # This resets history growth in long-running loops by:
            # 1. Completing the current instance (marking as "recurred")
            # 2. Archiving the current history
            # 3. Cleaning up subscriptions
            # 4. Starting a new instance with the provided arguments
            # 5. Linking new instance to old via `continued_from`

            logger.info(f"Workflow {instance_id} recurring with args: {exc.kwargs}")

            # Mark current workflow as "recurred"
            await ctx._update_status("recurred", {"recur_kwargs": exc.kwargs})

            # Archive history (move to archive table)
            archived_count = await self.storage.archive_history(instance_id)
            logger.info(f"Archived {archived_count} history entries for {instance_id}")

            # Clean up all subscriptions (event/timer/message)
            # This prevents old subscriptions from receiving events meant for the new instance
            await self.storage.cleanup_instance_subscriptions(instance_id)

            # Clear compensations (fresh start)
            await ctx._clear_compensations()

            # Create and start a new workflow instance
            # Note: we don't return the new instance_id here since _execute_workflow_logic returns None
            # The new workflow will execute in its own context
            await self._start_recurred_workflow(
                workflow_name=instance["workflow_name"],
                workflow_func=workflow_func,
                input_data=exc.kwargs,
                continued_from=instance_id,
                lock_timeout_seconds=instance.get("lock_timeout_seconds"),
            )

            logger.info(f"Workflow {instance_id} recurred successfully")

        except Exception as error:
            # Check if this is a cancellation exception
            from edda.exceptions import WorkflowCancelledException

            if isinstance(error, WorkflowCancelledException):
                # Workflow was cancelled during execution
                logger.info("Workflow %s was cancelled during execution", instance_id)

                # Execute compensations (idempotent - already executed ones will be skipped)
                # This ensures all compensations are executed, even if some were already
                # executed by cancel_workflow() in a concurrent process
                logger.debug("Executing compensations for %s", instance_id)
                await execute_compensations(ctx)

                # Ensure status is "cancelled"
                await ctx._update_status("cancelled", {"reason": "Workflow cancelled by user"})
                return

            # Execute compensations before marking as failed
            await execute_compensations(ctx)

            # Capture error details for debugging
            import traceback

            stack_trace = "".join(
                traceback.format_exception(type(error), error, error.__traceback__)
            )

            error_data = {
                "error_message": str(error),
                "error_type": type(error).__name__,
                "stack_trace": stack_trace,
            }

            await self.storage.append_history(
                instance_id=instance_id,
                activity_id="workflow_failed",
                event_type="WorkflowFailed",
                event_data=error_data,
            )

            # Mark as failed with detailed error information
            await ctx._update_status("failed", error_data)
            raise

    async def resume_by_name(
        self, instance_id: str, workflow_name: str, already_locked: bool = False
    ) -> None:
        """
        Resume a workflow by its name (convenience method for auto-recovery).

        This method looks up the workflow function from the global saga registry
        and resumes execution. This is primarily used by the auto-recovery mechanism
        after Stale Lock cleanup.

        Args:
            instance_id: Workflow instance ID
            workflow_name: Name of the workflow to resume
            already_locked: If True, assumes the lock is already held by the caller
                           (used in distributed coroutine event delivery)

        Raises:
            ValueError: If workflow not found in registry or instance not found
        """
        # Import here to avoid circular dependency
        from edda.workflow import get_all_workflows

        # Look up workflow in workflow registry
        workflows = get_all_workflows()
        workflow_obj = workflows.get(workflow_name)

        if workflow_obj is None:
            raise ValueError(
                f"Workflow '{workflow_name}' not found in workflow registry. "
                f"Available workflows: {list(workflows.keys())}"
            )

        # Resume using the workflow function from the workflow
        await self.resume_workflow(
            instance_id=instance_id, workflow_func=workflow_obj.func, already_locked=already_locked
        )

    async def _start_recurred_workflow(
        self,
        workflow_name: str,
        workflow_func: Callable[..., Any],
        input_data: dict[str, Any],
        continued_from: str,
        lock_timeout_seconds: int | None = None,
    ) -> str:
        """
        Start a new workflow instance as a recurrence of an existing workflow.

        This is an internal helper method used by the RecurException handler.
        It creates a new workflow instance linked to the previous one via continued_from.

        Args:
            workflow_name: Name of the workflow
            workflow_func: The workflow function to execute
            input_data: Input parameters for the workflow (from recur() kwargs)
            continued_from: Instance ID of the workflow that is recurring
            lock_timeout_seconds: Lock timeout for this workflow (None = global default 300s)

        Returns:
            Instance ID of the new workflow
        """
        # Generate new instance ID
        new_instance_id = f"{workflow_name}-{uuid.uuid4().hex}"

        # Extract source code for visualization
        try:
            source_code = inspect.getsource(workflow_func)
        except (OSError, TypeError) as e:
            logger.warning(
                f"Could not extract source code for workflow '{workflow_name}': {e}. "
                "Hybrid diagram visualization will not be available."
            )
            source_code = f"# Source code not available\n# Workflow: {workflow_name}\n# Error: {e}"

        # Calculate source code hash
        source_hash = hashlib.sha256(source_code.encode("utf-8")).hexdigest()

        # Store workflow definition (idempotent)
        await self.storage.upsert_workflow_definition(
            workflow_name=workflow_name,
            source_hash=source_hash,
            source_code=source_code,
        )

        # Create workflow instance in storage with continued_from reference
        await self.storage.create_instance(
            instance_id=new_instance_id,
            workflow_name=workflow_name,
            source_hash=source_hash,
            owner_service=self.service_name,
            input_data=input_data,
            lock_timeout_seconds=lock_timeout_seconds,
            continued_from=continued_from,
        )

        # Execute the new workflow with distributed lock
        async with workflow_lock(self.storage, new_instance_id, self.worker_id):
            # Create context for new execution
            ctx = WorkflowContext(
                instance_id=new_instance_id,
                workflow_name=workflow_name,
                storage=self.storage,
                worker_id=self.worker_id,
                is_replaying=False,
                hooks=self.hooks,
            )
            # Set default retry policy for activity resolution
            ctx._app_retry_policy = self.default_retry_policy

            try:
                # Call hook: workflow start
                if self.hooks and hasattr(self.hooks, "on_workflow_start"):
                    await self.hooks.on_workflow_start(new_instance_id, workflow_name, input_data)

                # Prepare input: convert JSON dicts to Pydantic models based on type hints
                processed_input = self._prepare_workflow_input(workflow_func, input_data)

                # Execute workflow function
                result = await workflow_func(ctx, **processed_input)

                # Convert Pydantic model result to JSON dict for storage
                result_dict = to_json_dict(result)

                # Mark as completed
                await ctx._update_status("completed", {"result": result_dict})

                # Call hook: workflow complete
                if self.hooks and hasattr(self.hooks, "on_workflow_complete"):
                    await self.hooks.on_workflow_complete(new_instance_id, workflow_name, result)

                return new_instance_id

            except WaitForTimerException as exc:
                # Workflow is waiting for a timer
                await self.storage.register_timer_subscription_and_release_lock(
                    instance_id=new_instance_id,
                    worker_id=self.worker_id,
                    timer_id=exc.timer_id,
                    expires_at=exc.expires_at,
                    activity_id=exc.activity_id,
                )
                return new_instance_id

            except WaitForChannelMessageException as exc:
                # Workflow is waiting for a message
                await self.storage.register_channel_receive_and_release_lock(
                    instance_id=new_instance_id,
                    worker_id=self.worker_id,
                    channel=exc.channel,
                    activity_id=exc.activity_id,
                    timeout_seconds=exc.timeout_seconds,
                )
                return new_instance_id

            except RecurException as exc:
                # Recur again immediately (nested recur)
                logger.info(
                    f"Workflow {new_instance_id} recurring immediately with args: {exc.kwargs}"
                )

                await ctx._update_status("recurred", {"recur_kwargs": exc.kwargs})
                archived_count = await self.storage.archive_history(new_instance_id)
                logger.info(f"Archived {archived_count} history entries for {new_instance_id}")

                # Clean up all subscriptions (event/timer/message)
                await self.storage.cleanup_instance_subscriptions(new_instance_id)

                await ctx._clear_compensations()

                # Recursively start another recurred workflow
                return await self._start_recurred_workflow(
                    workflow_name=workflow_name,
                    workflow_func=workflow_func,
                    input_data=exc.kwargs,
                    continued_from=new_instance_id,
                    lock_timeout_seconds=lock_timeout_seconds,
                )

            except Exception as error:
                # Execute compensations before marking as failed
                await execute_compensations(ctx)

                import traceback

                stack_trace = "".join(
                    traceback.format_exception(type(error), error, error.__traceback__)
                )

                error_data = {
                    "error_message": str(error),
                    "error_type": type(error).__name__,
                    "stack_trace": stack_trace,
                }

                await self.storage.append_history(
                    instance_id=new_instance_id,
                    activity_id="workflow_failed",
                    event_type="WorkflowFailed",
                    event_data=error_data,
                )

                await ctx._update_status("failed", error_data)

                if self.hooks and hasattr(self.hooks, "on_workflow_failed"):
                    await self.hooks.on_workflow_failed(new_instance_id, workflow_name, error)

                raise

    async def execute_with_lock(
        self,
        instance_id: str,
        workflow_func: Callable[..., Any],
        is_replay: bool = False,
    ) -> Any:
        """
        Execute workflow function with distributed lock.

        This is a lower-level method used by start_workflow and resume_workflow.

        Args:
            instance_id: Workflow instance ID
            workflow_func: The workflow function to execute
            is_replay: Whether this is a replay execution

        Returns:
            Workflow result
        """
        # Get instance
        instance = await self.storage.get_instance(instance_id)
        if instance is None:
            raise ValueError(f"Workflow instance {instance_id} not found")

        # Acquire lock
        async with workflow_lock(self.storage, instance_id, self.worker_id):
            # Create context
            ctx = WorkflowContext(
                instance_id=instance_id,
                workflow_name=instance["workflow_name"],
                storage=self.storage,
                worker_id=self.worker_id,
                is_replaying=is_replay,
                hooks=self.hooks,
            )
            # Set default retry policy for activity resolution
            ctx._app_retry_policy = self.default_retry_policy

            # Load history if replaying
            if is_replay:
                await ctx._load_history()

            # Execute workflow
            input_data = instance["input_data"]
            return await workflow_func(ctx, **input_data)

    async def cancel_workflow(self, instance_id: str, cancelled_by: str = "user") -> bool:
        """
        Cancel a running or waiting workflow.

        This method will:
        1. Verify the workflow is cancellable (not already completed/failed)
        2. Try to acquire lock (with short timeout)
        3. Execute compensations to clean up any side effects
        4. Mark the workflow as cancelled in storage

        Args:
            instance_id: Workflow instance ID to cancel
            cancelled_by: Who triggered the cancellation (e.g., "user", "admin", "timeout")

        Returns:
            True if successfully cancelled, False if:
            - Instance not found
            - Already completed/failed/cancelled
            - Lock acquisition failed (workflow is actively running)

        Example:
            >>> engine = ReplayEngine(storage, "service", "worker-1")
            >>> success = await engine.cancel_workflow("order-saga-abc123", "admin")
            >>> if success:
            ...     print("Workflow cancelled and compensations executed")
        """
        # Get instance to check status
        instance = await self.storage.get_instance(instance_id)
        if instance is None:
            return False

        current_status = instance["status"]

        # Only cancel running or waiting workflows
        if current_status not in (
            "running",
            "waiting_for_event",
            "waiting_for_timer",
            "waiting_for_message",
        ):
            return False

        # Try to acquire lock with short timeout (5 seconds)
        # If the workflow is actively executing, we may not be able to get the lock
        try:
            lock_acquired = await self.storage.try_acquire_lock(
                instance_id=instance_id,
                worker_id=self.worker_id,
                timeout_seconds=5,
            )

            if not lock_acquired:
                # Another worker has the lock, try to cancel anyway
                # The storage layer will handle atomicity
                return await self.storage.cancel_instance(instance_id, cancelled_by)

            try:
                # Re-fetch instance data AFTER acquiring lock
                logger.debug("Fetching instance data for %s", instance_id)
                instance_locked = await self.storage.get_instance(instance_id)
                if instance_locked is None:
                    logger.warning("Instance %s not found after lock acquisition", instance_id)
                    return False

                # Create context for compensation execution
                ctx = WorkflowContext(
                    instance_id=instance_id,
                    workflow_name=instance_locked["workflow_name"],
                    storage=self.storage,
                    worker_id=self.worker_id,
                    is_replaying=False,
                    hooks=self.hooks,
                )
                # Set default retry policy for activity resolution
                ctx._app_retry_policy = self.default_retry_policy

                # Execute compensations to clean up
                logger.debug("Executing compensations for %s", instance_id)
                await execute_compensations(ctx)

                # Mark as cancelled in storage
                success = await self.storage.cancel_instance(instance_id, cancelled_by)

                return success

            finally:
                # Always release the lock
                await self.storage.release_lock(instance_id, self.worker_id)

        except Exception as error:
            # Log error but don't propagate
            logger.error("Error cancelling workflow %s: %s", instance_id, error, exc_info=True)
            return False

    async def resume_compensating_workflow(self, instance_id: str) -> bool:
        """
        Resume a workflow that crashed during compensation execution.

        This method only re-executes incomplete compensations without running
        the workflow function. It determines the target status (failed/cancelled)
        from the instance metadata.

        Args:
            instance_id: Workflow instance ID

        Returns:
            True if compensations completed successfully, False otherwise
        """
        logger.info("Starting compensation recovery for %s", instance_id)

        try:
            # Acquire lock
            locked = await self.storage.try_acquire_lock(
                instance_id=instance_id,
                worker_id=self.worker_id,
                timeout_seconds=300,
            )

            if not locked:
                logger.debug("Could not acquire lock for %s", instance_id)
                return False

            try:
                # Get instance data
                instance = await self.storage.get_instance(instance_id)
                if instance is None:
                    logger.warning("Instance %s not found", instance_id)
                    return False

                # Check current status
                current_status = instance["status"]
                if current_status != "compensating":
                    logger.debug(
                        "Instance %s is not in compensating state (status=%s)",
                        instance_id,
                        current_status,
                    )
                    return False

                # Determine target status based on history or metadata
                # If we can't determine, default to "failed"
                target_status = "failed"

                # Check history for cancellation markers (LIMIT 1 optimization)
                cancellation_event = await self.storage.find_first_cancellation_event(instance_id)
                if cancellation_event is not None:
                    target_status = "cancelled"

                logger.debug("Target status after compensation: %s", target_status)

                # Create context for compensation execution
                ctx = WorkflowContext(
                    instance_id=instance_id,
                    workflow_name=instance["workflow_name"],
                    storage=self.storage,
                    worker_id=self.worker_id,
                    is_replaying=False,
                    hooks=self.hooks,
                )
                # Set default retry policy for activity resolution
                ctx._app_retry_policy = self.default_retry_policy

                # Re-execute compensations (idempotent - skips already executed)
                logger.debug("Re-executing compensations for %s", instance_id)
                await execute_compensations(ctx)

                # Mark with target status
                if target_status == "cancelled":
                    success = await self.storage.cancel_instance(instance_id, "crash_recovery")
                    logger.info("Marked %s as cancelled", instance_id)
                else:
                    await ctx._update_status(
                        "failed", {"error": "Workflow failed before compensation"}
                    )
                    logger.info("Marked %s as failed", instance_id)
                    success = True

                return success

            finally:
                # Always release the lock
                await self.storage.release_lock(instance_id, self.worker_id)

        except Exception as error:
            # Log error but don't propagate
            logger.error(
                "Error resuming compensating workflow %s: %s",
                instance_id,
                error,
                exc_info=True,
            )
            return False
