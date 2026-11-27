"""
Workflow context module for Edda framework.

This module provides the WorkflowContext class for workflow execution,
managing state, history, and replay during workflow execution.
"""

from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import asynccontextmanager
from contextvars import ContextVar
from typing import TYPE_CHECKING, Any, TypeVar, cast

from edda.channels import ChannelMessage, ReceivedEvent
from edda.exceptions import (
    RetryExhaustedError,
    RunAbortedError,
    TerminalError,
    WorkflowCancelledException,
    kernel_error_class,
    kernel_error_code,
)
from edda.locking import LockLostError
from edda.storage.protocol import StorageProtocol

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncSession

T = TypeVar("T")


# Currently executing workflow context, tracked with a ContextVar so nested
# workflow starts can attribute their spawn parent safely across asyncio tasks.
_current_workflow_context: ContextVar[Any] = ContextVar(
    "edda_current_workflow_context", default=None
)


# Contexts whose activity body the current asyncio task is running (one entry
# per nesting level). A ContextVar rather than a counter on the context, so a
# primitive another task runs next to a running activity (asyncio.gather) is
# still a recorded step: each task copies the value when it is created.
_activity_bodies: ContextVar[tuple[int, ...]] = ContextVar("edda_activity_bodies", default=())


def _restore_step_error(record: dict[str, Any]) -> Exception:
    """Rebuild the error a recorded step or activity raised.

    The stable kernel ``code`` wins when present (it is the cross-port
    contract); otherwise the recorded class name is used. Retry exhaustion
    and terminal activity failures keep their framework meaning. Old rows
    without these fields fall back to a generic Exception carrying the old
    ``Type: message`` text.
    """
    import builtins

    from edda import exceptions

    error_type = str(record.get("error_type") or "Exception")
    message = str(record.get("error_message") or "")
    cls = kernel_error_class(record.get("code"))
    if cls is None and record.get("terminal"):
        cls = TerminalError
    if cls is None:
        cls = getattr(exceptions, error_type, None) or getattr(builtins, error_type, None)
    restored: Exception | None = None
    if isinstance(cls, type) and issubclass(cls, Exception):
        try:
            restored = cls(message)
        except Exception:
            restored = None
    if restored is None:
        restored = Exception(f"{error_type}: {message}")
    attempts = record.get("attempts")
    if attempts:
        exhausted = RetryExhaustedError(f"Activity failed after {attempts} attempts: {message}")
        exhausted.__cause__ = restored
        return exhausted
    return restored


class WorkflowContext:
    """
    Context for workflow execution.

    Provides access to workflow instance metadata, storage, history management,
    and utilities for deterministic replay.

    This context is passed to activities and contains all the information needed
    for execution and replay.
    """

    def __init__(
        self,
        instance_id: str,
        workflow_name: str,
        storage: StorageProtocol,
        worker_id: str,
        is_replaying: bool = False,
        hooks: Any = None,
        lock_id: str | None = None,
    ):
        """
        Initialize workflow context.

        Args:
            instance_id: Workflow instance ID
            workflow_name: Name of the workflow
            storage: Storage backend
            worker_id: Worker ID holding the lock
            is_replaying: Whether this is a replay execution
            hooks: Optional WorkflowHooks implementation for observability
            lock_id: Epoch-bearing lock identity; when set, status/activity
                writes are fenced by ``locked_by == lock_id``.
        """
        self.instance_id = instance_id
        self.workflow_name = workflow_name
        self._storage = storage  # Private: use properties/methods instead
        self.worker_id = worker_id
        # Epoch-bearing lock identity; fenced storage writes use this when
        # set. None means unfenced (e.g. standalone/test contexts holding
        # no lock).
        self.lock_id = lock_id
        self.is_replaying = is_replaying
        self.hooks = hooks

        # Activity ID tracking for deterministic replay
        self.executed_activity_ids: set[str] = set()

        # History cache for replay (activity_id -> result)
        self._history_cache: dict[str, Any] = {}

        # Flag to track if we've loaded history
        self._history_loaded = False

        # Auto-generation counter for activity IDs (func_name -> call_count)
        self._activity_call_counters: dict[str, int] = {}

        # Default retry policy from EddaApp (set by ReplayEngine)
        self._app_retry_policy: Any = None

        # Capability key from EddaApp (set by ReplayEngine); None = random secrets
        self._capability_key: bytes | None = None

        # Direct subscriptions: channel names subscribed with mode="direct"
        self._direct_subscriptions: set[str] = set()

        # One unexpected primitive/storage failure poisons the whole run; the
        # same object is re-raised so the engine can tell abortion apart from
        # an ordinary workflow failure.
        self._run_abort_error: RunAbortedError | None = None

    @property
    def _activity_depth(self) -> int:
        """How many activity bodies of this context the current asyncio task is in."""
        return _activity_bodies.get().count(id(self))

    @_activity_depth.setter
    def _activity_depth(self, depth: int) -> None:
        others = tuple(i for i in _activity_bodies.get() if i != id(self))
        _activity_bodies.set(others + (id(self),) * depth)

    def _poison_run(self, error: Exception) -> RunAbortedError:
        """Poison this run with one stable abort error (internal use only)."""
        if self._run_abort_error is None:
            self._run_abort_error = RunAbortedError(error)
        return self._run_abort_error

    def _raise_if_run_aborted(self) -> None:
        """Raise the run's abort error when an earlier primitive poisoned it."""
        if self._run_abort_error is not None:
            raise self._run_abort_error

    @property
    def storage(self) -> StorageProtocol:
        """
        Get storage backend (internal use only).

        Warning:
            This property is for framework internal use only.
            Direct storage access may break deterministic replay guarantees.
            Use WorkflowContext methods instead (transaction(), in_transaction()).
        """
        return self._storage

    @property
    def session(self) -> "AsyncSession":
        """
        Get Edda-managed database session for custom database operations.

        This property provides access to the current transaction's SQLAlchemy session,
        allowing you to execute custom database operations (ORM queries, raw SQL, etc.)
        within the same transaction as Edda's workflow operations.

        The session is automatically managed by Edda:
        - Commit/rollback happens automatically at the end of @activity
        - All operations are atomic (workflow history + your DB operations)
        - Transaction safety is guaranteed

        Returns:
            AsyncSession managed by Edda's transaction context

        Raises:
            RuntimeError: If not inside a transaction (must use @activity or ctx.transaction())

        Example:
            @activity
            async def create_order(ctx: WorkflowContext, order_id: str, amount: float):
                # Get Edda-managed session
                session = ctx.session

                # Your business logic (same DB as Edda)
                order = Order(order_id=order_id, amount=amount)
                session.add(order)

                # Event publishing (same transaction)
                await send_event_transactional(
                    ctx, "order.created", "order-service",
                    {"order_id": order_id, "amount": amount}
                )

                # Edda commits automatically (or rolls back on error)
                return {"order_id": order_id, "status": "created"}

        Note:
            - Requires @activity (default) or async with ctx.transaction()
            - All operations commit/rollback together atomically
            - Your tables must be in the same database as Edda
            - Do NOT call session.commit() or session.rollback() manually
        """
        if not self.storage.in_transaction():
            raise RuntimeError(
                "ctx.session must be accessed inside a transaction. "
                "Use @activity (default) or async with ctx.transaction()"
            )

        return cast("AsyncSession", self.storage._get_session_for_operation())  # type: ignore[attr-defined]

    async def _load_history(self) -> None:
        """
        Load execution history from storage (internal use only).

        This is called at the beginning of a replay to populate the history cache.
        """
        if self._history_loaded:
            return

        history = await self.storage.get_history(self.instance_id)

        for event in history:
            activity_id = event["activity_id"]
            event_type = event["event_type"]
            event_data = event["event_data"]

            # Track executed activity IDs
            self.executed_activity_ids.add(activity_id)

            if event_type == "ActivityCompleted":
                # Cache the activity result
                self._history_cache[activity_id] = event_data.get("result")
            elif event_type == "ActivityFailed":
                # Cache the error for replay
                self._history_cache[activity_id] = {
                    "_error": True,
                    "error_type": event_data.get("error_type"),
                    "error_message": event_data.get("error_message"),
                    "code": event_data.get("code"),
                    "terminal": event_data.get("terminal"),
                    "attempts": event_data.get("attempts"),
                }
            elif event_type == "EventReceived":
                # Cache the event data and metadata for wait_event replay
                # Reconstruct ReceivedEvent from stored data
                payload = event_data.get("payload", {})
                metadata = event_data.get("metadata", {})
                extensions = event_data.get("extensions", {})

                # For backward compatibility: check if old format (event_data directly)
                if "payload" not in event_data and "metadata" not in event_data:
                    # Old format: {"event_data": {...}}
                    payload = event_data.get("event_data", {})
                    metadata = {
                        "type": "unknown",
                        "source": "unknown",
                        "id": "unknown",
                    }

                received_event = ReceivedEvent(
                    data=payload,
                    type=metadata.get("type", "unknown"),
                    source=metadata.get("source", "unknown"),
                    id=metadata.get("id", "unknown"),
                    time=metadata.get("time"),
                    datacontenttype=metadata.get("datacontenttype"),
                    subject=metadata.get("subject"),
                    extensions=extensions,
                )
                self._history_cache[activity_id] = received_event
            elif event_type == "ChannelMessageReceived":
                # Cache the message data for receive() replay
                from datetime import UTC, datetime

                from edda.channels import _message_data_from_history

                data = _message_data_from_history(event_data)
                if not isinstance(event_data, dict):
                    event_data = {}  # a raw binary payload (written before 0.17)
                # Parse published_at if available, otherwise use current time
                published_at_str = event_data.get("published_at")
                if published_at_str:
                    published_at = datetime.fromisoformat(published_at_str.replace("Z", "+00:00"))
                else:
                    published_at = datetime.now(UTC)
                message = ChannelMessage(
                    data=data,
                    channel=event_data.get("channel", "unknown"),
                    id=event_data.get("id", "unknown"),
                    metadata=event_data.get("metadata") or {},
                    published_at=published_at,
                )
                self._history_cache[activity_id] = message
            elif event_type == "TimerExpired":
                # Cache the timer result for wait_timer replay
                # Timer returns None, so we cache the result field
                self._history_cache[activity_id] = event_data.get("result")
            elif event_type == "MessageTimeout":
                # Cache the timeout error for receive() replay
                # This allows TimeoutError to be raised and caught in workflow code
                self._history_cache[activity_id] = {
                    "_error": True,
                    "error_type": event_data.get("error_type", "TimeoutError"),
                    "error_message": event_data.get("error_message", "Message timeout"),
                }

        self._history_loaded = True

    def _get_cached_result(self, activity_id: str) -> tuple[bool, Any]:
        """
        Get cached result for an activity during replay (internal use only).

        Args:
            activity_id: Activity ID

        Returns:
            Tuple of (found, result) where found is True if result was cached
        """
        if activity_id in self._history_cache:
            return True, self._history_cache[activity_id]
        return False, None

    def _generate_activity_id(self, function_name: str) -> str:
        """
        Generate a unique activity ID for auto-generation (internal use only).

        Uses the format: {function_name}:{counter}

        Args:
            function_name: Name of the activity function

        Returns:
            Generated activity ID (e.g., "reserve_inventory:1")
        """
        # Increment counter for this function
        count = self._activity_call_counters.get(function_name, 0) + 1
        self._activity_call_counters[function_name] = count

        activity_id = f"{function_name}:{count}"

        return activity_id

    def _record_activity_id(self, activity_id: str) -> None:
        """
        Record that an activity ID has been executed (internal use only).

        Args:
            activity_id: The activity ID to record
        """
        self.executed_activity_ids.add(activity_id)

    def _record_direct_subscription(self, channel: str) -> None:
        """
        Record that a channel was subscribed in direct mode (internal use only).

        Args:
            channel: The original channel name (before transformation)
        """
        self._direct_subscriptions.add(channel)

    def _is_direct_subscription(self, channel: str) -> bool:
        """
        Check if a channel was subscribed in direct mode (internal use only).

        Args:
            channel: The channel name to check

        Returns:
            True if the channel was subscribed with mode="direct"
        """
        return channel in self._direct_subscriptions

    async def _record_activity_completed(
        self,
        activity_id: str,
        activity_name: str,
        result: Any,
        input_data: dict[str, Any] | None = None,
        retry_metadata: Any = None,
    ) -> None:
        """
        Record that an activity completed successfully (internal use only).

        Args:
            activity_id: Activity ID
            activity_name: Name of the activity
            result: Activity result (must be JSON-serializable)
            input_data: Activity input parameters (args and kwargs)
            retry_metadata: Optional retry metadata (RetryMetadata instance)
        """
        event_data: dict[str, Any] = {
            "activity_name": activity_name,
            "result": result,
            "input": input_data or {},
        }

        # Include retry metadata if provided
        if retry_metadata is not None:
            event_data["retry_metadata"] = retry_metadata.to_dict()

        await self.storage.append_history(
            self.instance_id,
            activity_id=activity_id,
            event_type="ActivityCompleted",
            event_data=event_data,
            lock_id=self.lock_id,
        )

        # Update current activity ID
        await self.storage.update_instance_activity(
            self.instance_id, activity_id, lock_id=self.lock_id
        )

    async def _record_activity_failed(
        self,
        activity_id: str,
        activity_name: str,
        error: Exception,
        input_data: dict[str, Any] | None = None,
        retry_metadata: Any = None,
    ) -> None:
        """
        Record that an activity failed (internal use only).

        Args:
            activity_id: Activity ID
            activity_name: Name of the activity
            error: The exception that was raised
            input_data: Activity input parameters (args and kwargs)
            retry_metadata: Optional retry metadata (RetryMetadata instance)
        """
        import traceback

        # Capture full stack trace
        stack_trace = "".join(traceback.format_exception(type(error), error, error.__traceback__))

        event_data: dict[str, Any] = {
            "activity_name": activity_name,
            "error_type": type(error).__name__,
            "error_message": str(error),
            "stack_trace": stack_trace,
            "input": input_data or {},
        }
        code = kernel_error_code(error)
        if code is not None:
            event_data["code"] = code
        if isinstance(error, TerminalError):
            event_data["terminal"] = True

        # Include retry metadata if provided
        if retry_metadata is not None:
            event_data["retry_metadata"] = retry_metadata.to_dict()
            if getattr(retry_metadata, "exhausted", False):
                event_data["attempts"] = retry_metadata.total_attempts

        await self.storage.append_history(
            self.instance_id,
            activity_id=activity_id,
            event_type="ActivityFailed",
            event_data=event_data,
            lock_id=self.lock_id,
        )

    @asynccontextmanager
    async def _fenced_transaction(self) -> AsyncIterator[None]:
        """
        Run the block in a storage transaction that first proves this run
        still holds the instance lock, committing only if the block succeeds
        (internal use only).

        What the block writes lands together, and never for a run that lost
        its lease. Inside an enclosing transaction the block joins it; a
        context built outside a run (no lock_id) has no lock to prove.
        """
        if self.storage.in_transaction():
            yield
            return
        await self.storage.begin_transaction()
        try:
            if self.lock_id is not None:
                await self.storage.prove_lock(self.instance_id, self.lock_id)
            yield
        except BaseException:
            await self.storage.rollback_transaction()
            raise
        await self.storage.commit_transaction()

    async def _run_step(
        self, name: str, fn: Callable[[], Awaitable[T]], *, effect: bool = False
    ) -> T:
        """
        Run a kernel primitive as a recorded workflow step (internal use only).

        A value or a kernel-defined error is the step's outcome and is recorded.
        Any other exception is not an outcome: the run is poisoned and ends so a
        later run re-executes the primitive. A primitive reports a bad argument
        or payload it detects itself as InvalidArgumentError; a TypeError or
        ValueError from anywhere else (a driver's "Connection closed", say) is
        not an outcome. Inside an activity body fn runs directly; the activity
        is the recorded unit.
        """
        self._raise_if_run_aborted()
        if self._activity_depth > 0:
            return await fn()
        activity_id = self._generate_activity_id(name)
        self._record_activity_id(activity_id)
        found, cached = self._get_cached_result(activity_id)
        if found:
            if isinstance(cached, dict) and cached.get("_error"):
                raise _restore_step_error(cached)
            return cast(T, cached)

        async def record_kernel_failure(error: Exception) -> None:
            try:
                await self._record_activity_failed(activity_id, name, error)
            except (LockLostError, WorkflowCancelledException):
                raise
            except Exception as record_error:
                raise self._poison_run(record_error) from record_error

        if not effect:
            try:
                result = await fn()
            except (LockLostError, WorkflowCancelledException):
                raise
            except Exception as error:
                if kernel_error_code(error) is not None:
                    await record_kernel_failure(error)
                    raise error
                raise self._poison_run(error) from error
            try:
                await self._record_activity_completed(activity_id, name, result)
            except (LockLostError, WorkflowCancelledException):
                raise
            except Exception as error:
                raise self._poison_run(error) from error
            return result

        fn_error: Exception | None = None
        try:
            async with self._fenced_transaction():
                try:
                    result = await fn()
                except Exception as error:
                    fn_error = error
                    raise
                await self._record_activity_completed(activity_id, name, result)
        except (LockLostError, WorkflowCancelledException):
            raise
        except Exception as error:
            if fn_error is not None:
                if kernel_error_code(fn_error) is not None:
                    await record_kernel_failure(fn_error)
                    raise fn_error from error
                raise self._poison_run(fn_error) from fn_error
            # Recording or committing failed: storage errors are not outcomes.
            raise self._poison_run(error) from error
        return result

    async def _get_instance(self) -> dict[str, Any] | None:
        """
        Get the workflow instance metadata (internal use only).

        Returns:
            Instance metadata dictionary or None if not found
        """
        return await self.storage.get_instance(self.instance_id)

    async def _update_status(self, status: str, output_data: dict[str, Any] | None = None) -> None:
        """
        Update the workflow instance status (internal use only).

        Args:
            status: New status (e.g., "completed", "failed", "waiting_for_event")
            output_data: Optional output data for completed workflows
        """
        await self.storage.update_instance_status(
            self.instance_id, status, output_data, lock_id=self.lock_id
        )

    async def _push_compensation(self, compensation_action: Any, activity_id: str) -> None:
        """
        Register a compensation action for this workflow (internal use only).

        Compensation actions are stored in LIFO order and executed on failure.

        Args:
            compensation_action: The CompensationAction to register
            activity_id: The activity ID where compensation was registered
        """
        # Serialize compensation action with full args and kwargs
        await self.storage.push_compensation(
            instance_id=self.instance_id,
            activity_id=activity_id,
            activity_name=compensation_action.name,
            args={
                "name": compensation_action.name,
                "args": list(compensation_action.args),  # Convert tuple to list for JSON
                "kwargs": compensation_action.kwargs,
            },
        )

    async def _get_compensations(self) -> list[dict[str, Any]]:
        """
        Get all registered compensation actions (internal use only).

        Returns:
            List of compensation data dictionaries
        """
        return await self.storage.get_compensations(self.instance_id)

    async def _clear_compensations(self) -> None:
        """
        Clear all registered compensations (internal use only).

        This is called when a workflow completes successfully.
        """
        await self.storage.clear_compensations(self.instance_id)

    @asynccontextmanager
    async def transaction(self) -> AsyncIterator[None]:
        """
        Create a transactional context for atomic operations.

        This context manager allows you to execute multiple storage operations
        within a single database transaction. All operations will be committed
        together, or rolled back together if an exception occurs.

        Example:
            async with ctx.transaction():
                # All operations here are in the same transaction
                await ctx.storage.append_history(...)
                await send_event_transactional(ctx, ...)
                # If any operation fails, all changes are rolled back

        Yields:
            None

        Raises:
            Exception: If any operation within the transaction fails,
                      the transaction is rolled back and the exception is re-raised
        """
        await self.storage.begin_transaction()
        try:
            yield
            await self.storage.commit_transaction()
        except BaseException:
            await self.storage.rollback_transaction()
            raise

    def in_transaction(self) -> bool:
        """
        Check if currently in a transaction.

        This method is useful for ensuring that transactional operations
        (like send_event_transactional) are called within a transaction context.

        Returns:
            True if inside a transaction context, False otherwise

        Example:
            if ctx.in_transaction():
                await send_event_transactional(ctx, "order.created", ...)
            else:
                logger.warning("Not in transaction, using outbox pattern")
                await send_event_transactional(ctx, "order.created", ...)
        """
        return self.storage.in_transaction()

    def register_post_commit(self, callback: Callable[[], Awaitable[None]]) -> None:
        """
        Register a callback to be executed after the current transaction commits.

        The callback will be executed after the top-level transaction commits successfully.
        If the transaction is rolled back, the callback will NOT be executed.
        This is useful for deferring side effects (like message delivery) until after
        the transaction has been committed.

        Args:
            callback: An async function to call after commit.

        Raises:
            RuntimeError: If not in a transaction.

        Example:
            async with ctx.transaction():
                # Save order to database
                await ctx.storage.append_history(...)

                # Defer message delivery until after commit
                async def deliver_notifications():
                    await notify_subscribers(order_id)
                ctx.register_post_commit(deliver_notifications)
        """
        self.storage.register_post_commit_callback(callback)

    async def recur(self, **kwargs: Any) -> None:
        """
        Restart the workflow with fresh history (Erlang-style tail recursion).

        This method prevents unbounded history growth in long-running loops by
        ending the current iteration and continuing the SAME workflow instance
        with the provided arguments. The current history is archived and the
        instance briefly enters the live ``recurred`` state before the next
        iteration runs.

        This is similar to Erlang's tail recursion pattern where calling the same
        function at the end of a loop prevents stack growth. In Edda, `recur()`
        prevents history growth.

        Args:
            **kwargs: Arguments to pass to the new workflow instance.
                     These become the input parameters for the next iteration.

        Raises:
            RecurException: Always raised to signal the ReplayEngine to handle
                           the recur operation. This exception should not be caught.

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
            ...             # Reset history every 1000 iterations
            ...             await ctx.recur(processed_count=processed_count + count)
            ...             # Code after recur() is never executed

        Note:
            - Group memberships are NOT automatically transferred. You must re-join
              groups in the new iteration if needed.
            - The workflow's history is archived, not deleted.
            - The instance id, actor names, monitors, capabilities, tasks, FS
              ownership and sandbox flag survive the iteration.
            - During replay, if recur() was already called, this raises immediately
              without re-executing previous activities.
        """
        from edda.pydantic_utils import to_json_dict
        from edda.workflow import RecurException

        # Convert Pydantic models and Enums to JSON-compatible values
        processed_kwargs = {k: to_json_dict(v) for k, v in kwargs.items()}

        raise RecurException(kwargs=processed_kwargs)

    def __repr__(self) -> str:
        """String representation of the context."""
        return (
            f"WorkflowContext(instance_id={self.instance_id!r}, "
            f"workflow_name={self.workflow_name!r}, "
            f"executed_activities={len(self.executed_activity_ids)}, "
            f"is_replaying={self.is_replaying})"
        )
