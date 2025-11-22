"""
Workflow context module for Edda framework.

This module provides the WorkflowContext class for workflow execution,
managing state, history, and replay during workflow execution.
"""

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import TYPE_CHECKING, Any

from edda.events import ReceivedEvent
from edda.storage.protocol import StorageProtocol

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncSession


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
        """
        self.instance_id = instance_id
        self.workflow_name = workflow_name
        self._storage = storage  # Private: use properties/methods instead
        self.worker_id = worker_id
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

        return self.storage._get_session_for_operation()  # type: ignore[attr-defined]

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
            elif event_type == "TimerExpired":
                # Cache the timer result for wait_timer replay
                # Timer returns None, so we cache the result field
                self._history_cache[activity_id] = event_data.get("result")

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
        )

        # Update current activity ID
        await self.storage.update_instance_activity(self.instance_id, activity_id)

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

        # Include retry metadata if provided
        if retry_metadata is not None:
            event_data["retry_metadata"] = retry_metadata.to_dict()

        await self.storage.append_history(
            self.instance_id,
            activity_id=activity_id,
            event_type="ActivityFailed",
            event_data=event_data,
        )

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
        await self.storage.update_instance_status(self.instance_id, status, output_data)

    async def _register_event_subscription(
        self,
        event_type: str,
        timeout_seconds: int | None = None,
        activity_id: str | None = None,
    ) -> None:
        """
        Register an event subscription for wait_event (internal use only).

        This is called when a workflow calls wait_event() and needs to pause
        until a matching event arrives.

        Args:
            event_type: CloudEvent type to wait for
            timeout_seconds: Optional timeout in seconds
            activity_id: The activity ID where wait_event was called
        """
        from datetime import UTC, datetime, timedelta

        timeout_at = None
        if timeout_seconds is not None:
            timeout_at = datetime.now(UTC) + timedelta(seconds=timeout_seconds)

        await self.storage.add_event_subscription(
            instance_id=self.instance_id,
            event_type=event_type,
            timeout_at=timeout_at,
        )

        # Update current activity ID
        if activity_id is not None:
            await self.storage.update_instance_activity(self.instance_id, activity_id)

    async def _record_event_received(self, activity_id: str, event_data: dict[str, Any]) -> None:
        """
        Record that an event was received during wait_event (internal use only).

        This is called when resuming a workflow after an event arrives.

        Args:
            activity_id: The activity ID where wait_event was called
            event_data: The received event data
        """
        await self.storage.append_history(
            instance_id=self.instance_id,
            activity_id=activity_id,
            event_type="EventReceived",
            event_data={"event_data": event_data},
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
        except Exception:
            await self.storage.rollback_transaction()
            raise

    def use_session(self, session: "AsyncSession") -> None:
        """
        Share an external SQLAlchemy AsyncSession with Edda for atomic operations.

        This method enables true transactional outbox pattern by allowing user
        business logic and Edda's event publishing to execute within the same
        database transaction, ensuring atomicity.

        Example:
            from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession

            engine = create_async_engine("postgresql+asyncpg://...")

            async with AsyncSession(engine) as session:
                async with session.begin():
                    # Business logic
                    session.add(Order(order_id="ORD-001", amount=100))

                    # Share session with Edda
                    ctx.use_session(session)

                    # Activity execution + event publishing (same transaction)
                    await create_order_activity(ctx, order_id="ORD-001")
                    await send_event_transactional(ctx, "order.created", ...)

                    # All operations commit/rollback together (atomic)

        Args:
            session: SQLAlchemy AsyncSession to use for Edda operations

        Note:
            - Session lifecycle is managed by the caller (begin/commit/rollback)
            - Edda will NOT commit or rollback the session
            - Lock operations always use separate transactions for safety
            - This is a convenience method that delegates to storage.use_session()
        """
        self.storage.use_session(session)

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

    def __repr__(self) -> str:
        """String representation of the context."""
        return (
            f"WorkflowContext(instance_id={self.instance_id!r}, "
            f"workflow_name={self.workflow_name!r}, "
            f"executed_activities={len(self.executed_activity_ids)}, "
            f"is_replaying={self.is_replaying})"
        )
