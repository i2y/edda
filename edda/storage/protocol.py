"""
Storage protocol definition for Edda framework.

This module defines the StorageProtocol using Python's structural typing (Protocol).
Any storage implementation that conforms to this protocol can be used with Edda.
"""

from datetime import datetime
from typing import TYPE_CHECKING, Any, Protocol, runtime_checkable

if TYPE_CHECKING:
    pass


@runtime_checkable
class StorageProtocol(Protocol):
    """
    Protocol for storage backend implementations.

    This protocol defines all the methods that a storage backend must implement
    to work with the Edda framework. It supports workflow instances, execution
    history, compensations, event subscriptions, outbox events, and distributed locking.
    """

    async def initialize(self) -> None:
        """
        Initialize storage (create tables, connections, etc.).

        This method should be idempotent - calling it multiple times
        should not cause errors.
        """
        ...

    async def close(self) -> None:
        """
        Close storage connections and cleanup resources.

        This method should be called when shutting down the application.
        """
        ...

    # -------------------------------------------------------------------------
    # Transaction Management Methods
    # -------------------------------------------------------------------------

    async def begin_transaction(self) -> None:
        """
        Begin a new transaction.

        If a transaction is already in progress, this will create a nested
        transaction using savepoints (supported by SQLite and PostgreSQL).

        This method is typically called by WorkflowContext.transaction() and
        should not be called directly by user code.

        Example:
            async with ctx.transaction():
                # All operations here are in the same transaction
                await ctx.storage.append_history(...)
                await send_event_transactional(ctx, ...)
        """
        ...

    async def commit_transaction(self) -> None:
        """
        Commit the current transaction.

        For nested transactions (savepoints), this will release the savepoint.
        For top-level transactions, this will commit all changes to the database.

        This method is typically called by WorkflowContext.transaction() and
        should not be called directly by user code.

        Raises:
            RuntimeError: If not in a transaction
        """
        ...

    async def rollback_transaction(self) -> None:
        """
        Rollback the current transaction.

        For nested transactions (savepoints), this will rollback to the savepoint.
        For top-level transactions, this will rollback all changes.

        This method is typically called by WorkflowContext.transaction() on
        exception and should not be called directly by user code.

        Raises:
            RuntimeError: If not in a transaction
        """
        ...

    def in_transaction(self) -> bool:
        """
        Check if currently in a transaction.

        Returns:
            True if in a transaction, False otherwise.

        Note:
            This is a synchronous method because it only checks state,
            it does not perform any I/O operations.
        """
        ...

    # -------------------------------------------------------------------------
    # Workflow Definition Methods
    # -------------------------------------------------------------------------

    async def upsert_workflow_definition(
        self,
        workflow_name: str,
        source_hash: str,
        source_code: str,
    ) -> None:
        """
        Insert or update a workflow definition.

        This method stores the workflow source code with a unique combination
        of workflow_name and source_hash. If the same combination already exists,
        it updates the record (idempotent).

        Args:
            workflow_name: Name of the workflow (e.g., "order_saga")
            source_hash: SHA256 hash of the source code
            source_code: Source code of the workflow function
        """
        ...

    async def get_workflow_definition(
        self,
        workflow_name: str,
        source_hash: str,
    ) -> dict[str, Any] | None:
        """
        Get a workflow definition by name and hash.

        Args:
            workflow_name: Name of the workflow
            source_hash: SHA256 hash of the source code

        Returns:
            Dictionary containing definition metadata, or None if not found.
            Expected keys: workflow_name, source_hash, source_code, created_at
        """
        ...

    async def get_current_workflow_definition(
        self,
        workflow_name: str,
    ) -> dict[str, Any] | None:
        """
        Get the most recent workflow definition by name.

        This returns the latest definition for a workflow, which may differ
        from older definitions if the workflow code has changed.

        Args:
            workflow_name: Name of the workflow

        Returns:
            Dictionary containing definition metadata, or None if not found.
            Expected keys: workflow_name, source_hash, source_code, created_at
        """
        ...

    # -------------------------------------------------------------------------
    # Workflow Instance Methods
    # -------------------------------------------------------------------------

    async def create_instance(
        self,
        instance_id: str,
        workflow_name: str,
        source_hash: str,
        owner_service: str,
        input_data: dict[str, Any],
        lock_timeout_seconds: int | None = None,
    ) -> None:
        """
        Create a new workflow instance.

        Args:
            instance_id: Unique identifier for the workflow instance
            workflow_name: Name of the workflow (e.g., "order_saga")
            source_hash: SHA256 hash of the workflow source code
            owner_service: Service that owns this workflow (e.g., "order-service")
            input_data: Input parameters for the workflow (serializable dict)
            lock_timeout_seconds: Lock timeout for this workflow (None = use global default 300s)
        """
        ...

    async def get_instance(self, instance_id: str) -> dict[str, Any] | None:
        """
        Get workflow instance metadata with its definition.

        This method JOINs workflow_instances with workflow_definitions to
        return the instance along with its source code.

        Args:
            instance_id: Unique identifier for the workflow instance

        Returns:
            Dictionary containing instance metadata, or None if not found.
            Expected keys: instance_id, workflow_name, source_hash, owner_service,
            status, current_activity_id, started_at, updated_at, input_data, source_code,
            output_data, locked_by, locked_at
        """
        ...

    async def update_instance_status(
        self,
        instance_id: str,
        status: str,
        output_data: dict[str, Any] | None = None,
    ) -> None:
        """
        Update workflow instance status.

        Args:
            instance_id: Unique identifier for the workflow instance
            status: New status (e.g., "running", "completed", "failed", "waiting_for_event")
            output_data: Optional output data (for completed workflows)
        """
        ...

    async def update_instance_activity(self, instance_id: str, activity_id: str) -> None:
        """
        Update the current activity ID for a workflow instance.

        Args:
            instance_id: Unique identifier for the workflow instance
            activity_id: Current activity ID being executed
        """
        ...

    async def list_instances(
        self,
        limit: int = 50,
        status_filter: str | None = None,
    ) -> list[dict[str, Any]]:
        """
        List workflow instances with optional filtering.

        This method JOINs workflow_instances with workflow_definitions to
        return instances along with their source code.

        Args:
            limit: Maximum number of instances to return
            status_filter: Optional status filter (e.g., "running", "completed", "failed")

        Returns:
            List of workflow instances, ordered by started_at DESC.
            Each instance contains: instance_id, workflow_name, source_hash,
            owner_service, status, current_activity_id, started_at, updated_at,
            input_data, source_code, output_data, locked_by, locked_at
        """
        ...

    # -------------------------------------------------------------------------
    # Distributed Locking Methods
    # -------------------------------------------------------------------------

    async def try_acquire_lock(
        self,
        instance_id: str,
        worker_id: str,
        timeout_seconds: int = 300,
    ) -> bool:
        """
        Try to acquire lock for workflow instance.

        This method implements distributed locking to ensure only one worker
        processes a workflow instance at a time. It can acquire locks that
        have timed out.

        Args:
            instance_id: Workflow instance to lock
            worker_id: Unique identifier of the worker acquiring the lock
            timeout_seconds: Lock timeout in seconds (default: 300)

        Returns:
            True if lock was acquired, False if already locked by another worker
        """
        ...

    async def release_lock(self, instance_id: str, worker_id: str) -> None:
        """
        Release lock for workflow instance.

        Only the worker that holds the lock can release it.

        Args:
            instance_id: Workflow instance to unlock
            worker_id: Unique identifier of the worker releasing the lock
        """
        ...

    async def refresh_lock(self, instance_id: str, worker_id: str) -> bool:
        """
        Refresh lock timestamp for long-running workflows.

        This prevents the lock from timing out during long operations.

        Args:
            instance_id: Workflow instance to refresh
            worker_id: Unique identifier of the worker holding the lock

        Returns:
            True if successfully refreshed, False if lock was lost
        """
        ...

    async def cleanup_stale_locks(self) -> list[dict[str, str]]:
        """
        Clean up locks that have expired (based on lock_expires_at column).

        This should be called periodically to clean up locks from crashed workers.

        Returns:
            List of cleaned workflow instances with status='running' or 'compensating'.
            Each dict contains: {'instance_id': str, 'workflow_name': str, 'source_hash': str}
            These are workflows that need to be auto-resumed.
        """
        ...

    # -------------------------------------------------------------------------
    # History Methods (for Deterministic Replay)
    # -------------------------------------------------------------------------

    async def append_history(
        self,
        instance_id: str,
        activity_id: str,
        event_type: str,
        event_data: dict[str, Any] | bytes,
    ) -> None:
        """
        Append an event to workflow execution history.

        The history is used for deterministic replay - each activity result
        is stored as a history event.

        Args:
            instance_id: Workflow instance
            activity_id: Activity ID in the workflow
            event_type: Type of event (e.g., "ActivityCompleted", "ActivityFailed")
            event_data: Event payload (JSON dict or binary bytes)
        """
        ...

    async def get_history(self, instance_id: str) -> list[dict[str, Any]]:
        """
        Get workflow execution history in order.

        Args:
            instance_id: Workflow instance

        Returns:
            List of history events, ordered by creation time.
            Each event contains: id, instance_id, activity_id, event_type, event_data, created_at
        """
        ...

    # -------------------------------------------------------------------------
    # Compensation Methods (for Saga Pattern)
    # -------------------------------------------------------------------------

    async def push_compensation(
        self,
        instance_id: str,
        activity_id: str,
        activity_name: str,
        args: dict[str, Any],
    ) -> None:
        """
        Push a compensation to the stack (LIFO).

        Compensations are executed in reverse order when a saga fails.

        Args:
            instance_id: Workflow instance
            activity_id: Activity ID where compensation was registered
            activity_name: Name of the compensation activity
            args: Arguments to pass to the compensation activity
        """
        ...

    async def get_compensations(self, instance_id: str) -> list[dict[str, Any]]:
        """
        Get compensations in LIFO order (most recent first).

        Args:
            instance_id: Workflow instance

        Returns:
            List of compensations, ordered by creation time DESC (most recent first).
            Each compensation contains: id, instance_id, activity_id, activity_name, args, created_at
        """
        ...

    async def clear_compensations(self, instance_id: str) -> None:
        """
        Clear all compensations for a workflow instance.

        Called after successful workflow completion.

        Args:
            instance_id: Workflow instance
        """
        ...

    # -------------------------------------------------------------------------
    # Event Subscription Methods (for wait_event)
    # -------------------------------------------------------------------------

    async def add_event_subscription(
        self,
        instance_id: str,
        event_type: str,
        timeout_at: datetime | None = None,
    ) -> None:
        """
        Register an event wait subscription.

        When a workflow calls wait_event(), a subscription is created
        in the database so that incoming events can be routed to the
        waiting workflow.

        Note: filter_expr is not needed because subscriptions are uniquely
        identified by instance_id. Events are delivered to specific workflow
        instances, not filtered across multiple instances.

        Args:
            instance_id: Workflow instance
            event_type: CloudEvent type to wait for (e.g., "payment.completed")
            timeout_at: Optional timeout timestamp
        """
        ...

    async def find_waiting_instances(self, event_type: str) -> list[dict[str, Any]]:
        """
        Find workflow instances waiting for a specific event type.

        Called when an event arrives to find which workflows are waiting for it.

        Args:
            event_type: CloudEvent type

        Returns:
            List of waiting instances with subscription info.
            Each item contains: instance_id, event_type, timeout_at
        """
        ...

    async def remove_event_subscription(
        self,
        instance_id: str,
        event_type: str,
    ) -> None:
        """
        Remove event subscription after the event is received.

        Args:
            instance_id: Workflow instance
            event_type: CloudEvent type
        """
        ...

    async def cleanup_expired_subscriptions(self) -> int:
        """
        Clean up event subscriptions that have timed out.

        Returns:
            Number of subscriptions cleaned up
        """
        ...

    async def find_expired_event_subscriptions(
        self,
    ) -> list[dict[str, Any]]:
        """
        Find event subscriptions that have timed out.

        Returns:
            List of dictionaries containing:
            - instance_id: Workflow instance ID
            - event_type: Event type that was being waited for
            - timeout_at: Timeout timestamp (ISO 8601 string)
            - created_at: Subscription creation timestamp (ISO 8601 string)

        Note:
            This method does NOT delete the subscriptions - it only finds them.
            Use cleanup_expired_subscriptions() to delete them after processing.
        """
        ...

    async def register_event_subscription_and_release_lock(
        self,
        instance_id: str,
        worker_id: str,
        event_type: str,
        timeout_at: datetime | None = None,
        activity_id: str | None = None,
    ) -> None:
        """
        Atomically register event subscription and release workflow lock.

        This method performs the following operations in a SINGLE database transaction:
        1. Register event subscription (INSERT into workflow_event_subscriptions)
        2. Update current activity (UPDATE workflow_instances.current_activity_id)
        3. Release lock (UPDATE workflow_instances set locked_by=NULL)

        This ensures that when a workflow calls wait_event(), the subscription is
        registered and the lock is released atomically, preventing race conditions
        in distributed environments (distributed coroutines pattern).

        Note: filter_expr is not needed because subscriptions are uniquely identified
        by instance_id. Events are delivered to specific workflow instances.

        Args:
            instance_id: Workflow instance ID
            worker_id: Worker ID that currently holds the lock
            event_type: CloudEvent type to wait for
            timeout_at: Optional timeout timestamp
            activity_id: Current activity ID to record

        Raises:
            RuntimeError: If the worker doesn't hold the lock (sanity check)
        """
        ...

    async def register_timer_subscription_and_release_lock(
        self,
        instance_id: str,
        worker_id: str,
        timer_id: str,
        expires_at: datetime,
        activity_id: str | None = None,
    ) -> None:
        """
        Atomically register timer subscription and release workflow lock.

        This method performs the following operations in a SINGLE database transaction:
        1. Register timer subscription (INSERT into workflow_timer_subscriptions)
        2. Update current activity (UPDATE workflow_instances.current_activity_id)
        3. Release lock (UPDATE workflow_instances set locked_by=NULL)

        This ensures that when a workflow calls wait_timer(), the subscription is
        registered and the lock is released atomically, preventing race conditions
        in distributed environments (distributed coroutines pattern).

        Args:
            instance_id: Workflow instance ID
            worker_id: Worker ID that currently holds the lock
            timer_id: Timer identifier (unique per instance)
            expires_at: Expiration timestamp
            activity_id: Current activity ID to record

        Raises:
            RuntimeError: If the worker doesn't hold the lock (sanity check)
        """
        ...

    async def find_expired_timers(self) -> list[dict[str, Any]]:
        """
        Find timer subscriptions that have expired.

        This method is called periodically by background task to find
        workflows waiting for timers that have expired.

        Returns:
            List of expired timer subscriptions.
            Each item contains: instance_id, timer_id, expires_at, activity_id, workflow_name
        """
        ...

    async def remove_timer_subscription(
        self,
        instance_id: str,
        timer_id: str,
    ) -> None:
        """
        Remove timer subscription after the timer expires.

        Args:
            instance_id: Workflow instance ID
            timer_id: Timer identifier
        """
        ...

    # -------------------------------------------------------------------------
    # Transactional Outbox Methods
    # -------------------------------------------------------------------------

    async def add_outbox_event(
        self,
        event_id: str,
        event_type: str,
        event_source: str,
        event_data: dict[str, Any] | bytes,
        content_type: str = "application/json",
    ) -> None:
        """
        Add an event to the transactional outbox.

        Events in the outbox are published asynchronously by the relayer.

        Args:
            event_id: Unique event identifier
            event_type: CloudEvent type
            event_source: CloudEvent source
            event_data: Event payload (JSON dict or binary bytes)
            content_type: Content type (defaults to application/json)
        """
        ...

    async def get_pending_outbox_events(self, limit: int = 10) -> list[dict[str, Any]]:
        """
        Get pending/failed outbox events and atomically mark them as 'processing'.

        This method uses SELECT FOR UPDATE (with SKIP LOCKED on PostgreSQL/MySQL)
        to safely fetch events in a multi-worker environment. It fetches both
        'pending' and 'failed' events (for automatic retry). Fetched events are
        immediately marked as 'processing' within the same transaction to prevent
        duplicate processing by other workers.

        Args:
            limit: Maximum number of events to return

        Returns:
            List of events (now with status='processing'), ordered by created_at.
            Each event contains: event_id, event_type, event_source, event_data,
            created_at, status ('processing'), retry_count, last_error

        Note:
            - Fetches both 'pending' and 'failed' events (failed events will be retried)
            - Returned events will always have status='processing' (not 'pending'/'failed')
            - This prevents duplicate processing in distributed environments
            - After successful publishing, call mark_outbox_published(event_id)
            - On failure, call mark_outbox_failed(event_id, error_message)
        """
        ...

    async def mark_outbox_published(self, event_id: str) -> None:
        """
        Mark outbox event as successfully published.

        Args:
            event_id: Event identifier
        """
        ...

    async def mark_outbox_failed(self, event_id: str, error: str) -> None:
        """
        Mark outbox event as failed and increment retry count.

        Args:
            event_id: Event identifier
            error: Error message
        """
        ...

    async def mark_outbox_permanently_failed(self, event_id: str, error: str) -> None:
        """
        Mark outbox event as permanently failed (no more retries).

        Args:
            event_id: Event identifier
            error: Error message
        """
        ...

    async def mark_outbox_invalid(self, event_id: str, error: str) -> None:
        """
        Mark outbox event as invalid (client error, don't retry).

        Used for 4xx HTTP errors where retrying won't help (malformed payload,
        authentication failure, etc.).

        Args:
            event_id: Event identifier
            error: Error message (should include HTTP status code)
        """
        ...

    async def mark_outbox_expired(self, event_id: str, error: str) -> None:
        """
        Mark outbox event as expired (too old to retry).

        Used when max_age_hours is exceeded. Events become meaningless after
        a certain time.

        Args:
            event_id: Event identifier
            error: Error message
        """
        ...

    async def cleanup_published_events(self, older_than_hours: int = 24) -> int:
        """
        Clean up successfully published events older than threshold.

        Args:
            older_than_hours: Age threshold in hours

        Returns:
            Number of events cleaned up
        """
        ...

    # -------------------------------------------------------------------------
    # Workflow Cancellation Methods
    # -------------------------------------------------------------------------

    async def cancel_instance(self, instance_id: str, cancelled_by: str) -> bool:
        """
        Cancel a workflow instance.

        Only running or waiting_for_event workflows can be cancelled.
        This method will:
        1. Check current status (only cancel if running/waiting_for_event)
        2. Update status to 'cancelled'
        3. Clear locks so other workers are not blocked
        4. Remove event subscriptions (if waiting for event)
        5. Record cancellation metadata (cancelled_by, cancelled_at)

        Args:
            instance_id: Workflow instance to cancel
            cancelled_by: Who/what triggered the cancellation (e.g., "user", "timeout", "admin")

        Returns:
            True if successfully cancelled, False if already completed/failed/cancelled
            or if instance not found
        """
        ...
