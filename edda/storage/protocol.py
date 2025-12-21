"""
Storage protocol definition for Edda framework.

This module defines the StorageProtocol using Python's structural typing (Protocol).
Any storage implementation that conforms to this protocol can be used with Edda.
"""

from collections.abc import Awaitable, Callable
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
    history, compensations, message subscriptions, outbox events, and distributed locking.
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

    def register_post_commit_callback(self, callback: Callable[[], Awaitable[None]]) -> None:
        """
        Register a callback to be executed after the current transaction commits.

        The callback will be executed after the top-level transaction commits successfully.
        If the transaction is rolled back, the callback will NOT be executed.

        Args:
            callback: An async function to call after commit.

        Raises:
            RuntimeError: If not in a transaction.
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
        continued_from: str | None = None,
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
            continued_from: Optional instance ID this workflow continues from (for recur pattern)
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
        page_token: str | None = None,
        status_filter: str | None = None,
        workflow_name_filter: str | None = None,
        instance_id_filter: str | None = None,
        started_after: datetime | None = None,
        started_before: datetime | None = None,
        input_filters: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """
        List workflow instances with cursor-based pagination and filtering.

        This method JOINs workflow_instances with workflow_definitions to
        return instances along with their source code.

        Args:
            limit: Maximum number of instances to return per page
            page_token: Cursor for pagination (format: "ISO_DATETIME||INSTANCE_ID")
            status_filter: Optional status filter (e.g., "running", "completed", "failed")
            workflow_name_filter: Optional workflow name filter (partial match, case-insensitive)
            instance_id_filter: Optional instance ID filter (partial match, case-insensitive)
            started_after: Filter instances started after this datetime (inclusive)
            started_before: Filter instances started before this datetime (inclusive)
            input_filters: Filter by input data values. Keys are JSON paths
                (e.g., "order_id" or "customer.email"), values are expected
                values (exact match). All filters are AND-combined.

        Returns:
            Dictionary containing:
            - instances: List of workflow instances, ordered by started_at DESC
            - next_page_token: Cursor for the next page, or None if no more pages
            - has_more: Boolean indicating if there are more pages

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
    # System-level Locking Methods (for background task coordination)
    # -------------------------------------------------------------------------

    async def try_acquire_system_lock(
        self,
        lock_name: str,
        worker_id: str,
        timeout_seconds: int = 60,
    ) -> bool:
        """
        Try to acquire a system-level lock for coordinating background tasks.

        System locks are used to coordinate operational tasks (cleanup, auto-resume)
        across multiple pods, ensuring only one pod executes these tasks at a time.

        Unlike workflow locks (which lock existing instances), system locks create
        lock records on-demand.

        Args:
            lock_name: Unique name for this lock (e.g., "cleanup_stale_locks")
            worker_id: Unique identifier of the worker acquiring the lock
            timeout_seconds: Lock timeout in seconds (default: 60)

        Returns:
            True if lock was acquired, False if already locked by another worker
        """
        ...

    async def release_system_lock(self, lock_name: str, worker_id: str) -> None:
        """
        Release a system-level lock.

        Only the worker that holds the lock can release it.

        Args:
            lock_name: Name of the lock to release
            worker_id: Unique identifier of the worker releasing the lock
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

    async def archive_history(self, instance_id: str) -> int:
        """
        Archive workflow history for the recur pattern.

        Moves all history entries from workflow_history to workflow_history_archive.
        This is called when a workflow uses recur() to restart with fresh history.

        Args:
            instance_id: Workflow instance whose history should be archived

        Returns:
            Number of history entries archived
        """
        ...

    async def find_first_cancellation_event(self, instance_id: str) -> dict[str, Any] | None:
        """
        Find the first cancellation event in workflow history.

        This is an optimized query that uses LIMIT 1 to avoid loading
        all history events when checking for cancellation status.

        Args:
            instance_id: Workflow instance ID

        Returns:
            The first cancellation event if found, None otherwise.
            A cancellation event is any event where event_type is
            'WorkflowCancelled' or contains 'cancel' (case-insensitive).
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
    # Timer Subscription Methods (for wait_timer)
    # -------------------------------------------------------------------------

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

    # -------------------------------------------------------------------------
    # Message Subscription Methods (for wait_message)
    # -------------------------------------------------------------------------

    async def find_waiting_instances_by_channel(
        self,
        channel: str,
    ) -> list[dict[str, Any]]:
        """
        Find workflow instances waiting on a specific channel.

        Called when a message arrives to find which workflows are waiting for it.

        Args:
            channel: Channel name

        Returns:
            List of waiting instances with subscription info.
            Each item contains: instance_id, channel, activity_id, timeout_at
        """
        ...

    async def remove_message_subscription(
        self,
        instance_id: str,
        channel: str,
    ) -> None:
        """
        Remove message subscription after the message is received.

        Args:
            instance_id: Workflow instance
            channel: Channel name
        """
        ...

    async def deliver_message(
        self,
        instance_id: str,
        channel: str,
        data: dict[str, Any] | bytes,
        metadata: dict[str, Any],
        worker_id: str | None = None,
    ) -> dict[str, Any] | None:
        """
        Deliver a message to a workflow instance waiting on a channel.

        Uses Lock-First pattern to prevent race conditions in distributed environments:
        1. Checks if instance is waiting on the channel
        2. Acquires lock (Lock-First pattern) - if worker_id provided
        3. Records message to history
        4. Removes subscription
        5. Updates status to 'running'
        6. Releases lock

        The workflow will be resumed by the caller or background task.

        Args:
            instance_id: Target workflow instance ID
            channel: Channel name
            data: Message payload (dict or bytes)
            metadata: Message metadata
            worker_id: Worker ID for locking. If None, skip locking (unsafe for distributed).

        Returns:
            Dict with delivery info if successful:
                {"instance_id": str, "workflow_name": str, "activity_id": str}
            None if message was not delivered (no subscription or lock failed)
        """
        ...

    async def find_expired_message_subscriptions(self) -> list[dict[str, Any]]:
        """
        Find message subscriptions that have timed out.

        Returns:
            List of expired subscriptions with instance_id, channel, activity_id,
            timeout_at, created_at
        """
        ...

    # -------------------------------------------------------------------------
    # Group Membership Methods (Erlang pg style)
    # -------------------------------------------------------------------------

    async def join_group(self, instance_id: str, group_name: str) -> None:
        """
        Add a workflow instance to a group.

        Groups provide loose coupling for message broadcasting.
        Senders don't need to know receiver instance IDs.

        Args:
            instance_id: Workflow instance to add
            group_name: Group name (e.g., "order_notifications")
        """
        ...

    async def leave_group(self, instance_id: str, group_name: str) -> None:
        """
        Remove a workflow instance from a group.

        Args:
            instance_id: Workflow instance to remove
            group_name: Group name
        """
        ...

    async def get_group_members(self, group_name: str) -> list[str]:
        """
        Get all instance IDs in a group.

        Args:
            group_name: Group name

        Returns:
            List of instance IDs that are members of the group
        """
        ...

    async def leave_all_groups(self, instance_id: str) -> None:
        """
        Remove a workflow instance from all groups.

        Called when a workflow completes or fails.

        Args:
            instance_id: Workflow instance to remove from all groups
        """
        ...

    # -------------------------------------------------------------------------
    # Workflow Resumption Methods
    # -------------------------------------------------------------------------

    async def find_resumable_workflows(self, limit: int | None = None) -> list[dict[str, Any]]:
        """
        Find workflows that are ready to be resumed.

        Returns workflows with status='running' that don't have an active lock.
        These are typically workflows that:
        - Had a message delivered (deliver_message sets status='running')
        - Had their lock released after message delivery
        - Haven't been picked up by auto_resume yet

        This allows immediate resumption after message delivery rather than
        waiting for the stale lock cleanup cycle (60+ seconds).

        Args:
            limit: Optional maximum number of workflows to return.
                   If None, returns all resumable workflows.

        Returns:
            List of resumable workflows.
            Each item contains: instance_id, workflow_name
        """
        ...

    # -------------------------------------------------------------------------
    # Subscription Cleanup Methods (for recur())
    # -------------------------------------------------------------------------

    async def cleanup_instance_subscriptions(self, instance_id: str) -> None:
        """
        Remove all subscriptions for a workflow instance.

        Called during recur() to clean up timer/message subscriptions
        before archiving the history. This prevents:
        - Message delivery to archived instances
        - Timer expiration for non-existent workflows

        Removes entries from:
        - workflow_timer_subscriptions
        - channel_subscriptions
        - channel_message_claims

        Args:
            instance_id: Workflow instance ID to clean up
        """
        ...

    # -------------------------------------------------------------------------
    # Channel-based Message Queue Methods
    # -------------------------------------------------------------------------

    async def publish_to_channel(
        self,
        channel: str,
        data: dict[str, Any] | bytes,
        metadata: dict[str, Any] | None = None,
    ) -> str:
        """
        Publish a message to a channel.

        Messages are persisted to the channel_messages table and will be
        available for subscribers to receive. This implements the "mailbox"
        pattern where messages are queued even before receive() is called.

        Args:
            channel: Channel name (e.g., "orders", "payment.completed")
            data: Message payload (dict or bytes)
            metadata: Optional message metadata

        Returns:
            Generated message_id (UUID)
        """
        ...

    async def subscribe_to_channel(
        self,
        instance_id: str,
        channel: str,
        mode: str,
    ) -> None:
        """
        Subscribe a workflow instance to a channel.

        Args:
            instance_id: Workflow instance ID
            channel: Channel name
            mode: Subscription mode ('broadcast' or 'competing')
                  - broadcast: All subscribers receive all messages
                  - competing: Each message is received by only one subscriber

        Raises:
            ValueError: If mode is not 'broadcast' or 'competing'
        """
        ...

    async def unsubscribe_from_channel(
        self,
        instance_id: str,
        channel: str,
    ) -> None:
        """
        Unsubscribe a workflow instance from a channel.

        Args:
            instance_id: Workflow instance ID
            channel: Channel name
        """
        ...

    async def get_channel_subscription(
        self,
        instance_id: str,
        channel: str,
    ) -> dict[str, Any] | None:
        """
        Get the subscription info for a workflow instance on a channel.

        Args:
            instance_id: Workflow instance ID
            channel: Channel name

        Returns:
            Subscription info dict with: mode, activity_id, cursor_message_id
            or None if not subscribed
        """
        ...

    async def get_channel_mode(self, channel: str) -> str | None:
        """
        Get the mode for a channel (from any existing subscription).

        Args:
            channel: Channel name

        Returns:
            The mode ('broadcast' or 'competing') or None if no subscriptions exist
        """
        ...

    async def register_channel_receive_and_release_lock(
        self,
        instance_id: str,
        worker_id: str,
        channel: str,
        activity_id: str | None = None,
        timeout_seconds: int | None = None,
    ) -> None:
        """
        Atomically register that workflow is waiting for channel message and release lock.

        This method performs the following operations in a SINGLE database transaction:
        1. Update channel_subscriptions to set activity_id and timeout_at (waiting state)
        2. Update current activity (UPDATE workflow_instances.current_activity_id)
        3. Update status to 'waiting_for_message'
        4. Release lock (UPDATE workflow_instances set locked_by=NULL)

        Args:
            instance_id: Workflow instance ID
            worker_id: Worker ID that currently holds the lock
            channel: Channel name being waited on
            activity_id: Current activity ID to record
            timeout_seconds: Optional timeout in seconds for the message wait

        Raises:
            RuntimeError: If the worker doesn't hold the lock
            ValueError: If workflow is not subscribed to the channel
        """
        ...

    async def get_pending_channel_messages(
        self,
        instance_id: str,
        channel: str,
    ) -> list[dict[str, Any]]:
        """
        Get pending messages for a subscriber on a channel.

        For broadcast mode:
            Returns messages with id > cursor_message_id (messages not yet seen)

        For competing mode:
            Returns unclaimed messages (not in channel_message_claims)

        Args:
            instance_id: Workflow instance ID
            channel: Channel name

        Returns:
            List of pending messages, ordered by published_at ASC.
            Each message contains: id, message_id, channel, data, metadata, published_at
        """
        ...

    async def claim_channel_message(
        self,
        message_id: str,
        instance_id: str,
    ) -> bool:
        """
        Claim a message for competing consumption.

        Uses SELECT FOR UPDATE SKIP LOCKED pattern to ensure only one
        subscriber claims each message.

        Args:
            message_id: Message ID to claim
            instance_id: Workflow instance claiming the message

        Returns:
            True if claim succeeded, False if already claimed by another instance
        """
        ...

    async def delete_channel_message(self, message_id: str) -> None:
        """
        Delete a message from the channel queue.

        Called after successful message processing in competing mode.

        Args:
            message_id: Message ID to delete
        """
        ...

    async def update_delivery_cursor(
        self,
        channel: str,
        instance_id: str,
        message_id: int,
    ) -> None:
        """
        Update the delivery cursor for broadcast mode.

        Records the last message ID delivered to a subscriber, so the same
        messages are not delivered again.

        Args:
            channel: Channel name
            instance_id: Subscriber instance ID
            message_id: Last delivered message's internal ID (channel_messages.id)
        """
        ...

    async def get_channel_subscribers_waiting(
        self,
        channel: str,
    ) -> list[dict[str, Any]]:
        """
        Get channel subscribers that are waiting (activity_id is set).

        Called when a message is published to find subscribers to wake up.

        Args:
            channel: Channel name

        Returns:
            List of waiting subscribers.
            Each item contains: instance_id, channel, mode, activity_id
        """
        ...

    async def clear_channel_waiting_state(
        self,
        instance_id: str,
        channel: str,
    ) -> None:
        """
        Clear the waiting state for a channel subscription.

        Called after a message is delivered to a waiting subscriber.

        Args:
            instance_id: Workflow instance ID
            channel: Channel name
        """
        ...

    async def deliver_channel_message(
        self,
        instance_id: str,
        channel: str,
        message_id: str,
        data: dict[str, Any] | bytes,
        metadata: dict[str, Any],
        worker_id: str,
    ) -> dict[str, Any] | None:
        """
        Deliver a channel message to a waiting workflow.

        Uses Lock-First pattern:
        1. Acquire lock on the workflow instance
        2. Record message to history
        3. Clear waiting state / update cursor / claim message
        4. Update status to 'running'
        5. Release lock

        Args:
            instance_id: Target workflow instance ID
            channel: Channel name
            message_id: Message ID being delivered
            data: Message payload
            metadata: Message metadata
            worker_id: Worker ID for locking

        Returns:
            Dict with delivery info if successful:
                {"instance_id": str, "workflow_name": str, "activity_id": str}
            None if delivery failed (lock conflict, etc.)
        """
        ...

    async def cleanup_old_channel_messages(self, older_than_days: int = 7) -> int:
        """
        Clean up old messages from channel queues.

        For broadcast mode: Delete messages where all current subscribers have
        received them (cursor is past the message).

        For all modes: Delete messages older than the retention period.

        Args:
            older_than_days: Message retention period in days

        Returns:
            Number of messages deleted
        """
        ...
