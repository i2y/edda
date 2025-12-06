"""
Tests for atomic wait_event transaction.

These tests verify that channel subscription registration and lock release
happen atomically in a single database transaction.

Note: CloudEvents (wait_event) internally uses Channel-based Message Queue (receive),
so these tests verify the underlying channel subscription atomic behavior.
"""

import uuid

import pytest

from edda import workflow
from edda.channels import wait_event
from edda.context import WorkflowContext
from edda.replay import ReplayEngine

# Use sqlite_storage and create_test_instance from conftest.py


class TestAtomicWaitEvent:
    """Test atomic transaction for wait_event() operations."""

    @pytest.mark.asyncio
    async def test_atomic_method_registers_subscription_and_releases_lock(
        self, sqlite_storage, create_test_instance
    ):
        """Test that atomic method performs all operations in single transaction."""

        # Create a workflow instance
        instance_id = f"test-atomic-{uuid.uuid4().hex[:8]}"
        await create_test_instance(
            instance_id=instance_id,
            workflow_name="test_workflow",
            input_data={"test": "data"},
        )

        # Acquire lock
        acquired = await sqlite_storage.try_acquire_lock(
            instance_id, "worker-1", timeout_seconds=30
        )
        assert acquired is True

        # Verify lock is held
        instance = await sqlite_storage.get_instance(instance_id)
        assert instance["locked_by"] == "worker-1"

        # First subscribe to channel, then call atomic method
        await sqlite_storage.subscribe_to_channel(
            instance_id=instance_id,
            channel="test.event",
            mode="broadcast",
        )

        # Call atomic method (registers receive and releases lock atomically)
        await sqlite_storage.register_channel_receive_and_release_lock(
            instance_id=instance_id,
            worker_id="worker-1",
            channel="test.event",
            activity_id="receive_test.event:1",
        )

        # Verify subscription was registered
        subscriptions = await sqlite_storage.get_channel_subscribers_waiting("test.event")
        assert len(subscriptions) == 1
        assert subscriptions[0]["instance_id"] == instance_id

        # Verify lock was released
        instance = await sqlite_storage.get_instance(instance_id)
        assert instance["locked_by"] is None
        assert instance["locked_at"] is None

        # Verify activity_id was updated
        assert instance["current_activity_id"] == "receive_test.event:1"

    @pytest.mark.asyncio
    async def test_atomic_method_fails_if_worker_doesnt_hold_lock(
        self, sqlite_storage, create_test_instance
    ):
        """Test that atomic method fails if worker doesn't hold the lock."""

        # Create instance
        instance_id = f"test-nolock-{uuid.uuid4().hex[:8]}"
        await create_test_instance(
            instance_id=instance_id,
            workflow_name="test_workflow",
            input_data={},
        )

        # Acquire lock with worker-1
        await sqlite_storage.try_acquire_lock(instance_id, "worker-1", timeout_seconds=30)

        # Subscribe to channel first
        await sqlite_storage.subscribe_to_channel(
            instance_id=instance_id,
            channel="test.event",
            mode="broadcast",
        )

        # Try to use atomic method with worker-2 (should fail)
        with pytest.raises(RuntimeError, match="does not hold lock"):
            await sqlite_storage.register_channel_receive_and_release_lock(
                instance_id=instance_id,
                worker_id="worker-2",
                channel="test.event",
                activity_id="receive_test.event:1",
            )

        # Verify activity_id was NOT updated (transaction rolled back)
        # The subscription still exists but no activity_id is set
        subscriptions = await sqlite_storage.get_channel_subscribers_waiting("test.event")
        assert len(subscriptions) == 0  # Not waiting (no activity_id set)

        # Verify lock is still held by worker-1
        instance = await sqlite_storage.get_instance(instance_id)
        assert instance["locked_by"] == "worker-1"

    @pytest.mark.asyncio
    async def test_workflow_uses_atomic_method_on_wait_event(
        self, sqlite_storage, create_test_instance
    ):
        """Test that workflow execution uses atomic method when catching WaitForEventException."""

        @workflow
        async def event_waiting_workflow(ctx: WorkflowContext, order_id: str):
            # Wait for event (should trigger atomic operation)
            received_event = await wait_event(ctx, event_type="payment.received")
            return {"payment": received_event.data}

        engine = ReplayEngine(
            storage=sqlite_storage, service_name="test-service", worker_id="worker-1"
        )

        # Start workflow
        instance_id = await engine.start_workflow(
            workflow_func=event_waiting_workflow,
            workflow_name="event_waiting_workflow",
            input_data={"order_id": "order-123"},
        )

        # Verify workflow is waiting
        instance = await sqlite_storage.get_instance(instance_id)
        assert instance["status"] == "waiting_for_message"

        # Verify lock was released (atomic operation succeeded)
        assert instance["locked_by"] is None

        # Verify subscription was registered (CloudEvents uses channel subscriptions)
        subscriptions = await sqlite_storage.get_channel_subscribers_waiting("payment.received")
        assert len(subscriptions) == 1
        assert subscriptions[0]["instance_id"] == instance_id

        # Verify activity_id was updated
        assert instance["current_activity_id"] == "receive_payment.received:1"

    @pytest.mark.asyncio
    async def test_atomic_method_sets_activity_id(self, sqlite_storage, create_test_instance):
        """Test atomic method correctly sets activity_id for waiting state."""
        instance_id = f"test-activity-id-{uuid.uuid4().hex[:8]}"
        await create_test_instance(
            instance_id=instance_id,
            workflow_name="test_workflow",
            input_data={},
        )

        await sqlite_storage.try_acquire_lock(instance_id, "worker-1", timeout_seconds=30)

        # Subscribe to channel first
        await sqlite_storage.subscribe_to_channel(
            instance_id=instance_id,
            channel="test.event",
            mode="broadcast",
        )

        # Use atomic method
        await sqlite_storage.register_channel_receive_and_release_lock(
            instance_id=instance_id,
            worker_id="worker-1",
            channel="test.event",
            activity_id="receive_test.event:1",
        )

        # Verify subscription has activity_id set (waiting state)
        subscriptions = await sqlite_storage.get_channel_subscribers_waiting("test.event")
        assert len(subscriptions) == 1
        assert subscriptions[0]["activity_id"] == "receive_test.event:1"

    @pytest.mark.asyncio
    async def test_resume_workflow_uses_atomic_method(self, sqlite_storage, create_test_instance):
        """Test that resuming a workflow also uses atomic method for subsequent wait_event."""

        @workflow
        async def multi_wait_workflow(ctx: WorkflowContext):
            # First wait
            received_event1 = await wait_event(ctx, event_type="event.first")

            # Second wait
            received_event2 = await wait_event(ctx, event_type="event.second")

            return {"event1": received_event1.data, "event2": received_event2.data}

        engine = ReplayEngine(
            storage=sqlite_storage, service_name="test-service", worker_id="worker-1"
        )

        # Start workflow (will wait for first event)
        instance_id = await engine.start_workflow(
            workflow_func=multi_wait_workflow,
            workflow_name="multi_wait_workflow",
            input_data={},
        )

        # Verify waiting for first event
        instance = await sqlite_storage.get_instance(instance_id)
        assert instance["status"] == "waiting_for_message"
        assert instance["locked_by"] is None  # Lock released atomically

        subscriptions = await sqlite_storage.get_channel_subscribers_waiting("event.first")
        assert len(subscriptions) == 1

        # Manually deliver first event and resume (using channel subscription API)
        await sqlite_storage.append_history(
            instance_id,
            activity_id="receive_event.first:1",
            event_type="ChannelMessageReceived",
            event_data={
                "data": {"data": "test1"},
                "channel": "event.first",
                "id": "test-msg-1",
                "metadata": {},
            },
        )
        await sqlite_storage.unsubscribe_from_channel(instance_id, "event.first")

        # Resume workflow (will wait for second event)
        await engine.resume_workflow(instance_id=instance_id, workflow_func=multi_wait_workflow)

        # Verify waiting for second event with lock released
        instance = await sqlite_storage.get_instance(instance_id)
        assert instance["status"] == "waiting_for_message"
        assert instance["locked_by"] is None  # Lock released atomically on second wait

        subscriptions = await sqlite_storage.get_channel_subscribers_waiting("event.second")
        assert len(subscriptions) == 1

    @pytest.mark.asyncio
    async def test_atomic_rollback_on_error(self, sqlite_storage, create_test_instance):
        """Test that transaction rolls back if any operation fails."""

        instance_id = f"test-rollback-{uuid.uuid4().hex[:8]}"
        await create_test_instance(
            instance_id=instance_id,
            workflow_name="test_workflow",
            input_data={},
        )

        await sqlite_storage.try_acquire_lock(instance_id, "worker-1", timeout_seconds=30)

        # Subscribe to channel first
        await sqlite_storage.subscribe_to_channel(
            instance_id=instance_id,
            channel="test.event",
            mode="broadcast",
        )

        # Force an error by using invalid worker_id for atomic operation
        # (This is a bit artificial, but demonstrates rollback behavior)
        try:
            # We can't easily force a partial failure, but we can verify
            # that if the lock check fails, nothing else happens
            with pytest.raises(RuntimeError):
                await sqlite_storage.register_channel_receive_and_release_lock(
                    instance_id=instance_id,
                    worker_id="wrong-worker",  # This should cause lock check to fail
                    channel="test.event",
                    activity_id="receive_test.event:1",
                )
        except Exception:
            pass

        # Verify nothing was committed (lock still held, activity_id not set)
        instance = await sqlite_storage.get_instance(instance_id)
        assert instance["locked_by"] == "worker-1"  # Lock not released

        subscriptions = await sqlite_storage.get_channel_subscribers_waiting("test.event")
        assert len(subscriptions) == 0  # No waiting subscription (activity_id not set)
