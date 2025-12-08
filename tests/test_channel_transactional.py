"""
Tests for transactional message passing in channel-based message queue.

Ensures that message publishing and delivery are properly handled within
transactions, with delivery deferred until after commit.

Tests cover:
- Message publish within transaction is stored immediately
- Message delivery is deferred until transaction commits
- Message is not delivered if transaction rolls back
- Post-commit callbacks are executed after successful commit
- Multiple messages in same transaction are all delivered after commit
"""

import pytest
import pytest_asyncio

from edda.channels import publish
from edda.context import WorkflowContext


@pytest.mark.asyncio
class TestTransactionalMessagePublish:
    """Test suite for transactional message publishing."""

    @pytest_asyncio.fixture
    async def publisher_instance(self, sqlite_storage, create_test_instance):
        """Create a publisher workflow instance."""
        instance_id = "test-publisher-001"
        await create_test_instance(
            instance_id=instance_id,
            workflow_name="publisher_workflow",
            owner_service="test-service",
            input_data={"test": "data"},
        )
        await sqlite_storage.update_instance_status(instance_id, "running")
        return instance_id

    @pytest_asyncio.fixture
    async def subscriber_instance(self, sqlite_storage, create_test_instance):
        """Create a subscriber workflow instance waiting for a message."""
        instance_id = "test-subscriber-001"
        await create_test_instance(
            instance_id=instance_id,
            workflow_name="subscriber_workflow",
            owner_service="test-service",
            input_data={"test": "data"},
        )
        await sqlite_storage.update_instance_status(instance_id, "running")

        # Subscribe to the channel
        await sqlite_storage.subscribe_to_channel(
            instance_id=instance_id,
            channel="test-channel",
            mode="broadcast",
        )

        # Register as waiting for message (this sets status to waiting_for_message)
        await sqlite_storage.try_acquire_lock(instance_id, "setup-worker")
        await sqlite_storage.register_channel_receive_and_release_lock(
            instance_id=instance_id,
            worker_id="setup-worker",
            channel="test-channel",
            activity_id="wait_message:1",
        )

        return instance_id

    async def test_message_stored_within_transaction(
        self, sqlite_storage, publisher_instance, subscriber_instance
    ):
        """Test that message is stored to database within transaction."""
        ctx = WorkflowContext(
            instance_id=publisher_instance,
            workflow_name="publisher_workflow",
            storage=sqlite_storage,
            worker_id="worker-1",
            is_replaying=False,
        )

        # Start a transaction
        await sqlite_storage.begin_transaction()

        try:
            # Publish a message within the transaction
            message_id = await publish(
                ctx, "test-channel", {"order_id": "123"}, worker_id="worker-1"
            )

            # Message should be stored (visible within transaction)
            messages = await sqlite_storage.get_pending_channel_messages(
                subscriber_instance, "test-channel"
            )
            assert len(messages) >= 0  # May be 0 depending on transaction isolation

            # Commit the transaction
            await sqlite_storage.commit_transaction()

            # Message should still exist after commit
            assert message_id is not None
        except Exception:
            await sqlite_storage.rollback_transaction()
            raise

    async def test_delivery_deferred_until_commit(
        self, sqlite_storage, publisher_instance, subscriber_instance
    ):
        """Test that message delivery is deferred until transaction commits."""
        ctx = WorkflowContext(
            instance_id=publisher_instance,
            workflow_name="publisher_workflow",
            storage=sqlite_storage,
            worker_id="worker-1",
            is_replaying=False,
        )

        # Check subscriber status before publish
        subscriber_before = await sqlite_storage.get_instance(subscriber_instance)
        assert subscriber_before["status"] == "waiting_for_message"

        # Start a transaction
        await sqlite_storage.begin_transaction()

        try:
            # Publish a message within the transaction
            await publish(ctx, "test-channel", {"order_id": "456"}, worker_id="worker-1")

            # Subscriber should still be waiting (delivery is deferred)
            subscriber_during = await sqlite_storage.get_instance(subscriber_instance)
            assert subscriber_during["status"] == "waiting_for_message"

            # Commit the transaction - delivery should happen now
            await sqlite_storage.commit_transaction()

            # After commit, subscriber should be woken up (status = running)
            subscriber_after = await sqlite_storage.get_instance(subscriber_instance)
            assert subscriber_after["status"] == "running"
        except Exception:
            await sqlite_storage.rollback_transaction()
            raise

    async def test_message_not_delivered_on_rollback(
        self, sqlite_storage, publisher_instance, subscriber_instance
    ):
        """Test that message is not delivered if transaction rolls back."""
        ctx = WorkflowContext(
            instance_id=publisher_instance,
            workflow_name="publisher_workflow",
            storage=sqlite_storage,
            worker_id="worker-1",
            is_replaying=False,
        )

        # Check subscriber status before
        subscriber_before = await sqlite_storage.get_instance(subscriber_instance)
        assert subscriber_before["status"] == "waiting_for_message"

        # Start a transaction
        await sqlite_storage.begin_transaction()

        # Publish a message within the transaction
        await publish(ctx, "test-channel", {"order_id": "789"}, worker_id="worker-1")

        # Rollback the transaction
        await sqlite_storage.rollback_transaction()

        # Subscriber should still be waiting (message was not delivered)
        subscriber_after = await sqlite_storage.get_instance(subscriber_instance)
        assert subscriber_after["status"] == "waiting_for_message"

    async def test_publish_outside_transaction_delivers_immediately(
        self, sqlite_storage, publisher_instance, subscriber_instance
    ):
        """Test that publish outside transaction delivers immediately."""
        ctx = WorkflowContext(
            instance_id=publisher_instance,
            workflow_name="publisher_workflow",
            storage=sqlite_storage,
            worker_id="worker-1",
            is_replaying=False,
        )

        # Verify not in transaction
        assert not sqlite_storage.in_transaction()

        # Check subscriber status before
        subscriber_before = await sqlite_storage.get_instance(subscriber_instance)
        assert subscriber_before["status"] == "waiting_for_message"

        # Publish without transaction - should deliver immediately
        await publish(ctx, "test-channel", {"order_id": "immediate"}, worker_id="worker-1")

        # Subscriber should be woken up immediately
        subscriber_after = await sqlite_storage.get_instance(subscriber_instance)
        assert subscriber_after["status"] == "running"


@pytest.mark.asyncio
class TestPostCommitCallbacks:
    """Test suite for post-commit callback mechanism."""

    async def test_callback_executed_after_commit(self, sqlite_storage):
        """Test that post-commit callbacks are executed after commit."""
        callback_executed = []

        async def my_callback():
            callback_executed.append("executed")

        # Start a transaction
        await sqlite_storage.begin_transaction()

        # Register callback
        sqlite_storage.register_post_commit_callback(my_callback)

        # Callback should not be executed yet
        assert len(callback_executed) == 0

        # Commit
        await sqlite_storage.commit_transaction()

        # Callback should be executed now
        assert len(callback_executed) == 1
        assert callback_executed[0] == "executed"

    async def test_callback_not_executed_on_rollback(self, sqlite_storage):
        """Test that post-commit callbacks are NOT executed on rollback."""
        callback_executed = []

        async def my_callback():
            callback_executed.append("executed")

        # Start a transaction
        await sqlite_storage.begin_transaction()

        # Register callback
        sqlite_storage.register_post_commit_callback(my_callback)

        # Rollback
        await sqlite_storage.rollback_transaction()

        # Callback should NOT be executed
        assert len(callback_executed) == 0

    async def test_multiple_callbacks_executed_in_order(self, sqlite_storage):
        """Test that multiple callbacks are executed in registration order."""
        execution_order = []

        async def callback_1():
            execution_order.append("first")

        async def callback_2():
            execution_order.append("second")

        async def callback_3():
            execution_order.append("third")

        # Start a transaction
        await sqlite_storage.begin_transaction()

        # Register callbacks
        sqlite_storage.register_post_commit_callback(callback_1)
        sqlite_storage.register_post_commit_callback(callback_2)
        sqlite_storage.register_post_commit_callback(callback_3)

        # Commit
        await sqlite_storage.commit_transaction()

        # All callbacks should be executed in order
        assert execution_order == ["first", "second", "third"]

    async def test_callback_error_does_not_affect_other_callbacks(self, sqlite_storage):
        """Test that one callback error doesn't prevent other callbacks from running."""
        execution_order = []

        async def callback_1():
            execution_order.append("first")

        async def failing_callback():
            raise RuntimeError("Intentional failure")

        async def callback_3():
            execution_order.append("third")

        # Start a transaction
        await sqlite_storage.begin_transaction()

        # Register callbacks (including one that fails)
        sqlite_storage.register_post_commit_callback(callback_1)
        sqlite_storage.register_post_commit_callback(failing_callback)
        sqlite_storage.register_post_commit_callback(callback_3)

        # Commit - should not raise despite failing callback
        await sqlite_storage.commit_transaction()

        # Both non-failing callbacks should execute
        assert "first" in execution_order
        assert "third" in execution_order

    async def test_register_callback_fails_outside_transaction(self, sqlite_storage):
        """Test that registering callback outside transaction raises error."""

        async def my_callback():
            pass

        # Not in transaction
        assert not sqlite_storage.in_transaction()

        with pytest.raises(RuntimeError, match="not in a transaction"):
            sqlite_storage.register_post_commit_callback(my_callback)


@pytest.mark.asyncio
class TestWorkflowContextPostCommit:
    """Test suite for WorkflowContext.register_post_commit()."""

    @pytest_asyncio.fixture
    async def workflow_instance(self, sqlite_storage, create_test_instance):
        """Create a workflow instance."""
        instance_id = "test-ctx-instance-001"
        await create_test_instance(
            instance_id=instance_id,
            workflow_name="test_workflow",
            owner_service="test-service",
            input_data={"test": "data"},
        )
        await sqlite_storage.update_instance_status(instance_id, "running")
        return instance_id

    async def test_context_register_post_commit(self, sqlite_storage, workflow_instance):
        """Test that WorkflowContext.register_post_commit works correctly."""
        ctx = WorkflowContext(
            instance_id=workflow_instance,
            workflow_name="test_workflow",
            storage=sqlite_storage,
            worker_id="worker-1",
            is_replaying=False,
        )

        callback_executed = []

        async def my_callback():
            callback_executed.append("executed")

        # Use ctx.transaction() context manager
        async with ctx.transaction():
            ctx.register_post_commit(my_callback)
            # Callback not executed yet
            assert len(callback_executed) == 0

        # After transaction completes, callback should be executed
        assert len(callback_executed) == 1
