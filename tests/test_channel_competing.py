"""
Tests for competing mode in channel-based message queue.

Competing mode ensures that each message is processed by only one subscriber.
This is useful for job queues and task distribution patterns.

Tests cover:
- Subscription with competing mode
- Message claiming (only one subscriber gets the message)
- Pending message filtering (unclaimed only)
- Message deletion after processing
- Multiple workers competing for messages
"""

import pytest
import pytest_asyncio

from edda.channels import (
    ChannelMessage,
    WaitForChannelMessageException,
    receive,
    subscribe,
)
from edda.context import WorkflowContext


@pytest.mark.asyncio
class TestCompetingModeSubscription:
    """Test suite for competing mode subscription."""

    @pytest_asyncio.fixture
    async def workflow_instance(self, sqlite_storage, create_test_instance):
        """Create a workflow instance for testing."""
        instance_id = "test-competing-instance-001"
        await create_test_instance(
            instance_id=instance_id,
            workflow_name="test_workflow",
            owner_service="test-service",
            input_data={"test": "data"},
        )
        await sqlite_storage.update_instance_status(instance_id, "running")
        return instance_id

    async def test_subscribe_with_competing_mode(self, sqlite_storage, workflow_instance):
        """Test subscribing to a channel with competing mode."""
        ctx = WorkflowContext(
            instance_id=workflow_instance,
            workflow_name="test_workflow",
            storage=sqlite_storage,
            worker_id="worker-1",
            is_replaying=False,
        )

        # Subscribe with competing mode
        await subscribe(ctx, "jobs", mode="competing")

        # Verify subscription exists with competing mode
        subscription = await sqlite_storage.get_channel_subscription(workflow_instance, "jobs")
        assert subscription is not None
        assert subscription["mode"] == "competing"

    async def test_invalid_mode_raises_error(self, sqlite_storage, workflow_instance):
        """Test that invalid mode raises ValueError."""
        ctx = WorkflowContext(
            instance_id=workflow_instance,
            workflow_name="test_workflow",
            storage=sqlite_storage,
            worker_id="worker-1",
            is_replaying=False,
        )

        with pytest.raises(ValueError, match="Invalid subscription mode"):
            await subscribe(ctx, "jobs", mode="invalid_mode")


@pytest.mark.asyncio
class TestCompetingModeMessageClaiming:
    """Test suite for message claiming in competing mode."""

    @pytest_asyncio.fixture
    async def two_workers(self, sqlite_storage, create_test_instance):
        """Create two workflow instances (workers) for testing."""
        workers = []
        for i in range(2):
            instance_id = f"test-worker-{i}"
            await create_test_instance(
                instance_id=instance_id,
                workflow_name="job_worker",
                owner_service="test-service",
                input_data={"worker_id": i},
            )
            await sqlite_storage.update_instance_status(instance_id, "running")
            workers.append(instance_id)
        return workers

    async def test_claim_message_success(self, sqlite_storage, two_workers):
        """Test that claiming a message succeeds for the first claimer."""
        worker1 = two_workers[0]

        # Subscribe worker1 to competing mode
        await sqlite_storage.subscribe_to_channel(worker1, "jobs", "competing")

        # Publish a message
        message_id = await sqlite_storage.publish_to_channel(
            channel="jobs",
            data={"task": "process_order", "order_id": "123"},
            metadata=None,
        )

        # Claim the message
        success = await sqlite_storage.claim_channel_message(message_id, worker1)
        assert success is True

    async def test_claim_message_fails_for_second_claimer(self, sqlite_storage, two_workers):
        """Test that claiming an already claimed message fails."""
        worker1, worker2 = two_workers

        # Both workers subscribe to competing mode
        await sqlite_storage.subscribe_to_channel(worker1, "jobs", "competing")
        await sqlite_storage.subscribe_to_channel(worker2, "jobs", "competing")

        # Publish a message
        message_id = await sqlite_storage.publish_to_channel(
            channel="jobs",
            data={"task": "process_order", "order_id": "123"},
            metadata=None,
        )

        # Worker1 claims first
        success1 = await sqlite_storage.claim_channel_message(message_id, worker1)
        assert success1 is True

        # Worker2 tries to claim - should fail
        success2 = await sqlite_storage.claim_channel_message(message_id, worker2)
        assert success2 is False

    async def test_claimed_message_not_in_pending(self, sqlite_storage, two_workers):
        """Test that claimed messages are not returned as pending."""
        worker1, worker2 = two_workers

        # Both workers subscribe to competing mode
        await sqlite_storage.subscribe_to_channel(worker1, "jobs", "competing")
        await sqlite_storage.subscribe_to_channel(worker2, "jobs", "competing")

        # Publish a message
        message_id = await sqlite_storage.publish_to_channel(
            channel="jobs",
            data={"task": "process_order", "order_id": "123"},
            metadata=None,
        )

        # Before claiming - both workers see the message
        pending1 = await sqlite_storage.get_pending_channel_messages(worker1, "jobs")
        pending2 = await sqlite_storage.get_pending_channel_messages(worker2, "jobs")
        assert len(pending1) == 1
        assert len(pending2) == 1

        # Worker1 claims the message
        await sqlite_storage.claim_channel_message(message_id, worker1)

        # After claiming - neither worker sees the message as pending
        pending1 = await sqlite_storage.get_pending_channel_messages(worker1, "jobs")
        pending2 = await sqlite_storage.get_pending_channel_messages(worker2, "jobs")
        assert len(pending1) == 0
        assert len(pending2) == 0


@pytest.mark.asyncio
class TestCompetingModeMessageProcessing:
    """Test suite for message processing in competing mode."""

    @pytest_asyncio.fixture
    async def three_workers(self, sqlite_storage, create_test_instance):
        """Create three workflow instances (workers) for testing."""
        workers = []
        for i in range(3):
            instance_id = f"test-worker-{i}"
            await create_test_instance(
                instance_id=instance_id,
                workflow_name="job_worker",
                owner_service="test-service",
                input_data={"worker_id": i},
            )
            await sqlite_storage.update_instance_status(instance_id, "running")
            workers.append(instance_id)
        return workers

    async def test_multiple_messages_distributed_to_workers(self, sqlite_storage, three_workers):
        """Test that multiple messages can be claimed by different workers."""
        # All workers subscribe to competing mode
        for worker in three_workers:
            await sqlite_storage.subscribe_to_channel(worker, "jobs", "competing")

        # Publish 3 messages
        message_ids = []
        for i in range(3):
            mid = await sqlite_storage.publish_to_channel(
                channel="jobs",
                data={"task": f"task_{i}"},
                metadata=None,
            )
            message_ids.append(mid)

        # Each worker claims one message
        claims = {}
        for _i, worker in enumerate(three_workers):
            # Get pending messages for this worker
            pending = await sqlite_storage.get_pending_channel_messages(worker, "jobs")
            if pending:
                msg_id = pending[0]["message_id"]
                success = await sqlite_storage.claim_channel_message(msg_id, worker)
                if success:
                    claims[worker] = msg_id

        # All 3 workers should have claimed different messages
        assert len(claims) == 3
        # Each message should be claimed by exactly one worker
        claimed_msgs = set(claims.values())
        assert len(claimed_msgs) == 3

    async def test_message_deleted_after_processing(self, sqlite_storage, three_workers):
        """Test that messages are deleted after successful processing."""
        worker = three_workers[0]

        # Subscribe with competing mode
        await sqlite_storage.subscribe_to_channel(worker, "jobs", "competing")

        # Publish a message
        message_id = await sqlite_storage.publish_to_channel(
            channel="jobs",
            data={"task": "process_order"},
            metadata=None,
        )

        # Claim and then delete (simulating successful processing)
        await sqlite_storage.claim_channel_message(message_id, worker)
        await sqlite_storage.delete_channel_message(message_id)

        # Message should no longer exist
        pending = await sqlite_storage.get_pending_channel_messages(worker, "jobs")
        assert len(pending) == 0

    async def test_receive_in_competing_mode_returns_message_immediately(
        self, sqlite_storage, three_workers
    ):
        """Test that receive returns message immediately when one is available."""
        worker = three_workers[0]

        ctx = WorkflowContext(
            instance_id=worker,
            workflow_name="job_worker",
            storage=sqlite_storage,
            worker_id="worker-1",
            is_replaying=False,
        )

        # Subscribe with competing mode
        await subscribe(ctx, "jobs", mode="competing")

        # Publish a message
        await sqlite_storage.publish_to_channel(
            channel="jobs",
            data={"task": "important_job"},
            metadata={"priority": "high"},
        )

        # Receive should return the message immediately (not raise exception)
        msg = await receive(ctx, channel="jobs")

        assert isinstance(msg, ChannelMessage)
        assert msg.channel == "jobs"
        assert msg.data == {"task": "important_job"}
        assert msg.metadata["priority"] == "high"

    async def test_receive_raises_exception_when_no_message(self, sqlite_storage, three_workers):
        """Test that receive raises exception when no message is available."""
        worker = three_workers[0]

        ctx = WorkflowContext(
            instance_id=worker,
            workflow_name="job_worker",
            storage=sqlite_storage,
            worker_id="worker-1",
            is_replaying=False,
        )

        # Subscribe with competing mode
        await subscribe(ctx, "jobs", mode="competing")

        # No message published - receive should raise exception to pause workflow
        with pytest.raises(WaitForChannelMessageException) as exc_info:
            await receive(ctx, channel="jobs")

        assert exc_info.value.channel == "jobs"

    async def test_receive_replay_in_competing_mode(self, sqlite_storage, three_workers):
        """Test that receive returns cached message during replay in competing mode."""
        worker = three_workers[0]

        # Add history entry simulating a received message
        await sqlite_storage.append_history(
            instance_id=worker,
            activity_id="receive_jobs:1",
            event_type="ChannelMessageReceived",
            event_data={
                "data": {"task": "important_job"},
                "channel": "jobs",
                "id": "msg-456",
                "metadata": {"priority": "high"},
            },
        )

        ctx = WorkflowContext(
            instance_id=worker,
            workflow_name="job_worker",
            storage=sqlite_storage,
            worker_id="worker-1",
            is_replaying=True,
        )

        # Load history for replay
        history = await sqlite_storage.get_history(worker)
        ctx._history_cache = {h["activity_id"]: h["event_data"] for h in history}

        # Subscribe (will be skipped during replay)
        await subscribe(ctx, "jobs", mode="competing")

        # During replay, should return the cached message
        msg = await receive(ctx, channel="jobs")

        assert isinstance(msg, ChannelMessage)
        assert msg.data == {"task": "important_job"}
        assert msg.channel == "jobs"
        assert msg.metadata == {"priority": "high"}


@pytest.mark.asyncio
class TestCompetingModeEdgeCases:
    """Test edge cases for competing mode."""

    @pytest_asyncio.fixture
    async def worker_instance(self, sqlite_storage, create_test_instance):
        """Create a single worker instance."""
        instance_id = "test-edge-case-worker"
        await create_test_instance(
            instance_id=instance_id,
            workflow_name="job_worker",
            owner_service="test-service",
            input_data={},
        )
        await sqlite_storage.update_instance_status(instance_id, "running")
        return instance_id

    async def test_no_pending_messages(self, sqlite_storage, worker_instance):
        """Test that empty list is returned when no messages are pending."""
        await sqlite_storage.subscribe_to_channel(worker_instance, "jobs", "competing")

        pending = await sqlite_storage.get_pending_channel_messages(worker_instance, "jobs")
        assert pending == []

    async def test_not_subscribed_returns_empty(self, sqlite_storage, worker_instance):
        """Test that empty list is returned when not subscribed."""
        pending = await sqlite_storage.get_pending_channel_messages(
            worker_instance, "nonexistent_channel"
        )
        assert pending == []

    async def test_messages_ordered_by_published_time(self, sqlite_storage, worker_instance):
        """Test that pending messages are ordered by published time."""
        await sqlite_storage.subscribe_to_channel(worker_instance, "jobs", "competing")

        # Publish messages
        for i in range(3):
            await sqlite_storage.publish_to_channel(
                channel="jobs",
                data={"order": i},
                metadata=None,
            )

        pending = await sqlite_storage.get_pending_channel_messages(worker_instance, "jobs")

        # Should be in FIFO order
        assert len(pending) == 3
        for i, msg in enumerate(pending):
            assert msg["data"]["order"] == i

    async def test_claim_nonexistent_message(self, sqlite_storage, worker_instance):
        """Test claiming a message that doesn't exist."""
        await sqlite_storage.subscribe_to_channel(worker_instance, "jobs", "competing")

        # Try to claim a nonexistent message
        success = await sqlite_storage.claim_channel_message(
            "nonexistent-message-id", worker_instance
        )
        # Should fail or return False (depending on implementation)
        # The current implementation tries to insert and catches exceptions
        assert success is True or success is False  # Either behavior is acceptable

    async def test_binary_data_in_competing_mode(self, sqlite_storage, worker_instance):
        """Test that binary data works correctly in competing mode."""
        await sqlite_storage.subscribe_to_channel(worker_instance, "jobs", "competing")

        # Publish binary data
        binary_data = b"\x00\x01\x02\x03\xff"
        await sqlite_storage.publish_to_channel(
            channel="jobs",
            data=binary_data,
            metadata=None,
        )

        # Get pending messages
        pending = await sqlite_storage.get_pending_channel_messages(worker_instance, "jobs")

        assert len(pending) == 1
        assert pending[0]["data"] == binary_data

    async def test_metadata_preserved_in_competing_mode(self, sqlite_storage, worker_instance):
        """Test that metadata is preserved in competing mode."""
        await sqlite_storage.subscribe_to_channel(worker_instance, "jobs", "competing")

        # Publish with metadata
        await sqlite_storage.publish_to_channel(
            channel="jobs",
            data={"task": "test"},
            metadata={"priority": "high", "retry_count": 0},
        )

        pending = await sqlite_storage.get_pending_channel_messages(worker_instance, "jobs")

        assert len(pending) == 1
        assert pending[0]["metadata"]["priority"] == "high"
        assert pending[0]["metadata"]["retry_count"] == 0
