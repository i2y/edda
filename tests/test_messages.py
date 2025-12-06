"""
Tests for message passing module.

Tests cover:
- receive (wait_message) functionality
- send / send_to
- Message delivery and workflow resumption
- Timeout handling

Note: This module uses the new Channel-based Message Queue API (edda.channels).
"""

import pytest
import pytest_asyncio

from edda.channels import (
    ChannelMessage,
    WaitForChannelMessageException,
    publish,
    receive,
    send_to,
)
from edda.context import WorkflowContext


@pytest.mark.asyncio
class TestReceive:
    """Test suite for receive functionality."""

    @pytest_asyncio.fixture
    async def workflow_instance(self, sqlite_storage, create_test_instance):
        """Create a workflow instance for testing."""
        instance_id = "test-message-instance-001"
        await create_test_instance(
            instance_id=instance_id,
            workflow_name="test_workflow",
            owner_service="test-service",
            input_data={"test": "data"},
        )
        await sqlite_storage.update_instance_status(instance_id, "running")
        return instance_id

    async def test_receive_raises_exception_during_normal_execution(
        self, sqlite_storage, workflow_instance
    ):
        """Test that receive raises WaitForChannelMessageException during normal execution."""
        ctx = WorkflowContext(
            instance_id=workflow_instance,
            workflow_name="test_workflow",
            storage=sqlite_storage,
            worker_id="worker-1",
            is_replaying=False,
        )

        # Should raise exception to pause workflow
        with pytest.raises(WaitForChannelMessageException) as exc_info:
            await receive(
                ctx,
                channel="approval",
                timeout_seconds=300,
            )

        # Verify exception details
        assert exc_info.value.channel == "approval"
        assert exc_info.value.timeout_seconds == 300
        assert exc_info.value.activity_id == "receive_approval:1"

    async def test_receive_with_custom_message_id(self, sqlite_storage, workflow_instance):
        """Test receive with custom message_id."""
        ctx = WorkflowContext(
            instance_id=workflow_instance,
            workflow_name="test_workflow",
            storage=sqlite_storage,
            worker_id="worker-1",
            is_replaying=False,
        )

        with pytest.raises(WaitForChannelMessageException) as exc_info:
            await receive(
                ctx,
                channel="approval",
                message_id="custom_approval_id",
            )

        # Custom message_id should be used as activity_id
        assert exc_info.value.activity_id == "custom_approval_id"

    async def test_receive_returns_cached_during_replay(self, sqlite_storage, workflow_instance):
        """Test that receive returns cached message during replay."""
        # First, add history entry (simulating a received message)
        await sqlite_storage.append_history(
            instance_id=workflow_instance,
            activity_id="receive_approval:1",
            event_type="ChannelMessageReceived",
            event_data={
                "data": {"approved": True, "approver": "admin"},
                "channel": "approval",
                "id": "msg-123",
                "metadata": {"source_instance_id": "sender-workflow"},
            },
        )

        ctx = WorkflowContext(
            instance_id=workflow_instance,
            workflow_name="test_workflow",
            storage=sqlite_storage,
            worker_id="worker-1",
            is_replaying=True,
        )

        # Load history for replay - use _history_cache
        history = await sqlite_storage.get_history(workflow_instance)
        ctx._history_cache = {h["activity_id"]: h["event_data"] for h in history}

        # During replay, should return the cached message
        msg = await receive(ctx, channel="approval")

        assert isinstance(msg, ChannelMessage)
        assert msg.data == {"approved": True, "approver": "admin"}
        assert msg.channel == "approval"
        assert msg.id == "msg-123"
        assert msg.metadata == {"source_instance_id": "sender-workflow"}

    async def test_receive_timeout_error_during_replay(self, sqlite_storage, workflow_instance):
        """Test that receive raises TimeoutError during replay if timeout was recorded."""
        # Add history entry with timeout error
        await sqlite_storage.append_history(
            instance_id=workflow_instance,
            activity_id="receive_approval:1",
            event_type="MessageTimeout",
            event_data={
                "_error": True,
                "error_type": "TimeoutError",
                "error_message": "Message on channel 'approval' did not arrive within timeout",
                "channel": "approval",
            },
        )

        ctx = WorkflowContext(
            instance_id=workflow_instance,
            workflow_name="test_workflow",
            storage=sqlite_storage,
            worker_id="worker-1",
            is_replaying=True,
        )

        # Load history for replay - use _history_cache
        history = await sqlite_storage.get_history(workflow_instance)
        ctx._history_cache = {h["activity_id"]: h["event_data"] for h in history}

        # During replay, should raise TimeoutError
        with pytest.raises(TimeoutError) as exc_info:
            await receive(ctx, channel="approval")

        assert "did not arrive within timeout" in str(exc_info.value)


@pytest.mark.asyncio
class TestSendMessage:
    """Test suite for send_message and send_message_to functionality."""

    @pytest_asyncio.fixture
    async def sender_instance(self, sqlite_storage, create_test_instance):
        """Create a sender workflow instance."""
        instance_id = "sender-workflow-001"
        await create_test_instance(
            instance_id=instance_id,
            workflow_name="sender_workflow",
            owner_service="test-service",
            input_data={},
        )
        await sqlite_storage.update_instance_status(instance_id, "running")
        return instance_id

    @pytest_asyncio.fixture
    async def receiver_instance(self, sqlite_storage, create_test_instance):
        """Create a receiver workflow instance."""
        instance_id = "receiver-workflow-001"
        await create_test_instance(
            instance_id=instance_id,
            workflow_name="receiver_workflow",
            owner_service="test-service",
            input_data={},
        )
        await sqlite_storage.update_instance_status(instance_id, "waiting_for_message")
        return instance_id

    async def test_send_message_to_waiting_workflow(
        self, sqlite_storage, sender_instance, receiver_instance
    ):
        """Test sending a message to a workflow waiting on a channel."""
        # First acquire lock for receiver (required by register_channel_receive_and_release_lock)
        await sqlite_storage.try_acquire_lock(receiver_instance, "worker-1")

        # Subscribe to channel first
        await sqlite_storage.subscribe_to_channel(
            instance_id=receiver_instance,
            channel="approval",
            mode="broadcast",
        )

        # Register the receiver as waiting for a message (this releases the lock)
        await sqlite_storage.register_channel_receive_and_release_lock(
            instance_id=receiver_instance,
            worker_id="worker-1",
            channel="approval",
            activity_id="receive_approval:1",
        )

        # Acquire lock for sender (required by context)
        await sqlite_storage.try_acquire_lock(sender_instance, "worker-1")

        sender_ctx = WorkflowContext(
            instance_id=sender_instance,
            workflow_name="sender_workflow",
            storage=sqlite_storage,
            worker_id="worker-1",
            is_replaying=False,
        )

        # Send message from sender to receiver using publish with target_instance_id
        # (Point-to-Point delivery on a named channel)
        message_id = await publish(
            sender_ctx,
            channel="approval",
            data={"approved": True, "decision": "approved"},
            target_instance_id=receiver_instance,
        )

        assert message_id is not None

        # Verify message was recorded in history
        history = await sqlite_storage.get_history(receiver_instance)
        msg_history = [h for h in history if h["event_type"] == "ChannelMessageReceived"]
        assert len(msg_history) == 1

        event_data = msg_history[0]["event_data"]
        assert event_data["data"] == {"approved": True, "decision": "approved"}
        assert event_data["channel"] == "approval"

        # Verify subscription was removed
        waiting = await sqlite_storage.get_channel_subscribers_waiting("approval")
        assert len(waiting) == 0

    async def test_send_to_not_delivered_if_no_waiting_workflow(
        self, sqlite_storage, sender_instance, receiver_instance
    ):
        """Test that send_to returns False if target workflow is not waiting."""
        # Don't register receiver as waiting

        # Acquire lock for sender (required by context)
        await sqlite_storage.try_acquire_lock(sender_instance, "worker-1")

        sender_ctx = WorkflowContext(
            instance_id=sender_instance,
            workflow_name="sender_workflow",
            storage=sqlite_storage,
            worker_id="worker-1",
            is_replaying=False,
        )

        # Send message to receiver that is not waiting (should not be delivered)
        delivered = await send_to(
            sender_ctx,
            instance_id=receiver_instance,
            data={"approved": True},
            channel="approval",
        )

        assert delivered is False

    async def test_send_to_with_bytes_data(
        self, sqlite_storage, sender_instance, receiver_instance
    ):
        """Test sending binary data in a message via publish."""
        # First acquire lock for receiver (required by register_channel_receive_and_release_lock)
        await sqlite_storage.try_acquire_lock(receiver_instance, "worker-1")

        # Subscribe to channel first
        await sqlite_storage.subscribe_to_channel(
            instance_id=receiver_instance,
            channel="binary_channel",
            mode="broadcast",
        )

        # Register the receiver as waiting (this releases the lock)
        await sqlite_storage.register_channel_receive_and_release_lock(
            instance_id=receiver_instance,
            worker_id="worker-1",
            channel="binary_channel",
            activity_id="receive_binary_channel:1",
        )

        # Acquire lock for sender
        await sqlite_storage.try_acquire_lock(sender_instance, "worker-1")

        sender_ctx = WorkflowContext(
            instance_id=sender_instance,
            workflow_name="sender_workflow",
            storage=sqlite_storage,
            worker_id="worker-1",
            is_replaying=False,
        )

        # Send binary data using publish with target_instance_id
        binary_data = b"\x00\x01\x02\x03\x04"
        message_id = await publish(
            sender_ctx,
            channel="binary_channel",
            data=binary_data,
            target_instance_id=receiver_instance,
        )

        assert message_id is not None

        # Verify binary data was recorded
        # For binary data, event_data is the raw bytes directly
        history = await sqlite_storage.get_history(receiver_instance)
        msg_history = [h for h in history if h["event_type"] == "ChannelMessageReceived"]
        assert len(msg_history) == 1
        assert msg_history[0]["event_data"] == binary_data

    async def test_send_message_with_metadata(
        self, sqlite_storage, sender_instance, receiver_instance
    ):
        """Test sending a message with custom metadata."""
        # First acquire lock for receiver (required by register_channel_receive_and_release_lock)
        await sqlite_storage.try_acquire_lock(receiver_instance, "worker-1")

        # Subscribe to channel first
        await sqlite_storage.subscribe_to_channel(
            instance_id=receiver_instance,
            channel="metadata_channel",
            mode="broadcast",
        )

        # Register the receiver as waiting (this releases the lock)
        await sqlite_storage.register_channel_receive_and_release_lock(
            instance_id=receiver_instance,
            worker_id="worker-1",
            channel="metadata_channel",
            activity_id="receive_metadata_channel:1",
        )

        # Acquire lock for sender
        await sqlite_storage.try_acquire_lock(sender_instance, "worker-1")

        sender_ctx = WorkflowContext(
            instance_id=sender_instance,
            workflow_name="sender_workflow",
            storage=sqlite_storage,
            worker_id="worker-1",
            is_replaying=False,
        )

        # Send message with custom metadata using publish
        message_id = await publish(
            sender_ctx,
            channel="metadata_channel",
            data={"payload": "test"},
            metadata={"correlation_id": "corr-123", "priority": "high"},
            target_instance_id=receiver_instance,
        )

        assert message_id is not None

        # Verify metadata was recorded
        history = await sqlite_storage.get_history(receiver_instance)
        msg_history = [h for h in history if h["event_type"] == "ChannelMessageReceived"]
        assert len(msg_history) == 1

        metadata = msg_history[0]["event_data"]["metadata"]
        assert metadata["correlation_id"] == "corr-123"
        assert metadata["priority"] == "high"
        assert metadata["source_instance_id"] == sender_instance


@pytest.mark.asyncio
class TestMessageSubscription:
    """Test suite for message subscription management."""

    @pytest_asyncio.fixture
    async def workflow_instance(self, sqlite_storage, create_test_instance):
        """Create a workflow instance for testing."""
        instance_id = "test-sub-instance-001"
        await create_test_instance(
            instance_id=instance_id,
            workflow_name="test_workflow",
            owner_service="test-service",
            input_data={},
        )
        await sqlite_storage.update_instance_status(instance_id, "running")
        return instance_id

    async def test_find_waiting_instances_by_channel(self, sqlite_storage, workflow_instance):
        """Test finding all instances waiting on a specific channel."""
        # Acquire lock first (required for register_channel_receive_and_release_lock)
        await sqlite_storage.try_acquire_lock(workflow_instance, "worker-1")

        # Subscribe to channel first
        await sqlite_storage.subscribe_to_channel(
            instance_id=workflow_instance,
            channel="test_channel",
            mode="broadcast",
        )

        # Register subscription (releases lock)
        await sqlite_storage.register_channel_receive_and_release_lock(
            instance_id=workflow_instance,
            worker_id="worker-1",
            channel="test_channel",
            activity_id="receive_test_channel:1",
        )

        # Find waiting instances
        waiting = await sqlite_storage.get_channel_subscribers_waiting("test_channel")
        assert len(waiting) == 1
        assert waiting[0]["instance_id"] == workflow_instance
        assert waiting[0]["channel"] == "test_channel"
        assert waiting[0]["activity_id"] == "receive_test_channel:1"

    async def test_find_waiting_instances_empty_for_different_channel(
        self, sqlite_storage, workflow_instance
    ):
        """Test that find returns empty for different channel."""
        # Acquire lock first (required for register_channel_receive_and_release_lock)
        await sqlite_storage.try_acquire_lock(workflow_instance, "worker-1")

        # Subscribe to channel first
        await sqlite_storage.subscribe_to_channel(
            instance_id=workflow_instance,
            channel="channel_a",
            mode="broadcast",
        )

        # Register on one channel (releases lock)
        await sqlite_storage.register_channel_receive_and_release_lock(
            instance_id=workflow_instance,
            worker_id="worker-1",
            channel="channel_a",
            activity_id="receive_channel_a:1",
        )

        # Search for different channel
        waiting = await sqlite_storage.get_channel_subscribers_waiting("channel_b")
        assert len(waiting) == 0

    async def test_remove_message_subscription(self, sqlite_storage, workflow_instance):
        """Test removing a message subscription."""
        # Acquire lock first (required for register_channel_receive_and_release_lock)
        await sqlite_storage.try_acquire_lock(workflow_instance, "worker-1")

        # Subscribe to channel first
        await sqlite_storage.subscribe_to_channel(
            instance_id=workflow_instance,
            channel="remove_test",
            mode="broadcast",
        )

        # Register subscription (releases lock)
        await sqlite_storage.register_channel_receive_and_release_lock(
            instance_id=workflow_instance,
            worker_id="worker-1",
            channel="remove_test",
            activity_id="receive_remove_test:1",
        )

        # Verify it exists
        waiting = await sqlite_storage.get_channel_subscribers_waiting("remove_test")
        assert len(waiting) == 1

        # Remove subscription
        await sqlite_storage.unsubscribe_from_channel(workflow_instance, "remove_test")

        # Verify it's gone
        waiting = await sqlite_storage.get_channel_subscribers_waiting("remove_test")
        assert len(waiting) == 0
