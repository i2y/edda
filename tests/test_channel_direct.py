"""
Tests for channel direct mode (mode="direct").

This module tests the syntactic sugar for receiving messages sent via send_to().
The direct mode transforms channel names to "channel:instance_id" internally.
"""

import pytest
import pytest_asyncio

from edda.channels import (
    ChannelMessage,
    WaitForChannelMessageException,
    receive,
    send_to,
    subscribe,
    unsubscribe,
)
from edda.context import WorkflowContext


@pytest.mark.asyncio
class TestDirectMode:
    """Test suite for mode='direct' subscription."""

    @pytest_asyncio.fixture
    async def workflow_instance(self, sqlite_storage, create_test_instance):
        """Create a workflow instance for testing."""
        instance_id = "test-direct-instance-001"
        await create_test_instance(
            instance_id=instance_id,
            workflow_name="test_workflow",
            owner_service="test-service",
            input_data={"test": "data"},
        )
        await sqlite_storage.update_instance_status(instance_id, "running")
        return instance_id

    async def test_subscribe_direct_transforms_channel_name(
        self, sqlite_storage, workflow_instance
    ):
        """Test that subscribe with mode='direct' transforms channel to channel:instance_id."""
        ctx = WorkflowContext(
            instance_id=workflow_instance,
            workflow_name="test_workflow",
            storage=sqlite_storage,
            worker_id="worker-1",
            is_replaying=False,
        )

        # Subscribe with direct mode
        await subscribe(ctx, "notifications", mode="direct")

        # Verify subscription was created with transformed channel name
        transformed_channel = f"notifications:{workflow_instance}"
        subscription = await sqlite_storage.get_channel_subscription(
            workflow_instance, transformed_channel
        )
        assert subscription is not None
        assert subscription["mode"] == "broadcast"  # Direct uses broadcast internally

        # Verify the original channel is NOT subscribed
        original_subscription = await sqlite_storage.get_channel_subscription(
            workflow_instance, "notifications"
        )
        assert original_subscription is None

    async def test_subscribe_direct_records_in_context(self, sqlite_storage, workflow_instance):
        """Test that subscribe with mode='direct' records the subscription in context."""
        ctx = WorkflowContext(
            instance_id=workflow_instance,
            workflow_name="test_workflow",
            storage=sqlite_storage,
            worker_id="worker-1",
            is_replaying=False,
        )

        # Before subscribe, channel should not be recorded
        assert not ctx._is_direct_subscription("notifications")

        # Subscribe with direct mode
        await subscribe(ctx, "notifications", mode="direct")

        # After subscribe, channel should be recorded
        assert ctx._is_direct_subscription("notifications")

    async def test_receive_with_direct_uses_transformed_channel(
        self, sqlite_storage, workflow_instance
    ):
        """Test that receive with direct subscription uses transformed channel name."""
        ctx = WorkflowContext(
            instance_id=workflow_instance,
            workflow_name="test_workflow",
            storage=sqlite_storage,
            worker_id="worker-1",
            is_replaying=False,
        )

        # Subscribe with direct mode
        await subscribe(ctx, "notifications", mode="direct")

        # Receive should raise exception with transformed channel name
        with pytest.raises(WaitForChannelMessageException) as exc_info:
            await receive(ctx, "notifications")

        # The exception channel should be the transformed channel
        expected_channel = f"notifications:{workflow_instance}"
        assert exc_info.value.channel == expected_channel
        # But activity_id uses original channel name for determinism
        assert exc_info.value.activity_id == "receive_notifications:1"

    async def test_receive_returns_message_with_original_channel(
        self, sqlite_storage, workflow_instance
    ):
        """Test that received message has original channel name for readability."""
        ctx = WorkflowContext(
            instance_id=workflow_instance,
            workflow_name="test_workflow",
            storage=sqlite_storage,
            worker_id="worker-1",
            is_replaying=False,
        )

        # Subscribe with direct mode
        await subscribe(ctx, "notifications", mode="direct")

        # Publish a message to the transformed channel (simulating send_to)
        transformed_channel = f"notifications:{workflow_instance}"
        await sqlite_storage.publish_to_channel(
            transformed_channel,
            {"message": "Hello!"},
            {"source_instance_id": "sender-123"},
        )

        # Receive should return the message with original channel name
        msg = await receive(ctx, "notifications")

        assert isinstance(msg, ChannelMessage)
        assert msg.data == {"message": "Hello!"}
        assert msg.channel == "notifications"  # Original channel name for readability

    async def test_send_to_and_direct_receive_integration(
        self, sqlite_storage, create_test_instance
    ):
        """Test end-to-end: send_to from one workflow, receive with direct mode in another."""
        # Create receiver instance
        receiver_id = "receiver-instance-001"
        await create_test_instance(
            instance_id=receiver_id,
            workflow_name="receiver_workflow",
            owner_service="test-service",
            input_data={},
        )
        await sqlite_storage.update_instance_status(receiver_id, "running")

        # Create sender instance
        sender_id = "sender-instance-001"
        await create_test_instance(
            instance_id=sender_id,
            workflow_name="sender_workflow",
            owner_service="test-service",
            input_data={},
        )
        await sqlite_storage.update_instance_status(sender_id, "running")

        # Receiver subscribes with direct mode
        receiver_ctx = WorkflowContext(
            instance_id=receiver_id,
            workflow_name="receiver_workflow",
            storage=sqlite_storage,
            worker_id="worker-1",
            is_replaying=False,
        )
        await subscribe(receiver_ctx, "messages", mode="direct")

        # Sender sends message using send_to
        sender_ctx = WorkflowContext(
            instance_id=sender_id,
            workflow_name="sender_workflow",
            storage=sqlite_storage,
            worker_id="worker-1",
            is_replaying=False,
        )
        await send_to(
            sender_ctx,
            instance_id=receiver_id,
            data={"payload": "Hello from sender!"},
            channel="messages",
        )

        # Receiver should be able to receive the message
        msg = await receive(receiver_ctx, "messages")

        assert isinstance(msg, ChannelMessage)
        assert msg.data == {"payload": "Hello from sender!"}
        assert msg.channel == "messages"

    async def test_unsubscribe_with_direct_mode(self, sqlite_storage, workflow_instance):
        """Test that unsubscribe works correctly with direct mode subscriptions."""
        ctx = WorkflowContext(
            instance_id=workflow_instance,
            workflow_name="test_workflow",
            storage=sqlite_storage,
            worker_id="worker-1",
            is_replaying=False,
        )

        # Subscribe with direct mode
        await subscribe(ctx, "notifications", mode="direct")

        # Verify subscription exists
        transformed_channel = f"notifications:{workflow_instance}"
        subscription = await sqlite_storage.get_channel_subscription(
            workflow_instance, transformed_channel
        )
        assert subscription is not None

        # Unsubscribe using original channel name
        await unsubscribe(ctx, "notifications")

        # Verify subscription is removed
        subscription = await sqlite_storage.get_channel_subscription(
            workflow_instance, transformed_channel
        )
        assert subscription is None

    async def test_replay_with_direct_mode(self, sqlite_storage, workflow_instance):
        """Test that direct mode works correctly during replay."""
        # First execution: subscribe and receive
        ctx1 = WorkflowContext(
            instance_id=workflow_instance,
            workflow_name="test_workflow",
            storage=sqlite_storage,
            worker_id="worker-1",
            is_replaying=False,
        )
        await subscribe(ctx1, "notifications", mode="direct")

        # Publish message and receive
        transformed_channel = f"notifications:{workflow_instance}"
        await sqlite_storage.publish_to_channel(
            transformed_channel,
            {"message": "Test replay"},
            {},
        )
        msg = await receive(ctx1, "notifications")
        assert msg.data == {"message": "Test replay"}

        # Replay: create new context with is_replaying=True
        ctx2 = WorkflowContext(
            instance_id=workflow_instance,
            workflow_name="test_workflow",
            storage=sqlite_storage,
            worker_id="worker-1",
            is_replaying=True,
        )

        # Re-subscribe during replay (rebuilds _direct_subscriptions)
        await subscribe(ctx2, "notifications", mode="direct")

        # Verify direct subscription is recorded
        assert ctx2._is_direct_subscription("notifications")

        # Load history for replay
        history = await sqlite_storage.get_history(workflow_instance)
        ctx2._history_cache = {h["activity_id"]: h["event_data"] for h in history}

        # Receive during replay should return cached message
        replayed_msg = await receive(ctx2, "notifications")
        assert replayed_msg.data == {"message": "Test replay"}
        assert replayed_msg.channel == "notifications"

    async def test_broadcast_mode_unchanged(self, sqlite_storage, workflow_instance):
        """Test that broadcast mode still works correctly."""
        ctx = WorkflowContext(
            instance_id=workflow_instance,
            workflow_name="test_workflow",
            storage=sqlite_storage,
            worker_id="worker-1",
            is_replaying=False,
        )

        # Subscribe with broadcast mode
        await subscribe(ctx, "events", mode="broadcast")

        # Verify subscription uses original channel name
        subscription = await sqlite_storage.get_channel_subscription(workflow_instance, "events")
        assert subscription is not None
        assert subscription["mode"] == "broadcast"

        # Verify it's not recorded as direct
        assert not ctx._is_direct_subscription("events")

    async def test_competing_mode_unchanged(self, sqlite_storage, workflow_instance):
        """Test that competing mode still works correctly."""
        ctx = WorkflowContext(
            instance_id=workflow_instance,
            workflow_name="test_workflow",
            storage=sqlite_storage,
            worker_id="worker-1",
            is_replaying=False,
        )

        # Subscribe with competing mode
        await subscribe(ctx, "jobs", mode="competing")

        # Verify subscription uses original channel name
        subscription = await sqlite_storage.get_channel_subscription(workflow_instance, "jobs")
        assert subscription is not None
        assert subscription["mode"] == "competing"

        # Verify it's not recorded as direct
        assert not ctx._is_direct_subscription("jobs")

    async def test_invalid_mode_raises_error(self, sqlite_storage, workflow_instance):
        """Test that invalid mode raises ValueError."""
        ctx = WorkflowContext(
            instance_id=workflow_instance,
            workflow_name="test_workflow",
            storage=sqlite_storage,
            worker_id="worker-1",
            is_replaying=False,
        )

        with pytest.raises(ValueError) as exc_info:
            await subscribe(ctx, "events", mode="invalid")

        assert "Invalid subscription mode" in str(exc_info.value)
        assert "'broadcast', 'competing', or 'direct'" in str(exc_info.value)
