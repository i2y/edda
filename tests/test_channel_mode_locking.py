"""
Tests for channel mode locking functionality.

Ensures that once a channel is subscribed with a specific mode (broadcast/competing),
subsequent subscriptions with a different mode are rejected with ChannelModeConflictError.
"""

import pytest
import pytest_asyncio

from edda.channels import ChannelModeConflictError, subscribe
from edda.context import WorkflowContext


@pytest.mark.asyncio
class TestChannelModeLocking:
    """Test suite for channel mode locking."""

    @pytest_asyncio.fixture
    async def workflow_instances(self, sqlite_storage, create_test_instance):
        """Create multiple workflow instances for testing."""
        instances = []
        for i in range(1, 4):
            instance_id = f"mode-lock-test-{i}"
            await create_test_instance(
                instance_id=instance_id,
                workflow_name="test_workflow",
                owner_service="test-service",
                input_data={"test": True},
            )
            await sqlite_storage.update_instance_status(instance_id, "running")
            instances.append(instance_id)
        return instances

    async def test_broadcast_then_competing_raises_error(self, sqlite_storage, workflow_instances):
        """Test that subscribing with competing mode after broadcast raises error."""
        ctx1 = WorkflowContext(
            instance_id=workflow_instances[0],
            workflow_name="test_workflow",
            storage=sqlite_storage,
            worker_id="worker-1",
            is_replaying=False,
        )

        # Subscribe with broadcast mode first
        await subscribe(ctx1, "test-channel-1", mode="broadcast")

        # Verify mode was stored
        mode = await sqlite_storage.get_channel_mode("test-channel-1")
        assert mode == "broadcast"

        # Try to subscribe with competing mode - should fail
        ctx2 = WorkflowContext(
            instance_id=workflow_instances[1],
            workflow_name="test_workflow",
            storage=sqlite_storage,
            worker_id="worker-1",
            is_replaying=False,
        )

        with pytest.raises(ChannelModeConflictError) as exc_info:
            await subscribe(ctx2, "test-channel-1", mode="competing")

        assert exc_info.value.channel == "test-channel-1"
        assert exc_info.value.existing_mode == "broadcast"
        assert exc_info.value.requested_mode == "competing"

    async def test_competing_then_broadcast_raises_error(self, sqlite_storage, workflow_instances):
        """Test that subscribing with broadcast mode after competing raises error."""
        ctx1 = WorkflowContext(
            instance_id=workflow_instances[0],
            workflow_name="test_workflow",
            storage=sqlite_storage,
            worker_id="worker-1",
            is_replaying=False,
        )

        # Subscribe with competing mode first
        await subscribe(ctx1, "test-channel-2", mode="competing")

        # Verify mode was stored
        mode = await sqlite_storage.get_channel_mode("test-channel-2")
        assert mode == "competing"

        # Try to subscribe with broadcast mode - should fail
        ctx2 = WorkflowContext(
            instance_id=workflow_instances[1],
            workflow_name="test_workflow",
            storage=sqlite_storage,
            worker_id="worker-1",
            is_replaying=False,
        )

        with pytest.raises(ChannelModeConflictError) as exc_info:
            await subscribe(ctx2, "test-channel-2", mode="broadcast")

        assert exc_info.value.channel == "test-channel-2"
        assert exc_info.value.existing_mode == "competing"
        assert exc_info.value.requested_mode == "broadcast"

    async def test_same_mode_subscription_allowed(self, sqlite_storage, workflow_instances):
        """Test that multiple subscriptions with the same mode are allowed."""
        # Subscribe first instance with broadcast
        ctx1 = WorkflowContext(
            instance_id=workflow_instances[0],
            workflow_name="test_workflow",
            storage=sqlite_storage,
            worker_id="worker-1",
            is_replaying=False,
        )
        await subscribe(ctx1, "test-channel-3", mode="broadcast")

        # Subscribe second instance with same mode - should succeed
        ctx2 = WorkflowContext(
            instance_id=workflow_instances[1],
            workflow_name="test_workflow",
            storage=sqlite_storage,
            worker_id="worker-1",
            is_replaying=False,
        )
        await subscribe(ctx2, "test-channel-3", mode="broadcast")

        # Subscribe third instance with same mode - should succeed
        ctx3 = WorkflowContext(
            instance_id=workflow_instances[2],
            workflow_name="test_workflow",
            storage=sqlite_storage,
            worker_id="worker-1",
            is_replaying=False,
        )
        await subscribe(ctx3, "test-channel-3", mode="broadcast")

        # All three should be subscribed
        sub1 = await sqlite_storage.get_channel_subscription(
            workflow_instances[0], "test-channel-3"
        )
        sub2 = await sqlite_storage.get_channel_subscription(
            workflow_instances[1], "test-channel-3"
        )
        sub3 = await sqlite_storage.get_channel_subscription(
            workflow_instances[2], "test-channel-3"
        )

        assert sub1 is not None
        assert sub2 is not None
        assert sub3 is not None

    async def test_different_channels_independent_modes(self, sqlite_storage, workflow_instances):
        """Test that different channels can have different modes."""
        ctx1 = WorkflowContext(
            instance_id=workflow_instances[0],
            workflow_name="test_workflow",
            storage=sqlite_storage,
            worker_id="worker-1",
            is_replaying=False,
        )

        # Channel A with broadcast
        await subscribe(ctx1, "channel-a", mode="broadcast")

        # Channel B with competing - should succeed (different channel)
        await subscribe(ctx1, "channel-b", mode="competing")

        # Verify modes
        mode_a = await sqlite_storage.get_channel_mode("channel-a")
        mode_b = await sqlite_storage.get_channel_mode("channel-b")

        assert mode_a == "broadcast"
        assert mode_b == "competing"

    async def test_get_channel_mode_returns_none_for_new_channel(self, sqlite_storage):
        """Test that get_channel_mode returns None for channels with no subscriptions."""
        mode = await sqlite_storage.get_channel_mode("nonexistent-channel")
        assert mode is None

    async def test_error_message_is_informative(self, sqlite_storage, workflow_instances):
        """Test that ChannelModeConflictError has a clear message."""
        ctx1 = WorkflowContext(
            instance_id=workflow_instances[0],
            workflow_name="test_workflow",
            storage=sqlite_storage,
            worker_id="worker-1",
            is_replaying=False,
        )
        await subscribe(ctx1, "msg-test-channel", mode="broadcast")

        ctx2 = WorkflowContext(
            instance_id=workflow_instances[1],
            workflow_name="test_workflow",
            storage=sqlite_storage,
            worker_id="worker-1",
            is_replaying=False,
        )

        with pytest.raises(ChannelModeConflictError) as exc_info:
            await subscribe(ctx2, "msg-test-channel", mode="competing")

        error_msg = str(exc_info.value)
        assert "msg-test-channel" in error_msg
        assert "broadcast" in error_msg
        assert "competing" in error_msg
