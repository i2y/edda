"""
Tests for recur() subscription cleanup functionality.

Tests cover:
- Event subscription cleanup during recur() (via Message Passing layer)
- Timer subscription cleanup during recur()
- Message subscription cleanup during recur()
- Group membership cleanup at the fenced recur boundary
- Archive before cleanup ordering
- The same instance can rejoin groups after recur()

Note: CloudEvents internally uses Message Passing, so event subscriptions
are registered and cleaned up via the message subscription API.
"""

from datetime import UTC, datetime, timedelta

import pytest
import pytest_asyncio


@pytest.mark.asyncio
class TestRecurSubscriptionCleanup:
    """Test suite for recur() subscription cleanup."""

    @pytest_asyncio.fixture
    async def workflow_instance(self, sqlite_storage, create_test_instance):
        """Create a workflow instance for testing."""
        instance_id = "test-recur-cleanup-001"
        await create_test_instance(
            instance_id=instance_id,
            workflow_name="test_workflow",
            owner_service="test-service",
            input_data={"test": "data"},
        )
        await sqlite_storage.update_instance_status(instance_id, "running")
        return instance_id

    async def test_cleanup_removes_event_subscriptions(self, sqlite_storage, workflow_instance):
        """Test that cleanup_instance_subscriptions removes event subscriptions.

        Note: CloudEvents internally uses Message Passing, so event subscriptions
        are registered via channel subscription API with lock-first pattern.
        """
        instance_id = workflow_instance

        # Subscribe to channel first
        await sqlite_storage.subscribe_to_channel(
            instance_id=instance_id,
            channel="order.created",
            mode="broadcast",
        )

        # Acquire lock first, then register channel receive (which releases lock)
        await sqlite_storage.try_acquire_lock(instance_id, "test-worker")
        await sqlite_storage.register_channel_receive_and_release_lock(
            instance_id=instance_id,
            worker_id="test-worker",
            channel="order.created",
            activity_id="wait_message_order.created:1",
            timeout_seconds=3600,  # 1 hour
        )

        # Verify subscription exists
        subs = await sqlite_storage.find_waiting_instances_by_channel("order.created")
        assert len(subs) == 1
        assert subs[0]["instance_id"] == instance_id

        # Clean up subscriptions
        await sqlite_storage.cleanup_instance_subscriptions(instance_id)

        # Verify subscription is removed
        subs = await sqlite_storage.find_waiting_instances_by_channel("order.created")
        assert len(subs) == 0

    async def test_cleanup_removes_timer_subscriptions(self, sqlite_storage, workflow_instance):
        """Test that cleanup_instance_subscriptions removes timer subscriptions."""
        instance_id = workflow_instance

        # Acquire lock first, then register timer subscription (which releases lock)
        await sqlite_storage.try_acquire_lock(instance_id, "test-worker")
        expires_at = datetime.now(UTC) + timedelta(hours=1)
        await sqlite_storage.register_timer_subscription_and_release_lock(
            instance_id=instance_id,
            worker_id="test-worker",
            timer_id="wait_timer:1",
            expires_at=expires_at,
            activity_id="wait_timer:1",
        )

        # For cleanup test, we set expires_at to past to use find_expired_timers
        # But since cleanup_instance_subscriptions just deletes all timer subs,
        # we need another way to verify. Let's try creating a timer that's already expired.

        # Re-acquire lock and create another timer subscription
        await sqlite_storage.try_acquire_lock(instance_id, "test-worker")
        past_expires_at = datetime.now(UTC) - timedelta(seconds=1)
        await sqlite_storage.register_timer_subscription_and_release_lock(
            instance_id=instance_id,
            worker_id="test-worker",
            timer_id="wait_timer:2",
            expires_at=past_expires_at,
            activity_id="wait_timer:2",
        )

        # Verify timer subscription exists via find_expired_timers
        expired = await sqlite_storage.find_expired_timers()
        assert any(t["instance_id"] == instance_id for t in expired)

        # Clean up subscriptions
        await sqlite_storage.cleanup_instance_subscriptions(instance_id)

        # Verify timer subscriptions are removed
        expired = await sqlite_storage.find_expired_timers()
        assert not any(t["instance_id"] == instance_id for t in expired)

    async def test_cleanup_removes_message_subscriptions(self, sqlite_storage, workflow_instance):
        """Test that cleanup_instance_subscriptions removes message subscriptions."""
        instance_id = workflow_instance

        # Subscribe to channel first
        await sqlite_storage.subscribe_to_channel(
            instance_id=instance_id,
            channel="approval",
            mode="broadcast",
        )

        # Acquire lock first, then register channel receive (which releases lock)
        await sqlite_storage.try_acquire_lock(instance_id, "test-worker")
        await sqlite_storage.register_channel_receive_and_release_lock(
            instance_id=instance_id,
            worker_id="test-worker",
            channel="approval",
            activity_id="wait_message_approval:1",
        )

        # Verify subscription exists
        subs = await sqlite_storage.find_waiting_instances_by_channel("approval")
        assert len(subs) == 1
        assert subs[0]["instance_id"] == instance_id

        # Clean up subscriptions
        await sqlite_storage.cleanup_instance_subscriptions(instance_id)

        # Verify subscription is removed
        subs = await sqlite_storage.find_waiting_instances_by_channel("approval")
        assert len(subs) == 0

    async def test_group_memberships_not_cleaned_by_cleanup_subscriptions(
        self, sqlite_storage, workflow_instance
    ):
        """
        Test that group memberships are NOT removed by cleanup_instance_subscriptions.

        Note: this legacy helper only cleans subscriptions. The fenced recur
        reset (tested in test_kernel_rules.py) is what deletes group memberships.
        """
        instance_id = workflow_instance

        # Join a group
        await sqlite_storage.join_group(instance_id, "order_notifications")

        # Verify group membership exists
        members = await sqlite_storage.get_group_members("order_notifications")
        assert instance_id in members

        # Clean up subscriptions (should NOT affect group memberships)
        await sqlite_storage.cleanup_instance_subscriptions(instance_id)

        # Verify group membership is still intact
        members = await sqlite_storage.get_group_members("order_notifications")
        assert instance_id in members

    async def test_cleanup_multiple_event_subscriptions(self, sqlite_storage, workflow_instance):
        """Test cleanup with multiple event subscriptions.

        Note: CloudEvents internally uses Message Passing, so we register
        channel subscriptions. Each subscription requires lock-first pattern.
        """
        instance_id = workflow_instance
        channels = ["order.created", "order.shipped", "order.completed"]

        # Register multiple channel subscriptions (lock-first pattern for each)
        for i, channel in enumerate(channels, start=1):
            # Subscribe to channel first
            await sqlite_storage.subscribe_to_channel(
                instance_id=instance_id,
                channel=channel,
                mode="broadcast",
            )
            # Acquire lock and register channel receive
            await sqlite_storage.try_acquire_lock(instance_id, "test-worker")
            await sqlite_storage.register_channel_receive_and_release_lock(
                instance_id=instance_id,
                worker_id="test-worker",
                channel=channel,
                activity_id=f"wait_message_{channel}:{i}",
                timeout_seconds=3600,  # 1 hour
            )

        # Verify subscriptions exist
        for channel in channels:
            subs = await sqlite_storage.find_waiting_instances_by_channel(channel)
            assert len(subs) == 1
            assert subs[0]["instance_id"] == instance_id

        # Clean up all subscriptions in one call
        await sqlite_storage.cleanup_instance_subscriptions(instance_id)

        # Verify all subscriptions are removed
        for channel in channels:
            subs = await sqlite_storage.find_waiting_instances_by_channel(channel)
            assert not any(s["instance_id"] == instance_id for s in subs)

    async def test_same_instance_can_rejoin_groups_after_recur_reset(
        self, sqlite_storage, create_test_instance
    ):
        """The recur reset removes group memberships on the same instance."""
        instance_id = "same-instance-recur-001"

        await create_test_instance(
            instance_id=instance_id,
            workflow_name="recurring_workflow",
            owner_service="test-service",
            input_data={"count": 0},
        )
        await sqlite_storage.join_group(instance_id, "order_watchers")
        assert instance_id in await sqlite_storage.get_group_members("order_watchers")

        lock_id = "recur-reset-worker"
        assert await sqlite_storage.try_acquire_lock(instance_id, lock_id)
        await sqlite_storage.reset_instance_for_recur(instance_id, {"count": 100}, lock_id)
        await sqlite_storage.release_lock(instance_id, lock_id)

        assert instance_id not in await sqlite_storage.get_group_members("order_watchers")
        await sqlite_storage.join_group(instance_id, "order_watchers")
        assert instance_id in await sqlite_storage.get_group_members("order_watchers")


@pytest.mark.asyncio
class TestLeaveAllGroups:
    """Test suite for leave_all_groups functionality."""

    @pytest_asyncio.fixture
    async def workflow_instance(self, sqlite_storage, create_test_instance):
        """Create a workflow instance for testing."""
        instance_id = "test-leave-groups-001"
        await create_test_instance(
            instance_id=instance_id,
            workflow_name="test_workflow",
            owner_service="test-service",
            input_data={"test": "data"},
        )
        return instance_id

    async def test_leave_all_groups_removes_all_memberships(
        self, sqlite_storage, workflow_instance
    ):
        """Test that leave_all_groups removes all group memberships."""
        instance_id = workflow_instance

        # Join multiple groups
        for group in ["group_a", "group_b", "group_c"]:
            await sqlite_storage.join_group(instance_id, group)

        # Verify memberships exist
        for group in ["group_a", "group_b", "group_c"]:
            members = await sqlite_storage.get_group_members(group)
            assert instance_id in members

        # Leave all groups
        await sqlite_storage.leave_all_groups(instance_id)

        # Verify all memberships are removed
        for group in ["group_a", "group_b", "group_c"]:
            members = await sqlite_storage.get_group_members(group)
            assert instance_id not in members

    async def test_leave_all_groups_does_not_affect_other_instances(
        self, sqlite_storage, create_test_instance
    ):
        """Test that leave_all_groups only affects the specified instance."""
        instance_a = "instance-a"
        instance_b = "instance-b"

        await create_test_instance(
            instance_id=instance_a,
            workflow_name="test_workflow",
            owner_service="test-service",
            input_data={},
        )
        await create_test_instance(
            instance_id=instance_b,
            workflow_name="test_workflow",
            owner_service="test-service",
            input_data={},
        )

        # Both join the same group
        await sqlite_storage.join_group(instance_a, "shared_group")
        await sqlite_storage.join_group(instance_b, "shared_group")

        # Instance A leaves all groups
        await sqlite_storage.leave_all_groups(instance_a)

        # Instance B should still be in the group
        members = await sqlite_storage.get_group_members("shared_group")
        assert instance_a not in members
        assert instance_b in members
