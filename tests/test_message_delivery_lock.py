"""
Tests for deliver_message() Lock-First pattern.

Tests cover:
- Lock acquisition before message delivery
- Lock release on success/failure
- Race condition handling
- Status updates
- Subscription cleanup
- Binary data handling
"""

import pytest
import pytest_asyncio


@pytest.mark.asyncio
class TestDeliverMessageLockFirst:
    """Test suite for deliver_message() Lock-First pattern."""

    @pytest_asyncio.fixture
    async def workflow_instance(self, sqlite_storage, create_test_instance):
        """Create a workflow instance for testing with a message subscription."""
        instance_id = "test-deliver-message-001"
        await create_test_instance(
            instance_id=instance_id,
            workflow_name="test_workflow",
            owner_service="test-service",
            input_data={"test": "data"},
        )
        # Subscribe to channel first
        await sqlite_storage.subscribe_to_channel(
            instance_id=instance_id,
            channel="test_channel",
            mode="broadcast",
        )
        # Acquire lock and register channel receive (which releases lock and sets status)
        await sqlite_storage.try_acquire_lock(instance_id, "setup-worker")
        await sqlite_storage.register_channel_receive_and_release_lock(
            instance_id=instance_id,
            worker_id="setup-worker",
            channel="test_channel",
            activity_id="wait_message_test_channel:1",
        )
        return instance_id

    async def test_deliver_message_acquires_lock_first(self, sqlite_storage, workflow_instance):
        """Test that deliver_message acquires lock before processing."""
        instance_id = workflow_instance

        # Subscription already registered in fixture

        # Deliver message with worker_id (Lock-First mode)
        result = await sqlite_storage.deliver_message(
            instance_id=instance_id,
            channel="test_channel",
            data={"message": "hello"},
            metadata={"source": "test"},
            worker_id="worker-1",
        )

        # Should succeed
        assert result is not None
        assert result["instance_id"] == instance_id

        # Lock should be released after delivery
        instance = await sqlite_storage.get_instance(instance_id)
        assert instance["locked_by"] is None

    async def test_deliver_message_releases_lock_on_success(
        self, sqlite_storage, workflow_instance
    ):
        """Test that lock is released after successful delivery."""
        instance_id = workflow_instance

        # Subscription already registered in fixture

        # Deliver message
        result = await sqlite_storage.deliver_message(
            instance_id=instance_id,
            channel="test_channel",
            data={"message": "hello"},
            metadata={},
            worker_id="worker-1",
        )

        assert result is not None

        # Verify lock is released
        instance = await sqlite_storage.get_instance(instance_id)
        assert instance["locked_by"] is None

    async def test_deliver_message_releases_lock_on_no_subscription(
        self, sqlite_storage, create_test_instance
    ):
        """Test that lock is released even if no subscription exists."""
        instance_id = "test-no-subscription-001"
        await create_test_instance(
            instance_id=instance_id,
            workflow_name="test_workflow",
            owner_service="test-service",
            input_data={"test": "data"},
        )
        await sqlite_storage.update_instance_status(instance_id, "running")

        # Try to deliver without subscription - should return None
        result = await sqlite_storage.deliver_message(
            instance_id=instance_id,
            channel="test_channel",
            data={"message": "hello"},
            metadata={},
            worker_id="worker-1",
        )

        assert result is None

        # Lock should still be released
        instance = await sqlite_storage.get_instance(instance_id)
        assert instance["locked_by"] is None

    async def test_deliver_message_skips_if_lock_unavailable(
        self, sqlite_storage, workflow_instance
    ):
        """Test that delivery is skipped if another worker holds the lock."""
        instance_id = workflow_instance

        # Worker 1 acquires lock
        lock_acquired = await sqlite_storage.try_acquire_lock(instance_id, "worker-1")
        assert lock_acquired is True

        # Worker 2 tries to deliver - should fail due to lock
        result = await sqlite_storage.deliver_message(
            instance_id=instance_id,
            channel="test_channel",
            data={"message": "hello"},
            metadata={},
            worker_id="worker-2",
        )

        # Should return None (skipped due to lock)
        assert result is None

        # Subscription should still exist (not delivered)
        waiting = await sqlite_storage.find_waiting_instances_by_channel("test_channel")
        assert len(waiting) == 1

        # Release lock
        await sqlite_storage.release_lock(instance_id, "worker-1")

    async def test_deliver_message_updates_status_to_running(
        self, sqlite_storage, workflow_instance
    ):
        """Test that status is updated to 'running' after delivery."""
        instance_id = workflow_instance

        # Verify initial status is 'waiting_for_message' (set by subscription registration)
        instance = await sqlite_storage.get_instance(instance_id)
        assert instance["status"] == "waiting_for_message"

        # Deliver message
        result = await sqlite_storage.deliver_message(
            instance_id=instance_id,
            channel="test_channel",
            data={"message": "hello"},
            metadata={},
            worker_id="worker-1",
        )

        assert result is not None

        # Verify status changed to 'running'
        instance = await sqlite_storage.get_instance(instance_id)
        assert instance["status"] == "running"

    async def test_deliver_message_removes_subscription(self, sqlite_storage, workflow_instance):
        """Test that subscription is removed after delivery."""
        instance_id = workflow_instance

        # Verify subscription exists
        waiting = await sqlite_storage.find_waiting_instances_by_channel("test_channel")
        assert len(waiting) == 1

        # Deliver message
        result = await sqlite_storage.deliver_message(
            instance_id=instance_id,
            channel="test_channel",
            data={"message": "hello"},
            metadata={},
            worker_id="worker-1",
        )

        assert result is not None

        # Verify subscription is removed
        waiting = await sqlite_storage.find_waiting_instances_by_channel("test_channel")
        assert len(waiting) == 0

    async def test_deliver_message_records_history(self, sqlite_storage, workflow_instance):
        """Test that ChannelMessageReceived event is recorded in history."""
        instance_id = workflow_instance

        # Deliver message
        result = await sqlite_storage.deliver_message(
            instance_id=instance_id,
            channel="test_channel",
            data={"message": "hello", "value": 42},
            metadata={"source": "test"},
            worker_id="worker-1",
        )

        assert result is not None

        # Verify history entry
        history = await sqlite_storage.get_history(instance_id)
        assert len(history) == 1

        entry = history[0]
        assert entry["event_type"] == "ChannelMessageReceived"
        assert entry["activity_id"] == "wait_message_test_channel:1"
        assert entry["event_data"]["channel"] == "test_channel"

    async def test_deliver_message_with_binary_data(self, sqlite_storage, create_test_instance):
        """Test delivery with binary data payload."""
        instance_id = "test-binary-message-001"
        await create_test_instance(
            instance_id=instance_id,
            workflow_name="test_workflow",
            owner_service="test-service",
            input_data={"test": "data"},
        )
        # Subscribe to channel first
        await sqlite_storage.subscribe_to_channel(
            instance_id=instance_id,
            channel="binary_channel",
            mode="broadcast",
        )
        # Register channel receive
        await sqlite_storage.try_acquire_lock(instance_id, "setup-worker")
        await sqlite_storage.register_channel_receive_and_release_lock(
            instance_id=instance_id,
            worker_id="setup-worker",
            channel="binary_channel",
            activity_id="wait_message_binary_channel:1",
        )

        # Deliver binary message
        binary_data = b"\x00\x01\x02\x03\x04\x05"
        result = await sqlite_storage.deliver_message(
            instance_id=instance_id,
            channel="binary_channel",
            data=binary_data,
            metadata={"content_type": "application/octet-stream"},
            worker_id="worker-1",
        )

        assert result is not None

        # Verify binary data in history
        history = await sqlite_storage.get_history(instance_id)
        assert len(history) == 1
        assert history[0]["event_data"] == binary_data

    async def test_deliver_message_preserves_metadata(self, sqlite_storage, workflow_instance):
        """Test that metadata is preserved in the delivered message."""
        instance_id = workflow_instance

        # Deliver with metadata
        metadata = {
            "source_instance_id": "sender-001",
            "correlation_id": "corr-123",
            "priority": "high",
        }
        result = await sqlite_storage.deliver_message(
            instance_id=instance_id,
            channel="test_channel",
            data={"message": "hello"},
            metadata=metadata,
            worker_id="worker-1",
        )

        assert result is not None

        # Verify metadata in history
        history = await sqlite_storage.get_history(instance_id)
        assert len(history) == 1
        assert history[0]["event_data"]["metadata"] == metadata

    async def test_concurrent_delivery_only_one_succeeds(self, sqlite_storage, workflow_instance):
        """Test that concurrent delivery attempts result in only one success."""
        instance_id = workflow_instance

        # Simulate sequential delivery attempts from multiple workers
        # (SQLite doesn't handle true concurrent writes well, so test sequentially)
        results = []
        for worker_id in ["worker-1", "worker-2", "worker-3"]:
            result = await sqlite_storage.deliver_message(
                instance_id=instance_id,
                channel="test_channel",
                data={"message": f"from {worker_id}"},
                metadata={},
                worker_id=worker_id,
            )
            results.append(result)

        # Only one should succeed (non-None result) because:
        # - First delivery succeeds and removes the subscription
        # - Subsequent deliveries find no subscription
        successful = [r for r in results if r is not None]
        assert len(successful) == 1

        # Verify only one history entry
        history = await sqlite_storage.get_history(instance_id)
        assert len(history) == 1
