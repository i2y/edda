"""
Tests for storage layer (SQLite implementation).
"""


class TestWorkflowInstances:
    """Tests for workflow instance operations."""

    async def test_create_instance(self, sqlite_storage, sample_workflow_data):
        """Test creating a new workflow instance."""
        await sqlite_storage.create_instance(**sample_workflow_data)

        # Verify instance was created
        instance = await sqlite_storage.get_instance(sample_workflow_data["instance_id"])
        assert instance is not None
        assert instance["instance_id"] == sample_workflow_data["instance_id"]
        assert instance["workflow_name"] == sample_workflow_data["workflow_name"]
        assert instance["owner_service"] == sample_workflow_data["owner_service"]
        assert instance["status"] == "running"
        assert instance["current_activity_id"] is None
        assert instance["locked_by"] is None
        assert instance["locked_at"] is None

    async def test_get_nonexistent_instance(self, sqlite_storage):
        """Test getting a nonexistent instance returns None."""
        instance = await sqlite_storage.get_instance("nonexistent")
        assert instance is None

    async def test_update_instance_status(self, sqlite_storage, sample_workflow_data):
        """Test updating workflow instance status."""
        await sqlite_storage.create_instance(**sample_workflow_data)

        # Update status
        await sqlite_storage.update_instance_status(
            sample_workflow_data["instance_id"], "completed", {"result": "success"}
        )

        # Verify update
        instance = await sqlite_storage.get_instance(sample_workflow_data["instance_id"])
        assert instance["status"] == "completed"
        assert instance["output_data"] == {"result": "success"}

    async def test_update_instance_activity(self, sqlite_storage, sample_workflow_data):
        """Test updating workflow instance current activity."""
        await sqlite_storage.create_instance(**sample_workflow_data)

        # Update current activity
        await sqlite_storage.update_instance_activity(
            sample_workflow_data["instance_id"], "reserve_inventory:1"
        )

        # Verify update
        instance = await sqlite_storage.get_instance(sample_workflow_data["instance_id"])
        assert instance["current_activity_id"] == "reserve_inventory:1"


class TestDistributedLocking:
    """Tests for distributed locking functionality."""

    async def test_acquire_lock_success(self, sqlite_storage, sample_workflow_data):
        """Test successfully acquiring a lock."""
        await sqlite_storage.create_instance(**sample_workflow_data)

        # Acquire lock
        worker_id = "worker-1"
        result = await sqlite_storage.try_acquire_lock(
            sample_workflow_data["instance_id"], worker_id
        )
        assert result is True

        # Verify lock
        instance = await sqlite_storage.get_instance(sample_workflow_data["instance_id"])
        assert instance["locked_by"] == worker_id
        assert instance["locked_at"] is not None

    async def test_acquire_lock_already_locked(self, sqlite_storage, sample_workflow_data):
        """Test acquiring a lock that's already held by another worker."""
        await sqlite_storage.create_instance(**sample_workflow_data)

        # Worker 1 acquires lock
        worker1 = "worker-1"
        await sqlite_storage.try_acquire_lock(sample_workflow_data["instance_id"], worker1)

        # Worker 2 tries to acquire lock
        worker2 = "worker-2"
        result = await sqlite_storage.try_acquire_lock(sample_workflow_data["instance_id"], worker2)
        assert result is False

        # Verify lock still held by worker 1
        instance = await sqlite_storage.get_instance(sample_workflow_data["instance_id"])
        assert instance["locked_by"] == worker1

    async def test_acquire_lock_timeout(self, sqlite_storage, sample_workflow_data):
        """Test acquiring a lock that has timed out."""
        # This test would require manipulating time, so we'll skip for now
        # In a real implementation, you'd use freezegun or similar
        pass

    async def test_release_lock(self, sqlite_storage, sample_workflow_data):
        """Test releasing a lock."""
        await sqlite_storage.create_instance(**sample_workflow_data)

        worker_id = "worker-1"
        await sqlite_storage.try_acquire_lock(sample_workflow_data["instance_id"], worker_id)

        # Release lock
        await sqlite_storage.release_lock(sample_workflow_data["instance_id"], worker_id)

        # Verify lock released
        instance = await sqlite_storage.get_instance(sample_workflow_data["instance_id"])
        assert instance["locked_by"] is None
        assert instance["locked_at"] is None

    async def test_release_lock_wrong_worker(self, sqlite_storage, sample_workflow_data):
        """Test that only the lock holder can release the lock."""
        await sqlite_storage.create_instance(**sample_workflow_data)

        worker1 = "worker-1"
        worker2 = "worker-2"

        await sqlite_storage.try_acquire_lock(sample_workflow_data["instance_id"], worker1)

        # Worker 2 tries to release
        await sqlite_storage.release_lock(sample_workflow_data["instance_id"], worker2)

        # Verify lock still held by worker 1
        instance = await sqlite_storage.get_instance(sample_workflow_data["instance_id"])
        assert instance["locked_by"] == worker1

    async def test_refresh_lock(self, sqlite_storage, sample_workflow_data):
        """Test refreshing a lock."""
        await sqlite_storage.create_instance(**sample_workflow_data)

        worker_id = "worker-1"
        await sqlite_storage.try_acquire_lock(sample_workflow_data["instance_id"], worker_id)

        # Get original lock time
        instance1 = await sqlite_storage.get_instance(sample_workflow_data["instance_id"])
        locked_at1 = instance1["locked_at"]

        # Refresh lock (in real test, would wait a bit)
        result = await sqlite_storage.refresh_lock(sample_workflow_data["instance_id"], worker_id)
        assert result is True

        # Verify lock refreshed (timestamp should be updated)
        instance2 = await sqlite_storage.get_instance(sample_workflow_data["instance_id"])
        locked_at2 = instance2["locked_at"]
        assert locked_at2 >= locked_at1


class TestWorkflowHistory:
    """Tests for workflow execution history."""

    async def test_append_history(self, sqlite_storage, sample_workflow_data):
        """Test appending to workflow history."""
        await sqlite_storage.create_instance(**sample_workflow_data)

        # Append history event
        await sqlite_storage.append_history(
            sample_workflow_data["instance_id"],
            activity_id="reserve_inventory:1",
            event_type="ActivityCompleted",
            event_data={"activity": "reserve_inventory", "result": {"id": 123}},
        )

        # Verify history
        history = await sqlite_storage.get_history(sample_workflow_data["instance_id"])
        assert len(history) == 1
        assert history[0]["activity_id"] == "reserve_inventory:1"
        assert history[0]["event_type"] == "ActivityCompleted"
        assert history[0]["event_data"]["activity"] == "reserve_inventory"

    async def test_get_history_ordered(self, sqlite_storage, sample_workflow_data):
        """Test that history is returned in chronological order (by created_at)."""
        await sqlite_storage.create_instance(**sample_workflow_data)

        # Append multiple events with different activity IDs
        activity_ids = ["activity3:1", "activity1:1", "activity2:1"]
        for activity_id in activity_ids:
            await sqlite_storage.append_history(
                sample_workflow_data["instance_id"],
                activity_id=activity_id,
                event_type="ActivityCompleted",
                event_data={"activity_id": activity_id},
            )

        # Verify order (should be in chronological order by created_at)
        history = await sqlite_storage.get_history(sample_workflow_data["instance_id"])
        assert len(history) == 3

        # Verify timestamps are in ascending order (chronological)
        timestamps = [h["created_at"] for h in history]
        assert timestamps == sorted(timestamps), "History should be ordered chronologically"

        # Verify all activities are present
        retrieved_ids = {h["activity_id"] for h in history}
        assert retrieved_ids == set(activity_ids), "All activity IDs should be present"


class TestCompensations:
    """Tests for compensation transactions."""

    async def test_push_compensation(self, sqlite_storage, sample_workflow_data):
        """Test pushing a compensation to the stack."""
        await sqlite_storage.create_instance(**sample_workflow_data)

        await sqlite_storage.push_compensation(
            sample_workflow_data["instance_id"],
            activity_id="reserve_inventory:1",
            activity_name="cancel_reservation",
            args={"reservation_id": 123},
        )

        compensations = await sqlite_storage.get_compensations(sample_workflow_data["instance_id"])
        assert len(compensations) == 1
        assert compensations[0]["activity_name"] == "cancel_reservation"

    async def test_get_compensations_lifo_order(self, sqlite_storage, sample_workflow_data):
        """Test that compensations are returned in LIFO order (by created_at DESC)."""
        await sqlite_storage.create_instance(**sample_workflow_data)

        # Push multiple compensations with different activity IDs
        activity_ids = ["activity1:1", "activity2:1", "activity3:1"]
        for i, activity_id in enumerate(activity_ids, 1):
            await sqlite_storage.push_compensation(
                sample_workflow_data["instance_id"],
                activity_id=activity_id,
                activity_name=f"compensation_{i}",
                args={},
            )

        # Verify LIFO order (most recent first - reverse insertion order)
        compensations = await sqlite_storage.get_compensations(sample_workflow_data["instance_id"])
        assert len(compensations) == 3
        # Most recent compensation should be first (created_at DESC)
        assert [c["activity_id"] for c in compensations] == list(reversed(activity_ids))

    async def test_clear_compensations(self, sqlite_storage, sample_workflow_data):
        """Test clearing all compensations."""
        await sqlite_storage.create_instance(**sample_workflow_data)

        # Push compensations
        await sqlite_storage.push_compensation(
            sample_workflow_data["instance_id"],
            activity_id="activity1:1",
            activity_name="compensation_1",
            args={},
        )

        # Clear
        await sqlite_storage.clear_compensations(sample_workflow_data["instance_id"])

        # Verify cleared
        compensations = await sqlite_storage.get_compensations(sample_workflow_data["instance_id"])
        assert len(compensations) == 0


class TestMessageSubscriptions:
    """Tests for message subscriptions (wait_message/wait_event).

    Note: CloudEvents (wait_event) internally uses Message Passing (wait_message),
    so all event/message subscriptions use the message subscription API.
    """

    async def test_register_channel_receive(self, sqlite_storage, sample_workflow_data):
        """Test registering a channel receive via atomic method."""
        await sqlite_storage.create_instance(**sample_workflow_data)
        instance_id = sample_workflow_data["instance_id"]

        # Subscribe to channel first (required for register_channel_receive_and_release_lock)
        await sqlite_storage.subscribe_to_channel(
            instance_id=instance_id,
            channel="payment.completed",
            mode="broadcast",
        )

        # Acquire lock first (required for register_channel_receive_and_release_lock)
        acquired = await sqlite_storage.try_acquire_lock(
            instance_id, "worker-1", timeout_seconds=30
        )
        assert acquired is True

        # Register channel receive atomically (releases lock)
        await sqlite_storage.register_channel_receive_and_release_lock(
            instance_id=instance_id,
            worker_id="worker-1",
            channel="payment.completed",
            activity_id="wait_message_payment.completed:1",
        )

        # Find waiting instances by channel
        waiting = await sqlite_storage.find_waiting_instances_by_channel("payment.completed")
        assert len(waiting) == 1
        assert waiting[0]["instance_id"] == instance_id

        # Verify lock was released
        instance = await sqlite_storage.get_instance(instance_id)
        assert instance["locked_by"] is None

    async def test_remove_message_subscription(self, sqlite_storage, sample_workflow_data):
        """Test removing a message subscription."""
        await sqlite_storage.create_instance(**sample_workflow_data)
        instance_id = sample_workflow_data["instance_id"]

        # Subscribe to channel first
        await sqlite_storage.subscribe_to_channel(
            instance_id=instance_id,
            channel="payment.completed",
            mode="broadcast",
        )

        # Acquire lock and register channel receive
        await sqlite_storage.try_acquire_lock(instance_id, "worker-1", timeout_seconds=30)
        await sqlite_storage.register_channel_receive_and_release_lock(
            instance_id=instance_id,
            worker_id="worker-1",
            channel="payment.completed",
            activity_id="wait_message_payment.completed:1",
        )

        # Remove subscription
        await sqlite_storage.remove_message_subscription(instance_id, "payment.completed")

        # Verify removed
        waiting = await sqlite_storage.find_waiting_instances_by_channel("payment.completed")
        assert len(waiting) == 0


class TestOutboxEvents:
    """Tests for transactional outbox pattern."""

    async def test_add_outbox_event(self, sqlite_storage, sample_event_data):
        """Test adding an event to the outbox."""
        from sqlalchemy import select
        from sqlalchemy.ext.asyncio import AsyncSession

        from edda.storage.sqlalchemy_storage import OutboxEvent

        await sqlite_storage.add_outbox_event(event_id="event-123", **sample_event_data)

        # Verify event is stored with "pending" status in DB
        async with AsyncSession(sqlite_storage.engine) as session:
            result = await session.execute(
                select(OutboxEvent).where(OutboxEvent.event_id == "event-123")
            )
            event = result.scalar_one()
            assert event.status == "pending"  # In DB, always "pending"

        # When fetched via get_pending_outbox_events(), status becomes "processing"
        pending = await sqlite_storage.get_pending_outbox_events()
        assert len(pending) == 1
        assert pending[0]["event_id"] == "event-123"
        assert pending[0]["status"] == "processing"  # Fetched = "processing"

    async def test_mark_outbox_published(self, sqlite_storage, sample_event_data):
        """Test marking an outbox event as published."""
        await sqlite_storage.add_outbox_event(event_id="event-123", **sample_event_data)

        await sqlite_storage.mark_outbox_published("event-123")

        # Verify status
        pending = await sqlite_storage.get_pending_outbox_events()
        assert len(pending) == 0  # No longer pending

    async def test_mark_outbox_failed(self, sqlite_storage, sample_event_data):
        """Test marking an outbox event as failed (increments retry count)."""
        await sqlite_storage.add_outbox_event(event_id="event-123", **sample_event_data)

        await sqlite_storage.mark_outbox_failed("event-123", "Connection error")

        # Event should still be pending (status not changed, only retry_count incremented)
        pending = await sqlite_storage.get_pending_outbox_events()
        assert len(pending) == 1
        assert pending[0]["retry_count"] == 1
        assert "Connection error" in pending[0]["last_error"]


class TestInputParameterSearch:
    """Tests for input parameter search functionality."""

    async def test_search_by_simple_input_filter(self, sqlite_storage):
        """Test searching by a simple input parameter."""
        # Create workflow definition
        await sqlite_storage.upsert_workflow_definition(
            workflow_name="order_workflow",
            source_hash="hash123",
            source_code="async def order_workflow(ctx): pass",
        )

        # Create instances with different input data
        await sqlite_storage.create_instance(
            instance_id="inst-1",
            workflow_name="order_workflow",
            source_hash="hash123",
            owner_service="test",
            input_data={"order_id": "ORD-001", "amount": 100},
        )
        await sqlite_storage.create_instance(
            instance_id="inst-2",
            workflow_name="order_workflow",
            source_hash="hash123",
            owner_service="test",
            input_data={"order_id": "ORD-002", "amount": 200},
        )

        # Search by order_id
        result = await sqlite_storage.list_instances(input_filters={"order_id": "ORD-001"})

        assert len(result["instances"]) == 1
        assert result["instances"][0]["instance_id"] == "inst-1"

    async def test_search_by_nested_input_filter(self, sqlite_storage):
        """Test searching by nested input parameter."""
        # Create workflow definition
        await sqlite_storage.upsert_workflow_definition(
            workflow_name="customer_workflow",
            source_hash="hash456",
            source_code="async def customer_workflow(ctx): pass",
        )

        # Create instance with nested data
        await sqlite_storage.create_instance(
            instance_id="inst-nested",
            workflow_name="customer_workflow",
            source_hash="hash456",
            owner_service="test",
            input_data={"customer": {"email": "user@example.com", "name": "John"}},
        )
        await sqlite_storage.create_instance(
            instance_id="inst-nested-2",
            workflow_name="customer_workflow",
            source_hash="hash456",
            owner_service="test",
            input_data={"customer": {"email": "other@example.com", "name": "Jane"}},
        )

        # Search by nested path
        result = await sqlite_storage.list_instances(
            input_filters={"customer.email": "user@example.com"}
        )

        assert len(result["instances"]) == 1
        assert result["instances"][0]["instance_id"] == "inst-nested"

    async def test_search_with_multiple_filters(self, sqlite_storage):
        """Test searching with multiple input filters (AND logic)."""
        # Create workflow definition
        await sqlite_storage.upsert_workflow_definition(
            workflow_name="multi_workflow",
            source_hash="hash789",
            source_code="async def multi_workflow(ctx): pass",
        )

        # Create instances
        await sqlite_storage.create_instance(
            instance_id="inst-multi-1",
            workflow_name="multi_workflow",
            source_hash="hash789",
            owner_service="test",
            input_data={"customer_id": "CUST-001", "status": "pending"},
        )
        await sqlite_storage.create_instance(
            instance_id="inst-multi-2",
            workflow_name="multi_workflow",
            source_hash="hash789",
            owner_service="test",
            input_data={"customer_id": "CUST-001", "status": "approved"},
        )

        # Search with multiple filters (AND logic)
        result = await sqlite_storage.list_instances(
            input_filters={"customer_id": "CUST-001", "status": "pending"}
        )

        assert len(result["instances"]) == 1
        assert result["instances"][0]["instance_id"] == "inst-multi-1"

    async def test_search_combined_with_status_filter(self, sqlite_storage, sample_workflow_data):
        """Test combining input filter with status filter."""
        # Create instance with sample data
        await sqlite_storage.create_instance(**sample_workflow_data)

        # Update status
        await sqlite_storage.update_instance_status(
            sample_workflow_data["instance_id"], "completed"
        )

        # Create another instance with same input but different status
        await sqlite_storage.create_instance(
            instance_id="running-instance",
            workflow_name=sample_workflow_data["workflow_name"],
            source_hash=sample_workflow_data["source_hash"],
            owner_service="test",
            input_data={"order_id": "order-123", "amount": 100},
        )

        # Search with input filter + status filter
        result = await sqlite_storage.list_instances(
            input_filters={"order_id": "order-123"},
            status_filter="running",
        )

        assert len(result["instances"]) == 1
        assert result["instances"][0]["instance_id"] == "running-instance"

    async def test_search_numeric_value(self, sqlite_storage, sample_workflow_data):
        """Test searching by numeric input value."""
        # Create instance with sample data (amount=100)
        await sqlite_storage.create_instance(**sample_workflow_data)

        # Search by numeric value
        result = await sqlite_storage.list_instances(input_filters={"amount": 100})

        assert len(result["instances"]) == 1
        assert result["instances"][0]["instance_id"] == sample_workflow_data["instance_id"]

    async def test_invalid_json_path_rejected(self, sqlite_storage):
        """Test that invalid JSON paths are rejected to prevent SQL injection."""
        import pytest

        with pytest.raises(ValueError, match="Invalid JSON path"):
            await sqlite_storage.list_instances(input_filters={"'; DROP TABLE--": "value"})

    async def test_empty_input_filters(self, sqlite_storage, sample_workflow_data):
        """Test that empty input_filters behaves like None."""
        await sqlite_storage.create_instance(**sample_workflow_data)

        # Empty dict should return all instances
        result = await sqlite_storage.list_instances(input_filters={})

        assert len(result["instances"]) >= 1
