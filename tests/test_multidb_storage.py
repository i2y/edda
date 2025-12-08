"""
Multi-database tests for storage layer (PostgreSQL, MySQL, SQLite).

These tests use the `db_storage` parametrized fixture to run against all databases.
"""


class TestMultiDBWorkflowInstances:
    """Tests for workflow instance operations across all databases."""

    async def test_create_instance(self, db_storage, sample_workflow_data):
        """Test creating a new workflow instance."""
        await db_storage.create_instance(**sample_workflow_data)

        # Verify instance was created
        instance = await db_storage.get_instance(sample_workflow_data["instance_id"])
        assert instance is not None
        assert instance["instance_id"] == sample_workflow_data["instance_id"]
        assert instance["workflow_name"] == sample_workflow_data["workflow_name"]
        assert instance["owner_service"] == sample_workflow_data["owner_service"]
        assert instance["status"] == "running"
        assert instance["current_activity_id"] is None
        assert instance["locked_by"] is None
        assert instance["locked_at"] is None

    async def test_get_nonexistent_instance(self, db_storage):
        """Test getting a nonexistent instance returns None."""
        instance = await db_storage.get_instance("nonexistent")
        assert instance is None

    async def test_update_instance_status(self, db_storage, sample_workflow_data):
        """Test updating workflow instance status."""
        await db_storage.create_instance(**sample_workflow_data)

        # Update status
        await db_storage.update_instance_status(
            sample_workflow_data["instance_id"], "completed", {"result": "success"}
        )

        # Verify update
        instance = await db_storage.get_instance(sample_workflow_data["instance_id"])
        assert instance["status"] == "completed"
        assert instance["output_data"] == {"result": "success"}

    async def test_update_instance_activity(self, db_storage, sample_workflow_data):
        """Test updating workflow instance activity ID."""
        await db_storage.create_instance(**sample_workflow_data)

        # Update activity_id
        await db_storage.update_instance_activity(
            sample_workflow_data["instance_id"], "my_activity:5"
        )

        # Verify update
        instance = await db_storage.get_instance(sample_workflow_data["instance_id"])
        assert instance["current_activity_id"] == "my_activity:5"


class TestMultiDBDistributedLocking:
    """Tests for distributed locking functionality across all databases."""

    async def test_acquire_lock_success(self, db_storage, sample_workflow_data):
        """Test successfully acquiring a lock."""
        await db_storage.create_instance(**sample_workflow_data)

        # Acquire lock
        worker_id = "worker-1"
        result = await db_storage.try_acquire_lock(sample_workflow_data["instance_id"], worker_id)
        assert result is True

        # Verify lock
        instance = await db_storage.get_instance(sample_workflow_data["instance_id"])
        assert instance["locked_by"] == worker_id
        assert instance["locked_at"] is not None

    async def test_acquire_lock_already_locked(self, db_storage, sample_workflow_data):
        """Test acquiring a lock that's already held by another worker."""
        await db_storage.create_instance(**sample_workflow_data)

        # Worker 1 acquires lock
        worker1 = "worker-1"
        await db_storage.try_acquire_lock(sample_workflow_data["instance_id"], worker1)

        # Worker 2 tries to acquire lock
        worker2 = "worker-2"
        result = await db_storage.try_acquire_lock(sample_workflow_data["instance_id"], worker2)
        assert result is False

        # Verify lock still held by worker 1
        instance = await db_storage.get_instance(sample_workflow_data["instance_id"])
        assert instance["locked_by"] == worker1

    async def test_release_lock(self, db_storage, sample_workflow_data):
        """Test releasing a lock."""
        await db_storage.create_instance(**sample_workflow_data)

        # Acquire and release lock
        worker_id = "worker-1"
        await db_storage.try_acquire_lock(sample_workflow_data["instance_id"], worker_id)
        await db_storage.release_lock(sample_workflow_data["instance_id"], worker_id)

        # Verify lock released
        instance = await db_storage.get_instance(sample_workflow_data["instance_id"])
        assert instance["locked_by"] is None


class TestMultiDBHistory:
    """Tests for workflow history across all databases."""

    async def test_append_and_get_history(self, db_storage, sample_workflow_data):
        """Test appending and retrieving history."""
        await db_storage.create_instance(**sample_workflow_data)

        # Append history entries
        await db_storage.append_history(
            sample_workflow_data["instance_id"],
            activity_id="send_email:1",
            event_type="activity_scheduled",
            event_data={"activity": "send_email"},
        )
        await db_storage.append_history(
            sample_workflow_data["instance_id"],
            activity_id="send_email:2",
            event_type="activity_completed",
            event_data={"result": "success"},
        )

        # Retrieve history
        history = await db_storage.get_history(sample_workflow_data["instance_id"])
        assert len(history) == 2
        assert history[0]["event_type"] == "activity_scheduled"
        assert history[1]["event_type"] == "activity_completed"

    async def test_get_history_empty(self, db_storage, sample_workflow_data):
        """Test getting history for instance with no history."""
        await db_storage.create_instance(**sample_workflow_data)

        history = await db_storage.get_history(sample_workflow_data["instance_id"])
        assert len(history) == 0


class TestMultiDBMessageSubscriptions:
    """Tests for message subscriptions across all databases.

    Note: CloudEvents (wait_event) internally uses Message Passing (wait_message),
    so all event/message subscriptions use the message subscription API.
    """

    async def test_subscribe_and_find_waiting_workflows(self, db_storage, sample_workflow_data):
        """Test subscribing to messages and finding waiting workflows."""
        await db_storage.create_instance(**sample_workflow_data)
        instance_id = sample_workflow_data["instance_id"]

        # Subscribe to channel first (required for register_channel_receive_and_release_lock)
        await db_storage.subscribe_to_channel(
            instance_id=instance_id,
            channel="payment.completed",
            mode="broadcast",
        )

        # Acquire lock first (required for register_channel_receive_and_release_lock)
        acquired = await db_storage.try_acquire_lock(instance_id, "worker-1", timeout_seconds=30)
        assert acquired is True

        # Register channel receive atomically (releases lock)
        await db_storage.register_channel_receive_and_release_lock(
            instance_id=instance_id,
            worker_id="worker-1",
            channel="payment.completed",
            activity_id="wait_message_payment.completed:1",
        )

        # Find waiting workflows by channel
        workflows = await db_storage.find_waiting_instances_by_channel("payment.completed")
        assert len(workflows) == 1
        assert workflows[0]["instance_id"] == instance_id

    async def test_unsubscribe_from_message(self, db_storage, sample_workflow_data):
        """Test unsubscribing from messages."""
        await db_storage.create_instance(**sample_workflow_data)
        instance_id = sample_workflow_data["instance_id"]

        # Subscribe to channel first
        await db_storage.subscribe_to_channel(
            instance_id=instance_id,
            channel="payment.completed",
            mode="broadcast",
        )

        # Acquire lock and register channel receive
        await db_storage.try_acquire_lock(instance_id, "worker-1", timeout_seconds=30)
        await db_storage.register_channel_receive_and_release_lock(
            instance_id=instance_id,
            worker_id="worker-1",
            channel="payment.completed",
            activity_id="wait_message_payment.completed:1",
        )

        # Unsubscribe
        await db_storage.remove_message_subscription(instance_id, "payment.completed")

        # Verify unsubscribed
        workflows = await db_storage.find_waiting_instances_by_channel("payment.completed")
        assert len(workflows) == 0
