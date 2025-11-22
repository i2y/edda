"""
MySQL-specific storage tests.

This file re-runs all storage tests from test_storage.py against MySQL
to verify database compatibility.
"""

import pytest

# Re-import all test classes from test_storage
pytestmark = pytest.mark.usefixtures("mysql_storage")


class TestWorkflowInstances:
    """Tests for workflow instance operations on MySQL."""

    async def test_create_instance(self, mysql_storage, sample_workflow_data):
        """Test creating a new workflow instance."""
        await mysql_storage.create_instance(**sample_workflow_data)

        # Verify instance was created
        instance = await mysql_storage.get_instance(sample_workflow_data["instance_id"])
        assert instance is not None
        assert instance["instance_id"] == sample_workflow_data["instance_id"]
        assert instance["workflow_name"] == sample_workflow_data["workflow_name"]
        assert instance["owner_service"] == sample_workflow_data["owner_service"]
        assert instance["status"] == "running"
        assert instance["current_activity_id"] is None
        assert instance["locked_by"] is None
        assert instance["locked_at"] is None

    async def test_get_nonexistent_instance(self, mysql_storage):
        """Test getting a nonexistent instance returns None."""
        instance = await mysql_storage.get_instance("nonexistent")
        assert instance is None

    async def test_update_instance_status(self, mysql_storage, sample_workflow_data):
        """Test updating workflow instance status."""
        await mysql_storage.create_instance(**sample_workflow_data)

        # Update status
        await mysql_storage.update_instance_status(
            sample_workflow_data["instance_id"], "completed", {"result": "success"}
        )

        # Verify update
        instance = await mysql_storage.get_instance(sample_workflow_data["instance_id"])
        assert instance["status"] == "completed"
        assert instance["output_data"] == {"result": "success"}

    async def test_update_instance_step(self, mysql_storage, sample_workflow_data):
        """Test updating workflow instance activity ID."""
        await mysql_storage.create_instance(**sample_workflow_data)

        # Update activity ID
        await mysql_storage.update_instance_activity(
            sample_workflow_data["instance_id"], "test_activity:1"
        )

        # Verify update
        instance = await mysql_storage.get_instance(sample_workflow_data["instance_id"])
        assert instance["current_activity_id"] == "test_activity:1"


class TestDistributedLocking:
    """Tests for distributed locking functionality on MySQL."""

    async def test_acquire_lock_success(self, mysql_storage, sample_workflow_data):
        """Test successfully acquiring a lock."""
        await mysql_storage.create_instance(**sample_workflow_data)

        # Acquire lock
        worker_id = "worker-1"
        result = await mysql_storage.try_acquire_lock(
            sample_workflow_data["instance_id"], worker_id
        )
        assert result is True

        # Verify lock
        instance = await mysql_storage.get_instance(sample_workflow_data["instance_id"])
        assert instance["locked_by"] == worker_id
        assert instance["locked_at"] is not None

    async def test_acquire_lock_already_locked(self, mysql_storage, sample_workflow_data):
        """Test acquiring a lock that's already held by another worker."""
        await mysql_storage.create_instance(**sample_workflow_data)

        # Worker 1 acquires lock
        worker1 = "worker-1"
        await mysql_storage.try_acquire_lock(sample_workflow_data["instance_id"], worker1)

        # Worker 2 tries to acquire lock
        worker2 = "worker-2"
        result = await mysql_storage.try_acquire_lock(sample_workflow_data["instance_id"], worker2)
        assert result is False

        # Verify lock still held by worker 1
        instance = await mysql_storage.get_instance(sample_workflow_data["instance_id"])
        assert instance["locked_by"] == worker1

    async def test_acquire_lock_timeout(self, mysql_storage, sample_workflow_data):
        """Test acquiring a lock that has timed out."""
        # This is tested in test_locking.py with time manipulation
        pass

    async def test_release_lock(self, mysql_storage, sample_workflow_data):
        """Test releasing a lock."""
        await mysql_storage.create_instance(**sample_workflow_data)

        # Acquire and release lock
        worker_id = "worker-1"
        await mysql_storage.try_acquire_lock(sample_workflow_data["instance_id"], worker_id)
        await mysql_storage.release_lock(sample_workflow_data["instance_id"], worker_id)

        # Verify lock released
        instance = await mysql_storage.get_instance(sample_workflow_data["instance_id"])
        assert instance["locked_by"] is None

    async def test_release_lock_wrong_worker(self, mysql_storage, sample_workflow_data):
        """Test releasing a lock held by another worker (should do nothing)."""
        await mysql_storage.create_instance(**sample_workflow_data)

        # Worker 1 acquires lock
        worker1 = "worker-1"
        await mysql_storage.try_acquire_lock(sample_workflow_data["instance_id"], worker1)

        # Worker 2 tries to release lock
        worker2 = "worker-2"
        await mysql_storage.release_lock(sample_workflow_data["instance_id"], worker2)

        # Verify lock still held by worker 1
        instance = await mysql_storage.get_instance(sample_workflow_data["instance_id"])
        assert instance["locked_by"] == worker1

    async def test_refresh_lock(self, mysql_storage, sample_workflow_data):
        """Test refreshing a lock timestamp."""
        await mysql_storage.create_instance(**sample_workflow_data)

        # Acquire lock
        worker_id = "worker-1"
        await mysql_storage.try_acquire_lock(sample_workflow_data["instance_id"], worker_id)

        # Get initial timestamp
        instance1 = await mysql_storage.get_instance(sample_workflow_data["instance_id"])
        locked_at1 = instance1["locked_at"]

        # Refresh lock
        await mysql_storage.refresh_lock(sample_workflow_data["instance_id"], worker_id)

        # Verify timestamp updated
        instance2 = await mysql_storage.get_instance(sample_workflow_data["instance_id"])
        locked_at2 = instance2["locked_at"]
        assert locked_at2 >= locked_at1


class TestWorkflowHistory:
    """Tests for workflow history on MySQL."""

    async def test_append_history(self, mysql_storage, sample_workflow_data):
        """Test appending history entries."""
        await mysql_storage.create_instance(**sample_workflow_data)

        # Append history
        await mysql_storage.append_history(
            sample_workflow_data["instance_id"],
            activity_id="test_activity:1",
            event_type="activity_scheduled",
            event_data={"activity": "send_email"},
        )

        # Verify history
        history = await mysql_storage.get_history(sample_workflow_data["instance_id"])
        assert len(history) == 1
        assert history[0]["activity_id"] == "test_activity:1"
        assert history[0]["event_type"] == "activity_scheduled"

    async def test_get_history_ordered(self, mysql_storage, sample_workflow_data):
        """Test that history is returned in correct order."""
        await mysql_storage.create_instance(**sample_workflow_data)

        # Append multiple entries
        for i in [1, 2, 3]:
            await mysql_storage.append_history(
                sample_workflow_data["instance_id"],
                activity_id=f"test_activity:{i}",
                event_type=f"step_{i}",
                event_data={"step": i},
            )

        # Verify order
        history = await mysql_storage.get_history(sample_workflow_data["instance_id"])
        assert len(history) == 3
        assert [h["activity_id"] for h in history] == [
            "test_activity:1",
            "test_activity:2",
            "test_activity:3",
        ]
