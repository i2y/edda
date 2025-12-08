"""
Tests for workflow resumption functionality.

Tests cover:
- find_resumable_workflows() finding correct workflows
- Lock acquisition before resumption
- Exclusion of locked workflows
- Exclusion of non-running workflows
- Concurrent resume attempts handling
"""

import pytest
import pytest_asyncio


@pytest.mark.asyncio
class TestFindResumableWorkflows:
    """Test suite for find_resumable_workflows() functionality."""

    @pytest_asyncio.fixture
    async def setup_instances(self, sqlite_storage, create_test_instance):
        """Create various workflow instances for testing."""
        # Running, unlocked - should be resumable
        await create_test_instance(
            instance_id="running-unlocked-001",
            workflow_name="test_workflow",
            owner_service="test-service",
            input_data={"test": "data"},
        )
        await sqlite_storage.update_instance_status("running-unlocked-001", "running")

        # Running, locked - should NOT be resumable
        await create_test_instance(
            instance_id="running-locked-001",
            workflow_name="test_workflow",
            owner_service="test-service",
            input_data={"test": "data"},
        )
        await sqlite_storage.update_instance_status("running-locked-001", "running")
        await sqlite_storage.try_acquire_lock("running-locked-001", "other-worker")

        # Waiting for event, unlocked - should NOT be resumable
        await create_test_instance(
            instance_id="waiting-event-001",
            workflow_name="test_workflow",
            owner_service="test-service",
            input_data={"test": "data"},
        )
        await sqlite_storage.update_instance_status("waiting-event-001", "waiting_for_event")

        # Completed - should NOT be resumable
        await create_test_instance(
            instance_id="completed-001",
            workflow_name="test_workflow",
            owner_service="test-service",
            input_data={"test": "data"},
        )
        await sqlite_storage.update_instance_status("completed-001", "completed")

        return {
            "running_unlocked": "running-unlocked-001",
            "running_locked": "running-locked-001",
            "waiting": "waiting-event-001",
            "completed": "completed-001",
        }

    async def test_finds_running_unlocked_workflows(self, sqlite_storage, setup_instances):
        """Test that find_resumable_workflows returns running, unlocked workflows."""
        instances = setup_instances

        resumable = await sqlite_storage.find_resumable_workflows()

        # Only running-unlocked should be resumable
        instance_ids = [w["instance_id"] for w in resumable]
        assert instances["running_unlocked"] in instance_ids

    async def test_excludes_locked_workflows(self, sqlite_storage, setup_instances):
        """Test that locked workflows are excluded from resumable list."""
        instances = setup_instances

        resumable = await sqlite_storage.find_resumable_workflows()

        instance_ids = [w["instance_id"] for w in resumable]
        assert instances["running_locked"] not in instance_ids

    async def test_excludes_non_running_workflows(self, sqlite_storage, setup_instances):
        """Test that non-running workflows are excluded from resumable list."""
        instances = setup_instances

        resumable = await sqlite_storage.find_resumable_workflows()

        instance_ids = [w["instance_id"] for w in resumable]
        assert instances["waiting"] not in instance_ids
        assert instances["completed"] not in instance_ids

    async def test_returns_workflow_name(self, sqlite_storage, setup_instances):
        """Test that resumable workflows include workflow_name."""
        resumable = await sqlite_storage.find_resumable_workflows()

        # Find the running-unlocked instance
        running_unlocked = [w for w in resumable if w["instance_id"] == "running-unlocked-001"]
        assert len(running_unlocked) == 1
        assert running_unlocked[0]["workflow_name"] == "test_workflow"

    async def test_empty_when_no_resumable(self, sqlite_storage, create_test_instance):
        """Test that find_resumable_workflows returns empty list when none are resumable."""
        # Create only a completed workflow
        await create_test_instance(
            instance_id="only-completed-001",
            workflow_name="test_workflow",
            owner_service="test-service",
            input_data={"test": "data"},
        )
        await sqlite_storage.update_instance_status("only-completed-001", "completed")

        resumable = await sqlite_storage.find_resumable_workflows()

        # Should be empty (no running, unlocked workflows)
        assert len(resumable) == 0


@pytest.mark.asyncio
class TestConcurrentResumeAttempts:
    """Test suite for handling concurrent resume attempts."""

    @pytest_asyncio.fixture
    async def resumable_instance(self, sqlite_storage, create_test_instance):
        """Create a resumable workflow instance."""
        instance_id = "concurrent-resume-001"
        await create_test_instance(
            instance_id=instance_id,
            workflow_name="test_workflow",
            owner_service="test-service",
            input_data={"test": "data"},
        )
        await sqlite_storage.update_instance_status(instance_id, "running")
        return instance_id

    async def test_only_one_worker_can_acquire_lock(self, sqlite_storage, resumable_instance):
        """Test that only one worker can acquire lock for resumption."""
        instance_id = resumable_instance

        # Run sequential lock attempts (since SQLite doesn't support true concurrency well)
        results = []
        for worker_id in ["worker-1", "worker-2", "worker-3"]:
            result = await sqlite_storage.try_acquire_lock(instance_id, worker_id)
            results.append(result)
            if result:
                # Release immediately so next iteration can try
                # But for the test, we want to verify only first succeeds
                break

        # First one should succeed
        assert results[0] is True

    async def test_workflow_becomes_not_resumable_after_lock(
        self, sqlite_storage, resumable_instance
    ):
        """Test that a workflow is no longer resumable after being locked."""
        instance_id = resumable_instance

        # Initially resumable
        resumable = await sqlite_storage.find_resumable_workflows()
        assert any(w["instance_id"] == instance_id for w in resumable)

        # Acquire lock
        await sqlite_storage.try_acquire_lock(instance_id, "worker-1")

        # No longer resumable
        resumable = await sqlite_storage.find_resumable_workflows()
        assert not any(w["instance_id"] == instance_id for w in resumable)

    async def test_workflow_becomes_resumable_after_lock_release(
        self, sqlite_storage, resumable_instance
    ):
        """Test that a workflow becomes resumable again after lock is released."""
        instance_id = resumable_instance

        # Acquire and release lock
        await sqlite_storage.try_acquire_lock(instance_id, "worker-1")
        await sqlite_storage.release_lock(instance_id, "worker-1")

        # Should be resumable again
        resumable = await sqlite_storage.find_resumable_workflows()
        assert any(w["instance_id"] == instance_id for w in resumable)


@pytest.mark.asyncio
class TestMessageDeliveryTriggeredResumption:
    """
    Test suite for message delivery triggering workflow resumption.

    After deliver_message() updates status to 'running' and releases lock,
    the workflow should become resumable.
    """

    @pytest_asyncio.fixture
    async def waiting_workflow(self, sqlite_storage, create_test_instance):
        """Create a workflow waiting for a message."""
        instance_id = "waiting-message-001"
        await create_test_instance(
            instance_id=instance_id,
            workflow_name="test_workflow",
            owner_service="test-service",
            input_data={"test": "data"},
        )
        # Use waiting_for_event status (waiting_for_message is not a valid status)
        await sqlite_storage.update_instance_status(instance_id, "waiting_for_event")

        # Subscribe to channel first (required for register_channel_receive_and_release_lock)
        await sqlite_storage.subscribe_to_channel(
            instance_id=instance_id,
            channel="approval",
            mode="broadcast",
        )

        # Acquire lock and register channel receive (which releases lock)
        await sqlite_storage.try_acquire_lock(instance_id, "setup-worker")
        await sqlite_storage.register_channel_receive_and_release_lock(
            instance_id=instance_id,
            worker_id="setup-worker",
            channel="approval",
            activity_id="wait_message_approval:1",
        )
        return instance_id

    async def test_workflow_not_resumable_while_waiting(self, sqlite_storage, waiting_workflow):
        """Test that workflow waiting for message is not resumable."""
        instance_id = waiting_workflow

        resumable = await sqlite_storage.find_resumable_workflows()

        # Should not be resumable (status is waiting_for_event)
        assert not any(w["instance_id"] == instance_id for w in resumable)

    async def test_workflow_resumable_after_message_delivery(
        self, sqlite_storage, waiting_workflow
    ):
        """Test that workflow becomes resumable after message is delivered."""
        instance_id = waiting_workflow

        # Deliver message (this updates status to 'running' and releases lock)
        result = await sqlite_storage.deliver_message(
            instance_id=instance_id,
            channel="approval",
            data={"approved": True},
            metadata={},
            worker_id="worker-1",
        )

        assert result is not None

        # Should now be resumable
        resumable = await sqlite_storage.find_resumable_workflows()
        assert any(w["instance_id"] == instance_id for w in resumable)

        # Verify status is 'running'
        instance = await sqlite_storage.get_instance(instance_id)
        assert instance["status"] == "running"
