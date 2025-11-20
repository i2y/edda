"""
Tests for WorkflowContext module.

Tests cover:
- Context initialization
- History loading and caching
- Activity ID tracking and generation
- Activity result recording
- Instance status updates
"""

import pytest
import pytest_asyncio

from edda.context import WorkflowContext


@pytest.mark.asyncio
class TestWorkflowContext:
    """Test suite for WorkflowContext."""

    @pytest_asyncio.fixture
    async def workflow_instance(self, sqlite_storage, create_test_instance):
        """Create a workflow instance for testing."""
        instance_id = "test-workflow-instance-001"
        await create_test_instance(
            instance_id=instance_id,
            workflow_name="test_workflow",
            owner_service="test-service",
            input_data={"test": "data"},
        )
        # Update status to 'running' after creation
        await sqlite_storage.update_instance_status(instance_id, "running")
        return instance_id

    async def test_context_initialization(self, sqlite_storage, create_test_instance):
        """Test WorkflowContext initialization."""
        ctx = WorkflowContext(
            instance_id="test-001",
            workflow_name="test_workflow",
            storage=sqlite_storage,
            worker_id="worker-1",
            is_replaying=False,
        )

        assert ctx.instance_id == "test-001"
        assert ctx.workflow_name == "test_workflow"
        assert ctx.worker_id == "worker-1"
        assert ctx.is_replaying is False
        assert ctx.executed_activity_ids == set()
        assert ctx._history_cache == {}
        assert ctx._history_loaded is False

    async def test_context_initialization_replay_mode(self, sqlite_storage, create_test_instance):
        """Test WorkflowContext initialization in replay mode."""
        ctx = WorkflowContext(
            instance_id="test-002",
            workflow_name="test_workflow",
            storage=sqlite_storage,
            worker_id="worker-1",
            is_replaying=True,
        )

        assert ctx.is_replaying is True

    async def test_generate_activity_id(self, sqlite_storage, create_test_instance):
        """Test activity ID auto-generation."""
        ctx = WorkflowContext(
            instance_id="test-003",
            workflow_name="test_workflow",
            storage=sqlite_storage,
            worker_id="worker-1",
        )

        # First call to same activity generates :1
        activity_id_1 = ctx._generate_activity_id("test_activity")
        assert activity_id_1 == "test_activity:1"
        assert ctx._activity_call_counters["test_activity"] == 1

        # Second call generates :2
        activity_id_2 = ctx._generate_activity_id("test_activity")
        assert activity_id_2 == "test_activity:2"
        assert ctx._activity_call_counters["test_activity"] == 2

        # Different activity starts at :1
        activity_id_3 = ctx._generate_activity_id("another_activity")
        assert activity_id_3 == "another_activity:1"
        assert ctx._activity_call_counters["another_activity"] == 1

    async def test_record_activity_completed(
        self, sqlite_storage, workflow_instance, create_test_instance
    ):
        """Test recording successful activity completion."""
        ctx = WorkflowContext(
            instance_id=workflow_instance,
            workflow_name="test_workflow",
            storage=sqlite_storage,
            worker_id="worker-1",
        )

        result = {"data": "test_result", "count": 42}
        await ctx._record_activity_completed(
            activity_id="test_activity:1", activity_name="test_activity", result=result
        )

        # Verify history was recorded
        history = await sqlite_storage.get_history(workflow_instance)
        assert len(history) == 1

        event = history[0]
        assert event["activity_id"] == "test_activity:1"
        assert event["event_type"] == "ActivityCompleted"
        assert event["event_data"]["activity_name"] == "test_activity"
        assert event["event_data"]["result"] == result

        # Verify current activity was updated
        instance = await sqlite_storage.get_instance(workflow_instance)
        assert instance["current_activity_id"] == "test_activity:1"

    async def test_record_activity_failed(
        self, sqlite_storage, workflow_instance, create_test_instance
    ):
        """Test recording activity failure."""
        ctx = WorkflowContext(
            instance_id=workflow_instance,
            workflow_name="test_workflow",
            storage=sqlite_storage,
            worker_id="worker-1",
        )

        error = ValueError("Test error message")
        await ctx._record_activity_failed(
            activity_id="test_activity:1", activity_name="test_activity", error=error
        )

        # Verify history was recorded
        history = await sqlite_storage.get_history(workflow_instance)
        assert len(history) == 1

        event = history[0]
        assert event["activity_id"] == "test_activity:1"
        assert event["event_type"] == "ActivityFailed"
        assert event["event_data"]["activity_name"] == "test_activity"
        assert event["event_data"]["error_type"] == "ValueError"
        assert event["event_data"]["error_message"] == "Test error message"

    async def test_load_history_empty(
        self, sqlite_storage, workflow_instance, create_test_instance
    ):
        """Test loading history when no events exist."""
        ctx = WorkflowContext(
            instance_id=workflow_instance,
            workflow_name="test_workflow",
            storage=sqlite_storage,
            worker_id="worker-1",
            is_replaying=True,
        )

        await ctx._load_history()

        assert ctx._history_loaded is True
        assert ctx._history_cache == {}

    async def test_load_history_with_completed_activities(
        self, sqlite_storage, workflow_instance, create_test_instance
    ):
        """Test loading history with completed activities."""
        # Add some history events
        await sqlite_storage.append_history(
            workflow_instance,
            activity_id="activity1:1",
            event_type="ActivityCompleted",
            event_data={"activity_name": "activity1", "result": {"value": 10}},
        )
        await sqlite_storage.append_history(
            workflow_instance,
            activity_id="activity2:1",
            event_type="ActivityCompleted",
            event_data={"activity_name": "activity2", "result": {"value": 20}},
        )

        ctx = WorkflowContext(
            instance_id=workflow_instance,
            workflow_name="test_workflow",
            storage=sqlite_storage,
            worker_id="worker-1",
            is_replaying=True,
        )

        await ctx._load_history()

        assert ctx._history_loaded is True
        assert len(ctx._history_cache) == 2
        assert ctx._history_cache["activity1:1"] == {"value": 10}
        assert ctx._history_cache["activity2:1"] == {"value": 20}

    async def test_load_history_with_failed_activities(
        self, sqlite_storage, workflow_instance, create_test_instance
    ):
        """Test loading history with failed activities."""
        # Add a failed activity event
        await sqlite_storage.append_history(
            workflow_instance,
            activity_id="activity1:1",
            event_type="ActivityFailed",
            event_data={
                "activity_name": "activity1",
                "error_type": "ValueError",
                "error_message": "Test error",
            },
        )

        ctx = WorkflowContext(
            instance_id=workflow_instance,
            workflow_name="test_workflow",
            storage=sqlite_storage,
            worker_id="worker-1",
            is_replaying=True,
        )

        await ctx._load_history()

        assert ctx._history_loaded is True
        assert len(ctx._history_cache) == 1
        cached = ctx._history_cache["activity1:1"]
        assert cached["_error"] is True
        assert cached["error_type"] == "ValueError"
        assert cached["error_message"] == "Test error"

    async def test_load_history_only_once(
        self, sqlite_storage, workflow_instance, create_test_instance
    ):
        """Test that history is only loaded once."""
        ctx = WorkflowContext(
            instance_id=workflow_instance,
            workflow_name="test_workflow",
            storage=sqlite_storage,
            worker_id="worker-1",
            is_replaying=True,
        )

        # Load history first time
        await ctx._load_history()
        assert ctx._history_loaded is True

        # Add history after loading (should not be picked up on second load)
        await sqlite_storage.append_history(
            workflow_instance,
            activity_id="new_activity:1",
            event_type="ActivityCompleted",
            event_data={"activity_name": "new_activity", "result": {"value": 99}},
        )

        # Load history second time (should be no-op)
        await ctx._load_history()

        # Cache should still be empty since history was already loaded
        assert ctx._history_cache == {}

    async def test_get_cached_result_found(self, sqlite_storage, create_test_instance):
        """Test getting cached result when it exists."""
        ctx = WorkflowContext(
            instance_id="test-004",
            workflow_name="test_workflow",
            storage=sqlite_storage,
            worker_id="worker-1",
        )

        # Manually add to cache
        ctx._history_cache["test_activity:1"] = {"result": "test_value"}

        found, result = ctx._get_cached_result("test_activity:1")
        assert found is True
        assert result == {"result": "test_value"}

    async def test_get_cached_result_not_found(self, sqlite_storage, create_test_instance):
        """Test getting cached result when it doesn't exist."""
        ctx = WorkflowContext(
            instance_id="test-005",
            workflow_name="test_workflow",
            storage=sqlite_storage,
            worker_id="worker-1",
        )

        found, result = ctx._get_cached_result("test_activity:1")
        assert found is False
        assert result is None

    async def test_get_instance(self, sqlite_storage, workflow_instance, create_test_instance):
        """Test getting workflow instance metadata."""
        ctx = WorkflowContext(
            instance_id=workflow_instance,
            workflow_name="test_workflow",
            storage=sqlite_storage,
            worker_id="worker-1",
        )

        instance = await ctx._get_instance()
        assert instance is not None
        assert instance["instance_id"] == workflow_instance
        assert instance["workflow_name"] == "test_workflow"
        assert instance["status"] == "running"

    async def test_update_status(self, sqlite_storage, workflow_instance, create_test_instance):
        """Test updating workflow instance status."""
        ctx = WorkflowContext(
            instance_id=workflow_instance,
            workflow_name="test_workflow",
            storage=sqlite_storage,
            worker_id="worker-1",
        )

        await ctx._update_status("completed", {"result": "success"})

        instance = await sqlite_storage.get_instance(workflow_instance)
        assert instance["status"] == "completed"
        assert instance["output_data"] == {"result": "success"}

    async def test_update_status_without_output(
        self, sqlite_storage, workflow_instance, create_test_instance
    ):
        """Test updating status without output data."""
        ctx = WorkflowContext(
            instance_id=workflow_instance,
            workflow_name="test_workflow",
            storage=sqlite_storage,
            worker_id="worker-1",
        )

        await ctx._update_status("waiting_for_event")

        instance = await sqlite_storage.get_instance(workflow_instance)
        assert instance["status"] == "waiting_for_event"

    async def test_context_repr(self, sqlite_storage, create_test_instance):
        """Test string representation of context."""
        ctx = WorkflowContext(
            instance_id="test-007",
            workflow_name="my_workflow",
            storage=sqlite_storage,
            worker_id="worker-1",
            is_replaying=True,
        )
        # Track some activities
        ctx._record_activity_id("activity1:1")
        ctx._record_activity_id("activity2:1")
        ctx._record_activity_id("activity3:1")

        repr_str = repr(ctx)
        assert "test-007" in repr_str
        assert "my_workflow" in repr_str
        assert "executed_activities=3" in repr_str
        assert "is_replaying=True" in repr_str
