"""
Tests for Activity decorator module.

Tests cover:
- Activity decorator functionality
- Activity execution during normal mode
- Activity execution during replay mode
- Result caching and retrieval
- Error handling and recording
"""

import pytest
import pytest_asyncio

from edda.activity import Activity, activity
from edda.context import WorkflowContext
from edda.exceptions import TerminalError


@pytest.mark.asyncio
class TestActivityDecorator:
    """Test suite for @activity decorator."""

    async def test_activity_decorator_marks_function(self):
        """Test that @activity decorator marks function as activity."""

        @activity
        async def test_activity(ctx: WorkflowContext, value: int) -> dict:
            return {"result": value * 2}

        assert hasattr(test_activity, "_is_activity")
        assert test_activity._is_activity is True

    async def test_activity_decorator_preserves_metadata(self):
        """Test that decorator preserves function metadata."""

        @activity
        async def my_test_activity(ctx: WorkflowContext) -> dict:
            """Test activity docstring."""
            return {}

        assert my_test_activity.__name__ == "my_test_activity"
        assert my_test_activity.__doc__ == "Test activity docstring."

    async def test_activity_decorator_supports_sync(self):
        """Test that decorator supports both sync and async functions."""

        @activity
        def sync_function(ctx: WorkflowContext) -> dict:
            return {"result": "sync"}

        # Verify it's a valid activity
        assert hasattr(sync_function, "_is_activity")
        assert sync_function._is_activity is True

        # Verify it's detected as a sync function
        assert hasattr(sync_function, "is_async")
        assert sync_function.is_async is False

    async def test_activity_decorator_creates_wrapper(self):
        """Test that decorator creates Activity wrapper."""

        @activity
        async def test_activity(ctx: WorkflowContext) -> dict:
            return {}

        assert isinstance(test_activity, Activity)


@pytest.mark.asyncio
class TestActivityExecution:
    """Test suite for Activity execution."""

    @pytest_asyncio.fixture
    async def workflow_instance(self, sqlite_storage, create_test_instance):
        """Create a workflow instance for testing."""
        instance_id = "test-activity-instance-001"
        await create_test_instance(
            instance_id=instance_id,
            workflow_name="test_workflow",
            input_data={"test": "data"},
        )
        # Update status to 'running' after creation
        await sqlite_storage.update_instance_status(instance_id, "running")
        return instance_id

    @pytest.fixture
    def context(self, sqlite_storage, workflow_instance):
        """Create a workflow context for testing."""
        return WorkflowContext(
            instance_id=workflow_instance,
            workflow_name="test_workflow",
            storage=sqlite_storage,
            worker_id="worker-1",
            is_replaying=False,
        )

    async def test_simple_activity_execution(self, context):
        """Test executing a simple activity."""

        @activity
        async def simple_activity(ctx: WorkflowContext, value: int) -> dict:
            return {"doubled": value * 2}

        result = await simple_activity(context, 21, activity_id="simple_activity:1")

        assert result == {"doubled": 42}
        assert len(context.executed_activity_ids) == 1
        assert "simple_activity:1" in context.executed_activity_ids

    async def test_activity_execution_records_history(
        self, context, sqlite_storage, workflow_instance
    ):
        """Test that activity execution records history."""

        @activity
        async def test_activity(ctx: WorkflowContext, name: str) -> dict:
            return {"message": f"Hello, {name}"}

        result = await test_activity(context, "Alice", activity_id="test_activity:1")

        assert result == {"message": "Hello, Alice"}

        # Verify history was recorded
        history = await sqlite_storage.get_history(workflow_instance)
        assert len(history) == 1

        event = history[0]
        assert event["activity_id"] == "test_activity:1"
        assert event["event_type"] == "ActivityCompleted"
        assert event["event_data"]["activity_name"] == "test_activity"
        assert event["event_data"]["result"] == {"message": "Hello, Alice"}

    async def test_activity_execution_tracks_activity_ids(self, context):
        """Test that each activity execution tracks activity IDs."""

        @activity
        async def activity1(ctx: WorkflowContext) -> dict:
            return {"executed": len(ctx.executed_activity_ids)}

        @activity
        async def activity2(ctx: WorkflowContext) -> dict:
            return {"executed": len(ctx.executed_activity_ids)}

        result1 = await activity1(context, activity_id="activity1:1")
        result2 = await activity2(context, activity_id="activity2:1")

        # Each activity was executed
        assert result1 == {"executed": 1}
        assert result2 == {"executed": 2}

        # Both activity IDs should be tracked
        assert len(context.executed_activity_ids) == 2
        assert "activity1:1" in context.executed_activity_ids
        assert "activity2:1" in context.executed_activity_ids

    async def test_activity_with_multiple_arguments(self, context):
        """Test activity with multiple arguments."""

        @activity
        async def multi_arg_activity(ctx: WorkflowContext, a: int, b: int, c: int) -> dict:
            return {"sum": a + b + c}

        result = await multi_arg_activity(context, 10, 20, 30)

        assert result == {"sum": 60}

    async def test_activity_with_keyword_arguments(self, context):
        """Test activity with keyword arguments."""

        @activity
        async def kwarg_activity(
            ctx: WorkflowContext, name: str, age: int, city: str = "Unknown"
        ) -> dict:
            return {"name": name, "age": age, "city": city}

        result = await kwarg_activity(context, name="Bob", age=25, city="Tokyo")

        assert result == {"name": "Bob", "age": 25, "city": "Tokyo"}


@pytest.mark.asyncio
class TestActivityReplay:
    """Test suite for Activity replay functionality."""

    @pytest_asyncio.fixture
    async def workflow_instance_with_history(self, sqlite_storage, create_test_instance):
        """Create a workflow instance with existing history."""
        instance_id = "test-replay-instance-001"
        await create_test_instance(
            instance_id=instance_id,
            workflow_name="test_workflow",
            input_data={"test": "data"},
        )
        # Update status after creation
        await sqlite_storage.update_instance_status(instance_id, "running")
        # Update current activity (latest activity executed)
        await sqlite_storage.update_instance_activity(instance_id, "activity2:1")

        # Add history events
        await sqlite_storage.append_history(
            instance_id,
            activity_id="activity1:1",
            event_type="ActivityCompleted",
            event_data={"activity_name": "activity1", "result": {"value": 100}},
        )
        await sqlite_storage.append_history(
            instance_id,
            activity_id="activity2:1",
            event_type="ActivityCompleted",
            event_data={"activity_name": "activity2", "result": {"value": 200}},
        )

        return instance_id

    @pytest_asyncio.fixture
    async def replay_context(self, sqlite_storage, workflow_instance_with_history):
        """Create a replay context with history loaded."""
        ctx = WorkflowContext(
            instance_id=workflow_instance_with_history,
            workflow_name="test_workflow",
            storage=sqlite_storage,
            worker_id="worker-1",
            is_replaying=True,
        )
        await ctx._load_history()
        return ctx

    async def test_activity_replay_returns_cached_result(self, replay_context):
        """Test that activity returns cached result during replay."""
        execution_count = 0

        @activity
        async def test_activity(ctx: WorkflowContext) -> dict:
            nonlocal execution_count
            execution_count += 1
            return {"computed": "new_value"}

        # Execute activity during replay with same activity_id as in history
        result = await test_activity(replay_context, activity_id="activity1:1")

        # Should return cached result, not execute function
        assert result == {"value": 100}
        assert execution_count == 0

    async def test_activity_replay_multiple_activities(self, replay_context):
        """Test replaying multiple activities."""

        @activity
        async def activity1(ctx: WorkflowContext) -> dict:
            return {"computed": "should_not_see_this"}

        @activity
        async def activity2(ctx: WorkflowContext) -> dict:
            return {"computed": "should_not_see_this_either"}

        result1 = await activity1(replay_context, activity_id="activity1:1")
        result2 = await activity2(replay_context, activity_id="activity2:1")

        assert result1 == {"value": 100}
        assert result2 == {"value": 200}

    async def test_activity_replay_does_not_record_history(
        self, replay_context, sqlite_storage, workflow_instance_with_history
    ):
        """Test that replayed activities don't record history."""

        @activity
        async def test_activity(ctx: WorkflowContext) -> dict:
            return {}

        # Execute during replay with existing activity_id
        await test_activity(replay_context, activity_id="activity1:1")

        # History should still only have 2 events (from fixture)
        history = await sqlite_storage.get_history(workflow_instance_with_history)
        assert len(history) == 2


@pytest.mark.asyncio
class TestActivityErrorHandling:
    """Test suite for Activity error handling."""

    @pytest_asyncio.fixture
    async def workflow_instance(self, sqlite_storage, create_test_instance):
        """Create a workflow instance for testing."""
        instance_id = "test-error-instance-001"
        await create_test_instance(
            instance_id=instance_id,
            workflow_name="test_workflow",
            input_data={"test": "data"},
        )
        # Update status to 'running' after creation
        await sqlite_storage.update_instance_status(instance_id, "running")
        return instance_id

    @pytest.fixture
    def context(self, sqlite_storage, workflow_instance):
        """Create a workflow context for testing."""
        return WorkflowContext(
            instance_id=workflow_instance,
            workflow_name="test_workflow",
            storage=sqlite_storage,
            worker_id="worker-1",
            is_replaying=False,
        )

    async def test_activity_error_is_raised(self, context):
        """Test that activity errors are raised to caller."""

        @activity
        async def failing_activity(ctx: WorkflowContext) -> dict:
            # TerminalError is never retried and propagates original exception
            raise TerminalError("Test error")

        with pytest.raises(TerminalError, match="Test error"):
            await failing_activity(context)

    async def test_activity_error_records_history(self, context, sqlite_storage, workflow_instance):
        """Test that activity failures are recorded in history."""

        @activity
        async def failing_activity(ctx: WorkflowContext) -> dict:
            # TerminalError is never retried and propagates original exception
            raise TerminalError("Activity failed")

        with pytest.raises(TerminalError):
            await failing_activity(context, activity_id="failing_activity:1")

        # Verify failure was recorded
        history = await sqlite_storage.get_history(workflow_instance)
        assert len(history) == 1

        event = history[0]
        assert event["activity_id"] == "failing_activity:1"
        assert event["event_type"] == "ActivityFailed"
        assert event["event_data"]["activity_name"] == "failing_activity"
        assert event["event_data"]["error_type"] == "TerminalError"
        assert event["event_data"]["error_message"] == "Activity failed"

        # Verify stack trace was recorded
        assert "stack_trace" in event["event_data"]
        stack_trace = event["event_data"]["stack_trace"]
        assert "TerminalError: Activity failed" in stack_trace
        assert "raise TerminalError" in stack_trace  # Should contain the line that raised

    async def test_activity_replay_cached_error(self, sqlite_storage, create_test_instance):
        """Test that replayed activities re-raise cached errors."""
        instance_id = "test-error-replay-001"
        await create_test_instance(
            instance_id=instance_id,
            workflow_name="test_workflow",
            input_data={},
        )
        # Update status and current activity
        await sqlite_storage.update_instance_status(instance_id, "running")
        await sqlite_storage.update_instance_activity(instance_id, "test_activity:1")

        # Add failed activity to history
        await sqlite_storage.append_history(
            instance_id,
            activity_id="test_activity:1",
            event_type="ActivityFailed",
            event_data={
                "activity_name": "test_activity",
                "error_type": "ValueError",
                "error_message": "Cached error",
            },
        )

        # Create replay context
        ctx = WorkflowContext(
            instance_id=instance_id,
            workflow_name="test_workflow",
            storage=sqlite_storage,
            worker_id="worker-1",
            is_replaying=True,
        )
        await ctx._load_history()

        @activity
        async def test_activity(ctx: WorkflowContext) -> dict:
            return {"should": "not_execute"}

        # Should raise cached error during replay
        with pytest.raises(Exception, match="ValueError: Cached error"):
            await test_activity(ctx, activity_id="test_activity:1")

    async def test_activity_error_tracks_activity_id(self, context):
        """Test that failed activities still track activity IDs."""

        @activity
        async def failing_activity(ctx: WorkflowContext) -> dict:
            # TerminalError is never retried and propagates original exception
            raise TerminalError("Error")

        with pytest.raises(TerminalError):
            await failing_activity(context, activity_id="failing_activity:1")

        # Activity ID should be tracked even though it failed
        assert len(context.executed_activity_ids) == 1
        assert "failing_activity:1" in context.executed_activity_ids

    async def test_activity_error_stack_trace_format(
        self, context, sqlite_storage, workflow_instance
    ):
        """Test that stack trace contains detailed information."""

        @activity
        async def nested_failing_activity(ctx: WorkflowContext) -> dict:
            # Nested function to create a deeper stack trace
            # TerminalError is never retried and propagates original exception
            def inner_function():
                raise TerminalError("Detailed error message")

            inner_function()
            return {}

        with pytest.raises(TerminalError):
            await nested_failing_activity(context)

        # Verify stack trace format
        history = await sqlite_storage.get_history(workflow_instance)
        event = history[0]
        stack_trace = event["event_data"]["stack_trace"]

        # Stack trace should contain:
        # - Exception type and message
        assert "TerminalError: Detailed error message" in stack_trace

        # - File path
        assert "test_activity.py" in stack_trace

        # - Function names
        assert "nested_failing_activity" in stack_trace
        assert "inner_function" in stack_trace

        # - Line information
        assert 'File "' in stack_trace
        assert ", line " in stack_trace

        # - Full traceback format
        assert "Traceback (most recent call last):" in stack_trace
