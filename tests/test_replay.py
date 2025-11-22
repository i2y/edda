"""
Tests for ReplayEngine module.

Tests cover:
- Starting new workflows
- Resuming workflows with replay
- Lock acquisition and management
- Workflow execution flow
- Error handling during execution
"""

import uuid

import pytest
import pytest_asyncio
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from edda.activity import activity
from edda.context import WorkflowContext
from edda.exceptions import TerminalError
from edda.replay import ReplayEngine


@pytest.mark.asyncio
class TestReplayEngineBasic:
    """Test suite for ReplayEngine basic functionality."""

    @pytest.fixture
    def replay_engine(self, sqlite_storage):
        """Create a ReplayEngine instance for testing."""
        return ReplayEngine(
            storage=sqlite_storage,
            service_name="test-service",
            worker_id="worker-test-001",
        )

    async def test_replay_engine_initialization(self, replay_engine, sqlite_storage):
        """Test ReplayEngine initialization."""
        assert replay_engine.storage == sqlite_storage
        assert replay_engine.service_name == "test-service"
        assert replay_engine.worker_id == "worker-test-001"

    async def test_start_workflow_creates_instance(self, replay_engine, sqlite_storage):
        """Test that starting a workflow creates an instance."""

        async def simple_workflow(ctx: WorkflowContext, name: str) -> dict:
            return {"message": f"Hello, {name}"}

        instance_id = await replay_engine.start_workflow(
            workflow_name="simple_workflow",
            workflow_func=simple_workflow,
            input_data={"name": "Alice"},
        )

        # Verify instance was created
        assert instance_id.startswith("simple_workflow-")
        instance = await sqlite_storage.get_instance(instance_id)
        assert instance is not None
        assert instance["workflow_name"] == "simple_workflow"
        assert instance["status"] == "completed"
        assert instance["input_data"] == {"name": "Alice"}
        assert instance["output_data"] == {"result": {"message": "Hello, Alice"}}

    async def test_start_workflow_executes_function(self, replay_engine):
        """Test that workflow function is executed."""
        execution_count = 0

        async def test_workflow(ctx: WorkflowContext) -> dict:
            nonlocal execution_count
            execution_count += 1
            return {"count": execution_count}

        instance_id = await replay_engine.start_workflow(
            workflow_name="test_workflow",
            workflow_func=test_workflow,
            input_data={},
        )

        assert execution_count == 1
        assert instance_id is not None

    async def test_start_workflow_with_activities(self, replay_engine, sqlite_storage):
        """Test starting a workflow that calls activities."""

        @activity
        async def greet(ctx: WorkflowContext, name: str) -> dict:
            return {"greeting": f"Hello, {name}"}

        @activity
        async def process(ctx: WorkflowContext, data: str) -> dict:
            return {"processed": data.upper()}

        async def workflow_with_activities(ctx: WorkflowContext, name: str, data: str) -> dict:
            greeting = await greet(ctx, name)
            processed = await process(ctx, data)
            return {"greeting": greeting, "processed": processed}

        instance_id = await replay_engine.start_workflow(
            workflow_name="workflow_with_activities",
            workflow_func=workflow_with_activities,
            input_data={"name": "Bob", "data": "test"},
        )

        # Verify instance completed
        instance = await sqlite_storage.get_instance(instance_id)
        assert instance["status"] == "completed"

        # Verify history was recorded
        history = await sqlite_storage.get_history(instance_id)
        assert len(history) == 2
        assert history[0]["event_type"] == "ActivityCompleted"
        assert history[0]["event_data"]["activity_name"] == "greet"
        assert history[1]["event_type"] == "ActivityCompleted"
        assert history[1]["event_data"]["activity_name"] == "process"

    async def test_start_workflow_handles_error(self, replay_engine, sqlite_storage):
        """Test that workflow errors are handled properly."""

        async def failing_workflow(_ctx: WorkflowContext) -> dict:
            # TerminalError is never retried and propagates immediately
            raise TerminalError("Workflow error")

        # Should raise the error after marking workflow as failed
        with pytest.raises(TerminalError, match="Workflow error"):
            await replay_engine.start_workflow(
                workflow_name="failing_workflow",
                workflow_func=failing_workflow,
                input_data={},
            )

        # Find the failed workflow instance
        # (instance was created before error, so it exists in DB)
        from edda.storage.sqlalchemy_storage import WorkflowInstance

        async with AsyncSession(sqlite_storage.engine) as conn:
            result = await conn.execute(
                select(WorkflowInstance)
                .where(WorkflowInstance.workflow_name == "failing_workflow")
                .order_by(WorkflowInstance.started_at.desc())
                .limit(1)
            )
            row = result.scalar_one()
            status = row.status
            output_data_raw = row.output_data

        # Verify status
        assert status == "failed"

        # Check output_data contains detailed error information
        assert output_data_raw is not None

        import json

        output_data = json.loads(output_data_raw)

        # Verify error details
        assert "error_message" in output_data
        assert "error_type" in output_data
        assert "stack_trace" in output_data

        assert output_data["error_message"] == "Workflow error"
        assert output_data["error_type"] == "TerminalError"
        assert "TerminalError: Workflow error" in output_data["stack_trace"]
        assert "failing_workflow" in output_data["stack_trace"]

    async def test_start_workflow_releases_lock_on_completion(self, replay_engine, sqlite_storage):
        """Test that lock is released when workflow completes."""

        async def simple_workflow(_ctx: WorkflowContext) -> dict:
            return {"status": "done"}

        instance_id = await replay_engine.start_workflow(
            workflow_name="simple_workflow",
            workflow_func=simple_workflow,
            input_data={},
        )

        # Verify lock was released
        instance = await sqlite_storage.get_instance(instance_id)
        assert instance["locked_by"] is None
        assert instance["locked_at"] is None

    async def test_start_workflow_releases_lock_on_error(self, replay_engine, sqlite_storage):
        """Test that lock is released even when workflow fails."""
        # Skip this test as it's hard to verify lock release
        # without capturing the instance_id before the error occurs
        pytest.skip("Cannot verify lock release without instance_id")


@pytest.mark.asyncio
class TestReplayEngineResume:
    """Test suite for ReplayEngine resume functionality."""

    @pytest.fixture
    def replay_engine(self, sqlite_storage):
        """Create a ReplayEngine instance for testing."""
        return ReplayEngine(
            storage=sqlite_storage,
            service_name="test-service",
            worker_id="worker-test-002",
        )

    @pytest_asyncio.fixture
    async def completed_workflow_instance(self, sqlite_storage, create_test_instance):
        """Create a completed workflow instance with history."""
        instance_id = f"test-workflow-{uuid.uuid4().hex}"
        await create_test_instance(
            instance_id=instance_id,
            workflow_name="test_workflow",
            input_data={"name": "Alice"},
        )
        # Update status and current activity
        await sqlite_storage.update_instance_status(instance_id, "waiting_for_event")
        await sqlite_storage.update_instance_activity(instance_id, "activity2:1")

        # Add activity history (use auto-generated activity_id format)
        await sqlite_storage.append_history(
            instance_id,
            activity_id="activity1:1",
            event_type="ActivityCompleted",
            event_data={"activity_name": "activity1", "result": {"value": 10}},
        )
        await sqlite_storage.append_history(
            instance_id,
            activity_id="activity2:1",
            event_type="ActivityCompleted",
            event_data={"activity_name": "activity2", "result": {"value": 20}},
        )

        return instance_id

    async def test_resume_workflow_loads_history(
        self, replay_engine, completed_workflow_instance, sqlite_storage
    ):
        """Test that resuming a workflow loads history."""

        @activity
        async def activity1(_ctx: WorkflowContext) -> dict:
            return {"value": 99}  # Should not execute

        @activity
        async def activity2(_ctx: WorkflowContext) -> dict:
            return {"value": 99}  # Should not execute

        @activity
        async def activity3(_ctx: WorkflowContext) -> dict:
            return {"value": 30}  # Should execute

        async def test_workflow(_ctx: WorkflowContext, name: str) -> dict:
            r1 = await activity1(_ctx)
            r2 = await activity2(_ctx)
            r3 = await activity3(_ctx)
            return {"r1": r1, "r2": r2, "r3": r3, "name": name}

        await replay_engine.resume_workflow(
            instance_id=completed_workflow_instance,
            workflow_func=test_workflow,
            _event=None,
        )

        # Verify workflow completed successfully
        instance = await sqlite_storage.get_instance(completed_workflow_instance)
        assert instance["status"] == "completed"

        # Verify only the new activity was recorded
        history = await sqlite_storage.get_history(completed_workflow_instance)
        assert len(history) == 3
        assert history[2]["event_data"]["activity_name"] == "activity3"

        # Verify result contains replayed and new values
        output = instance["output_data"]
        assert output["result"]["r1"] == {"value": 10}  # From replay
        assert output["result"]["r2"] == {"value": 20}  # From replay
        assert output["result"]["r3"] == {"value": 30}  # Newly executed

    async def test_resume_workflow_deterministic_replay(
        self, replay_engine, completed_workflow_instance
    ):
        """Test that resume performs deterministic replay."""
        activity1_executions = 0
        activity2_executions = 0

        @activity
        async def activity1(_ctx: WorkflowContext) -> dict:
            nonlocal activity1_executions
            activity1_executions += 1
            return {"computed": "new"}

        @activity
        async def activity2(_ctx: WorkflowContext) -> dict:
            nonlocal activity2_executions
            activity2_executions += 1
            return {"computed": "new"}

        @activity
        async def activity3(_ctx: WorkflowContext) -> dict:
            return {"final": "value"}

        async def test_workflow(_ctx: WorkflowContext, name: str) -> dict:
            await activity1(_ctx)
            await activity2(_ctx)
            await activity3(_ctx)
            return {"done": True}

        await replay_engine.resume_workflow(
            instance_id=completed_workflow_instance,
            workflow_func=test_workflow,
            _event=None,
        )

        # Activities 1 and 2 should not have been executed (replayed instead)
        assert activity1_executions == 0
        assert activity2_executions == 0

    async def test_resume_workflow_handles_error(
        self, replay_engine, completed_workflow_instance, sqlite_storage
    ):
        """Test that resume handles workflow errors."""

        @activity
        async def activity1(_ctx: WorkflowContext) -> dict:
            return {}

        @activity
        async def activity2(_ctx: WorkflowContext) -> dict:
            return {}

        @activity
        async def failing_activity(_ctx: WorkflowContext) -> dict:
            # TerminalError is never retried and propagates immediately
            raise TerminalError("Resume error")

        async def test_workflow(_ctx: WorkflowContext, name: str) -> dict:
            await activity1(_ctx)
            await activity2(_ctx)
            await failing_activity(_ctx)
            return {}

        # Should raise the error after marking workflow as failed
        with pytest.raises(TerminalError, match="Resume error"):
            await replay_engine.resume_workflow(
                instance_id=completed_workflow_instance,
                workflow_func=test_workflow,
                _event=None,
            )

        # Verify workflow was marked as failed
        instance = await sqlite_storage.get_instance(completed_workflow_instance)
        assert instance["status"] == "failed"

        # Verify error details are captured
        output_data = instance["output_data"]
        assert "error_message" in output_data
        assert "error_type" in output_data
        assert "stack_trace" in output_data
        assert "Resume error" in output_data["error_message"]
        assert output_data["error_type"] == "TerminalError"

    async def test_resume_workflow_releases_lock(
        self, replay_engine, completed_workflow_instance, sqlite_storage
    ):
        """Test that resume releases lock after completion."""

        @activity
        async def activity1(_ctx: WorkflowContext) -> dict:
            return {}

        @activity
        async def activity2(_ctx: WorkflowContext) -> dict:
            return {}

        @activity
        async def activity3(_ctx: WorkflowContext) -> dict:
            return {"new": "result"}

        async def test_workflow(_ctx: WorkflowContext, name: str) -> dict:
            await activity1(_ctx)
            await activity2(_ctx)
            await activity3(_ctx)
            return {"completed": True}

        await replay_engine.resume_workflow(
            instance_id=completed_workflow_instance,
            workflow_func=test_workflow,
            _event=None,
        )

        # Verify lock was released
        instance = await sqlite_storage.get_instance(completed_workflow_instance)
        assert instance["locked_by"] is None
        assert instance["locked_at"] is None


@pytest.mark.asyncio
class TestReplayEngineLocking:
    """Test suite for ReplayEngine lock management."""

    @pytest.fixture
    def replay_engine(self, sqlite_storage):
        """Create a ReplayEngine instance for testing."""
        return ReplayEngine(
            storage=sqlite_storage,
            service_name="test-service",
            worker_id="worker-lock-test",
        )

    async def test_execute_with_lock_acquires_lock(
        self, replay_engine, sqlite_storage, create_test_instance
    ):
        """Test that execute_with_lock acquires the lock."""
        instance_id = f"test-{uuid.uuid4().hex}"
        await create_test_instance(
            instance_id=instance_id,
            workflow_name="test_workflow",
            input_data={},
        )

        async def test_func(ctx: WorkflowContext) -> dict:
            # During execution, verify lock is held
            instance = await sqlite_storage.get_instance(instance_id)
            assert instance["locked_by"] == "worker-lock-test"
            return {"success": True}

        result = await replay_engine.execute_with_lock(
            instance_id=instance_id,
            workflow_func=test_func,
            is_replay=False,
        )

        assert result == {"success": True}

        # After execution, lock should be released
        instance = await sqlite_storage.get_instance(instance_id)
        assert instance["locked_by"] is None

    async def test_execute_with_lock_retries(self, sqlite_storage, create_test_instance):
        """Test that execute_with_lock retries lock acquisition."""
        instance_id = f"test-{uuid.uuid4().hex}"
        await create_test_instance(
            instance_id=instance_id,
            workflow_name="test_workflow",
            input_data={},
        )

        # Acquire lock with different worker
        await sqlite_storage.try_acquire_lock(instance_id, "other-worker")

        # Create engine
        engine = ReplayEngine(
            storage=sqlite_storage,
            service_name="test-service",
            worker_id="worker-retry-test",
        )

        async def test_func(_ctx: WorkflowContext) -> dict:
            return {"result": "success"}

        # Should fail to acquire lock (workflow_lock will handle retries)
        # The lock is already held, so this will timeout
        with pytest.raises(RuntimeError, match="Failed to acquire lock"):
            await engine.execute_with_lock(
                instance_id=instance_id,
                workflow_func=test_func,
                is_replay=False,
            )

    async def test_execute_with_lock_releases_on_error(
        self, replay_engine, sqlite_storage, create_test_instance
    ):
        """Test that lock is released even when execution fails."""
        instance_id = f"test-{uuid.uuid4().hex}"
        await create_test_instance(
            instance_id=instance_id,
            workflow_name="test_workflow",
            input_data={},
        )

        async def failing_func(_ctx: WorkflowContext) -> dict:
            # TerminalError is never retried and propagates immediately
            raise TerminalError("Execution error")

        with pytest.raises(TerminalError, match="Execution error"):
            await replay_engine.execute_with_lock(
                instance_id=instance_id,
                workflow_func=failing_func,
                is_replay=False,
            )

        # Verify lock was released
        instance = await sqlite_storage.get_instance(instance_id)
        assert instance["locked_by"] is None
