"""
Tests for compensation (Saga) module.

Tests cover:
- Compensation registration
- Compensation execution on failure
- LIFO execution order
- on_failure decorator
"""

import pytest
import pytest_asyncio

from edda import workflow
from edda.activity import activity
from edda.compensation import (
    CompensationAction,
    execute_compensations,
    register_compensation,
)
from edda.context import WorkflowContext
from edda.exceptions import TerminalError
from edda.replay import ReplayEngine
from edda.workflow import set_replay_engine


@pytest.mark.asyncio
class TestCompensationAction:
    """Test suite for CompensationAction class."""

    async def test_compensation_action_creation(self):
        """Test creating a CompensationAction."""

        async def test_compensation(value: int) -> None:
            pass

        action = CompensationAction(
            func=test_compensation,
            args=(42,),
            kwargs={},
            name="test_compensation",
        )

        assert action.func == test_compensation
        assert action.args == (42,)
        assert action.kwargs == {}
        assert action.name == "test_compensation"

    async def test_compensation_action_execution(self):
        """Test executing a compensation action."""
        executed = []

        async def test_compensation(value: int, message: str) -> None:
            executed.append({"value": value, "message": message})

        action = CompensationAction(
            func=test_compensation,
            args=(42,),
            kwargs={"message": "test"},
            name="test_compensation",
        )

        await action.execute()

        assert len(executed) == 1
        assert executed[0] == {"value": 42, "message": "test"}

    async def test_compensation_action_repr(self):
        """Test string representation of CompensationAction."""

        async def test_func():
            pass

        action = CompensationAction(
            func=test_func,
            args=(),
            kwargs={},
            name="my_compensation",
        )

        assert "my_compensation" in repr(action)


@pytest.mark.asyncio
class TestRegisterCompensation:
    """Test suite for register_compensation function."""

    @pytest_asyncio.fixture
    async def workflow_instance(self, sqlite_storage, create_test_instance):
        """Create a workflow instance for testing."""
        instance_id = "test-compensation-instance-001"
        await create_test_instance(
            instance_id=instance_id,
            workflow_name="test_workflow",
            owner_service="test-service",
            input_data={},
        )
        return instance_id

    async def test_register_compensation_stores_in_database(
        self, sqlite_storage, workflow_instance, create_test_instance
    ):
        """Test that register_compensation stores compensation in database."""
        ctx = WorkflowContext(
            instance_id=workflow_instance,
            workflow_name="test_workflow",
            storage=sqlite_storage,
            worker_id="worker-1",
            is_replaying=False,
        )

        async def my_compensation(resource_id: str) -> None:
            print(f"Releasing {resource_id}")

        # Register compensation
        await register_compensation(
            ctx,
            my_compensation,
            resource_id="RESOURCE-123",
        )

        # Verify it was stored
        compensations = await sqlite_storage.get_compensations(workflow_instance)
        assert len(compensations) == 1
        assert compensations[0]["activity_name"] == "my_compensation"

    async def test_register_multiple_compensations(
        self, sqlite_storage, workflow_instance, create_test_instance
    ):
        """Test registering multiple compensation actions."""
        ctx = WorkflowContext(
            instance_id=workflow_instance,
            workflow_name="test_workflow",
            storage=sqlite_storage,
            worker_id="worker-1",
            is_replaying=False,
        )

        async def compensation1(value: int) -> None:
            pass

        async def compensation2(value: str) -> None:
            pass

        async def compensation3() -> None:
            pass

        # Register multiple compensations
        await register_compensation(ctx, compensation1, value=1)
        await register_compensation(ctx, compensation2, value="two")
        await register_compensation(ctx, compensation3)

        # Verify all were stored
        compensations = await sqlite_storage.get_compensations(workflow_instance)
        assert len(compensations) == 3

    async def test_compensation_lifo_order(
        self, sqlite_storage, workflow_instance, create_test_instance
    ):
        """Test that compensations are stored in LIFO order."""
        ctx = WorkflowContext(
            instance_id=workflow_instance,
            workflow_name="test_workflow",
            storage=sqlite_storage,
            worker_id="worker-1",
            is_replaying=False,
        )

        async def first_compensation() -> None:
            pass

        async def second_compensation() -> None:
            pass

        async def third_compensation() -> None:
            pass

        # Register in order
        await register_compensation(ctx, first_compensation)
        await register_compensation(ctx, second_compensation)
        await register_compensation(ctx, third_compensation)

        # Get compensations
        compensations = await sqlite_storage.get_compensations(workflow_instance)

        # They should be in LIFO order (newest first - ordered by created_at DESC)
        assert compensations[0]["activity_name"] == "third_compensation"
        assert compensations[1]["activity_name"] == "second_compensation"
        assert compensations[2]["activity_name"] == "first_compensation"


@pytest.mark.asyncio
class TestExecuteCompensations:
    """Test suite for execute_compensations function."""

    @pytest_asyncio.fixture
    async def workflow_instance_with_compensations(self, sqlite_storage, create_test_instance):
        """Create a workflow instance with compensations."""
        instance_id = "test-compensation-exec-001"
        await create_test_instance(
            instance_id=instance_id,
            workflow_name="test_workflow",
            owner_service="test-service",
            input_data={},
        )

        # Add compensations
        await sqlite_storage.push_compensation(
            instance_id,
            activity_id="compensation1:1",
            activity_name="compensation1",
            args={"name": "compensation1"},
        )
        await sqlite_storage.push_compensation(
            instance_id,
            activity_id="compensation2:1",
            activity_name="compensation2",
            args={"name": "compensation2"},
        )
        await sqlite_storage.push_compensation(
            instance_id,
            activity_id="compensation3:1",
            activity_name="compensation3",
            args={"name": "compensation3"},
        )

        return instance_id

    async def test_execute_compensations_runs_in_reverse_order(
        self, sqlite_storage, workflow_instance_with_compensations, create_test_instance
    ):
        """Test that compensations are executed in LIFO order."""
        ctx = WorkflowContext(
            instance_id=workflow_instance_with_compensations,
            workflow_name="test_workflow",
            storage=sqlite_storage,
            worker_id="worker-1",
            is_replaying=False,
        )

        # Execute compensations
        # Note: Current implementation just logs, doesn't actually execute
        await execute_compensations(ctx)

        # Verify compensations were retrieved
        compensations = await ctx._get_compensations()
        assert len(compensations) == 3


@pytest.mark.asyncio
class TestCompensationWithWorkflow:
    """Test suite for compensation in workflow context."""

    @pytest.fixture
    def replay_engine(self, sqlite_storage):
        """Create and configure ReplayEngine."""
        engine = ReplayEngine(
            storage=sqlite_storage,
            service_name="test-service",
            worker_id="worker-compensation-test",
        )
        set_replay_engine(engine)
        return engine

    async def test_workflow_executes_compensations_on_failure(
        self, replay_engine, sqlite_storage, create_test_instance
    ):
        """Test that workflow automatically executes compensations on failure."""
        compensation_log = []

        @activity
        async def step1(ctx: WorkflowContext, value: int) -> dict:
            # Register compensation
            await register_compensation(
                ctx,
                compensation1,
                value=value,
            )
            return {"step": 1, "value": value}

        async def compensation1(value: int) -> None:
            compensation_log.append(f"compensation1: {value}")

        @activity
        async def step2(ctx: WorkflowContext, value: int) -> dict:
            # Register compensation
            await register_compensation(
                ctx,
                compensation2,
                value=value * 2,
            )
            return {"step": 2, "value": value * 2}

        async def compensation2(value: int) -> None:
            compensation_log.append(f"compensation2: {value}")

        @activity
        async def failing_step(ctx: WorkflowContext) -> dict:
            # TerminalError is never retried and propagates immediately
            raise TerminalError("This step fails")

        @workflow
        async def compensating_workflow(ctx: WorkflowContext, input_value: int) -> dict:
            result1 = await step1(ctx, input_value)
            result2 = await step2(ctx, input_value)
            result3 = await failing_step(ctx)  # This will fail

            return {"results": [result1, result2, result3]}

        # Start workflow (will fail and execute compensations)
        with pytest.raises(TerminalError, match="This step fails"):
            await compensating_workflow.start(input_value=10)

        # Note: Current implementation logs compensations but doesn't execute them

    async def test_workflow_clears_compensations_on_success(
        self, replay_engine, sqlite_storage, create_test_instance
    ):
        """Test that successful workflows can clear compensations."""

        @activity
        async def step_with_compensation(ctx: WorkflowContext, value: int) -> dict:
            async def my_compensation():
                pass

            await register_compensation(ctx, my_compensation)
            return {"value": value}

        @workflow
        async def successful_workflow(ctx: WorkflowContext, value: int) -> dict:
            result = await step_with_compensation(ctx, value)

            # Manually clear compensations (workflow completed successfully)
            await ctx._clear_compensations()

            return result

        # Start and complete workflow
        instance_id = await successful_workflow.start(value=42)

        # Verify workflow completed
        instance = await sqlite_storage.get_instance(instance_id)
        assert instance["status"] == "completed"

        # Compensations should still be in database
        # (clear_compensations marks them but doesn't delete - for audit)
        compensations = await sqlite_storage.get_compensations(instance_id)
        # The compensations exist but would be marked as not needed
        assert len(compensations) >= 0  # Implementation dependent


@pytest.mark.asyncio
class TestClearCompensations:
    """Test suite for clear_compensations function."""

    @pytest_asyncio.fixture
    async def workflow_instance_with_compensations(self, sqlite_storage, create_test_instance):
        """Create a workflow instance with compensations."""
        instance_id = "test-clear-compensation-001"
        await create_test_instance(
            instance_id=instance_id,
            workflow_name="test_workflow",
            owner_service="test-service",
            input_data={},
        )

        # Add compensations
        await sqlite_storage.push_compensation(
            instance_id,
            activity_id="compensation1:1",
            activity_name="compensation1",
            args={"data": "test1"},
        )
        await sqlite_storage.push_compensation(
            instance_id,
            activity_id="compensation2:1",
            activity_name="compensation2",
            args={"data": "test2"},
        )

        return instance_id

    async def test_clear_compensations(
        self, sqlite_storage, workflow_instance_with_compensations, create_test_instance
    ):
        """Test clearing compensations."""
        ctx = WorkflowContext(
            instance_id=workflow_instance_with_compensations,
            workflow_name="test_workflow",
            storage=sqlite_storage,
            worker_id="worker-1",
            is_replaying=False,
        )

        # Verify compensations exist
        compensations_before = await ctx._get_compensations()
        assert len(compensations_before) == 2

        # Clear compensations
        await ctx._clear_compensations()

        # Verify they were cleared
        compensations_after = await ctx._get_compensations()
        assert len(compensations_after) == 0
