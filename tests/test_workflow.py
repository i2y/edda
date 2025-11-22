"""
Tests for Saga workflow decorator module.

Tests cover:
- Saga decorator functionality
- Workflow start mechanism
- Workflow resume mechanism
- Integration with ReplayEngine
"""

import uuid

import pytest
import pytest_asyncio

from edda.activity import activity
from edda.context import WorkflowContext
from edda.exceptions import TerminalError
from edda.replay import ReplayEngine
from edda.workflow import Workflow, set_replay_engine, workflow


@pytest.mark.asyncio
class TestSagaDecorator:
    """Test suite for @workflow decorator."""

    async def test_saga_decorator_marks_function(self):
        """Test that @workflow decorator marks function as workflow."""

        @workflow
        async def test_workflow(ctx: WorkflowContext, value: int) -> dict:
            return {"result": value}

        # Saga decorator returns a Saga instance

        assert isinstance(test_workflow, Workflow)
        assert test_workflow.name == "test_workflow"

    async def test_saga_decorator_preserves_metadata(self):
        """Test that decorator preserves function metadata."""

        @workflow
        async def my_test_workflow(ctx: WorkflowContext) -> dict:
            """Test workflow docstring."""
            return {}

        assert my_test_workflow.__name__ == "my_test_workflow"
        assert my_test_workflow.__doc__ == "Test workflow docstring."

    async def test_saga_decorator_requires_async(self):
        """Test that decorator raises error for non-async functions."""

        with pytest.raises(TypeError, match="must be an async function"):

            @workflow
            def sync_function(ctx: WorkflowContext) -> dict:
                return {}

    async def test_saga_decorator_creates_wrapper(self):
        """Test that decorator creates Saga wrapper."""

        @workflow
        async def test_workflow(ctx: WorkflowContext) -> dict:
            return {}

        assert isinstance(test_workflow, Workflow)


@pytest.mark.asyncio
class TestSagaStart:
    """Test suite for Saga.start() method."""

    @pytest.fixture
    def replay_engine(self, sqlite_storage):
        """Create and configure ReplayEngine."""
        engine = ReplayEngine(
            storage=sqlite_storage,
            service_name="test-service",
            worker_id="worker-saga-test",
        )
        set_replay_engine(engine)
        return engine

    async def test_saga_start_simple(self, replay_engine, sqlite_storage, create_test_instance):
        """Test starting a simple saga."""

        @workflow
        async def simple_saga(ctx: WorkflowContext, name: str) -> dict:
            return {"message": f"Hello, {name}"}

        instance_id = await simple_saga.start(name="Alice")

        # Verify instance was created and completed
        assert instance_id.startswith("simple_saga-")
        instance = await sqlite_storage.get_instance(instance_id)
        assert instance is not None
        assert instance["workflow_name"] == "simple_saga"
        assert instance["status"] == "completed"
        assert instance["output_data"]["result"] == {"message": "Hello, Alice"}

    async def test_saga_start_with_activities(
        self, replay_engine, sqlite_storage, create_test_instance
    ):
        """Test starting a workflow that calls activities."""

        @activity
        async def step1(ctx: WorkflowContext, value: int) -> dict:
            return {"doubled": value * 2}

        @activity
        async def step2(ctx: WorkflowContext, value: int) -> dict:
            return {"squared": value**2}

        @workflow
        async def multi_step_saga(ctx: WorkflowContext, number: int) -> dict:
            result1 = await step1(ctx, number)
            result2 = await step2(ctx, result1["doubled"])
            return {"step1": result1, "step2": result2}

        instance_id = await multi_step_saga.start(number=5)

        # Verify execution
        instance = await sqlite_storage.get_instance(instance_id)
        assert instance["status"] == "completed"

        # Verify history
        history = await sqlite_storage.get_history(instance_id)
        assert len(history) == 2
        assert history[0]["event_data"]["activity_name"] == "step1"
        assert history[0]["event_data"]["result"] == {"doubled": 10}
        assert history[1]["event_data"]["activity_name"] == "step2"
        assert history[1]["event_data"]["result"] == {"squared": 100}

    async def test_saga_start_with_multiple_kwargs(
        self, replay_engine, sqlite_storage, create_test_instance
    ):
        """Test starting a workflow with multiple keyword arguments."""

        @workflow
        async def multi_param_saga(ctx: WorkflowContext, name: str, age: int, city: str) -> dict:
            return {"name": name, "age": age, "city": city}

        instance_id = await multi_param_saga.start(name="Bob", age=30, city="Tokyo")

        instance = await sqlite_storage.get_instance(instance_id)
        assert instance["status"] == "completed"
        assert instance["output_data"]["result"] == {
            "name": "Bob",
            "age": 30,
            "city": "Tokyo",
        }

    async def test_saga_start_handles_error(
        self, replay_engine, sqlite_storage, create_test_instance
    ):
        """Test that saga handles errors during start."""

        @activity
        async def failing_step(ctx: WorkflowContext) -> dict:
            # TerminalError is never retried and propagates immediately
            raise TerminalError("Step failed")

        @workflow
        async def failing_saga(ctx: WorkflowContext) -> dict:
            await failing_step(ctx)
            return {}

        # Should raise the error after marking workflow as failed
        with pytest.raises(TerminalError, match="Step failed"):
            await failing_saga.start()

    async def test_saga_start_without_engine_raises_error(self):
        """Test that starting saga without engine raises error."""
        # Clear the global engine
        set_replay_engine(None)

        @workflow
        async def test_workflow(ctx: WorkflowContext) -> dict:
            return {}

        with pytest.raises(RuntimeError, match="Replay engine not initialized"):
            await test_workflow.start()


@pytest.mark.asyncio
class TestSagaResume:
    """Test suite for Saga.resume() method."""

    @pytest.fixture
    def replay_engine(self, sqlite_storage):
        """Create and configure ReplayEngine."""
        engine = ReplayEngine(
            storage=sqlite_storage,
            service_name="test-service",
            worker_id="worker-resume-test",
        )
        set_replay_engine(engine)
        return engine

    @pytest_asyncio.fixture
    async def paused_workflow_instance(self, sqlite_storage, create_test_instance):
        """Create a paused workflow instance with history."""
        instance_id = f"paused-workflow-{uuid.uuid4().hex}"
        await create_test_instance(
            instance_id=instance_id,
            workflow_name="paused_saga",
            owner_service="test-service",
            input_data={"initial": "data"},
        )
        # Update status and current activity
        await sqlite_storage.update_instance_status(instance_id, "waiting_for_event")
        await sqlite_storage.update_instance_activity(instance_id, "first_step:1")

        # Add history for completed activities (use auto-generated activity_id format)
        await sqlite_storage.append_history(
            instance_id,
            activity_id="first_step:1",
            event_type="ActivityCompleted",
            event_data={"activity_name": "first_step", "result": {"value": 42}},
        )

        return instance_id

    async def test_saga_resume_continues_execution(
        self, replay_engine, paused_workflow_instance, sqlite_storage, create_test_instance
    ):
        """Test that resume continues workflow execution."""

        @activity
        async def first_step(ctx: WorkflowContext) -> dict:
            return {"should": "not_execute"}

        @activity
        async def second_step(ctx: WorkflowContext, event_data: dict) -> dict:
            return {"event_received": event_data}

        @workflow
        async def paused_saga(ctx: WorkflowContext, initial: str) -> dict:
            result1 = await first_step(ctx)
            result2 = await second_step(ctx, {"event": "data"})
            return {"first": result1, "second": result2}

        await paused_saga.resume(instance_id=paused_workflow_instance, event={"trigger": "event"})

        # Verify workflow completed
        instance = await sqlite_storage.get_instance(paused_workflow_instance)
        assert instance["status"] == "completed"

        # Verify first step was replayed (not re-executed)
        history = await sqlite_storage.get_history(paused_workflow_instance)
        assert len(history) == 2
        assert history[0]["event_data"]["result"] == {"value": 42}  # Original
        assert history[1]["event_data"]["activity_name"] == "second_step"

    async def test_saga_resume_with_event_data(
        self, replay_engine, paused_workflow_instance, sqlite_storage, create_test_instance
    ):
        """Test that resume passes event data to workflow."""

        @activity
        async def first_step(ctx: WorkflowContext) -> dict:
            return {}

        @activity
        async def second_step(ctx: WorkflowContext) -> dict:
            return {"completed": True}

        @workflow
        async def paused_saga(ctx: WorkflowContext, initial: str) -> dict:
            await first_step(ctx)
            await second_step(ctx)
            return {"resumed": True}

        event = {"event_type": "user.approved", "user_id": "123"}
        await paused_saga.resume(instance_id=paused_workflow_instance, event=event)

        instance = await sqlite_storage.get_instance(paused_workflow_instance)
        assert instance["status"] == "completed"

    async def test_saga_resume_deterministic_replay(self, replay_engine, paused_workflow_instance):
        """Test that resume performs deterministic replay."""
        first_step_executions = 0

        @activity
        async def first_step(ctx: WorkflowContext) -> dict:
            nonlocal first_step_executions
            first_step_executions += 1
            return {"new": "value"}

        @activity
        async def second_step(ctx: WorkflowContext) -> dict:
            return {"done": True}

        @workflow
        async def paused_saga(ctx: WorkflowContext, initial: str) -> dict:
            await first_step(ctx)
            await second_step(ctx)
            return {"finished": True}

        await paused_saga.resume(instance_id=paused_workflow_instance, event=None)

        # First step should not have been executed (replayed instead)
        assert first_step_executions == 0

    async def test_saga_resume_handles_error(
        self, replay_engine, paused_workflow_instance, sqlite_storage, create_test_instance
    ):
        """Test that resume handles errors properly."""

        @activity
        async def first_step(ctx: WorkflowContext) -> dict:
            return {}

        @activity
        async def failing_step(ctx: WorkflowContext) -> dict:
            # TerminalError is never retried and propagates immediately
            raise TerminalError("Resume failed")

        @workflow
        async def paused_saga(ctx: WorkflowContext, initial: str) -> dict:
            await first_step(ctx)
            await failing_step(ctx)
            return {}

        # Should raise the error after marking workflow as failed
        with pytest.raises(TerminalError, match="Resume failed"):
            await paused_saga.resume(instance_id=paused_workflow_instance, event=None)

        # Verify workflow was marked as failed
        instance = await sqlite_storage.get_instance(paused_workflow_instance)
        assert instance["status"] == "failed"

        # Verify error details are captured
        output_data = instance["output_data"]
        assert "error_message" in output_data
        assert "error_type" in output_data
        assert "stack_trace" in output_data
        assert "Resume failed" in output_data["error_message"]
        assert output_data["error_type"] == "TerminalError"

    async def test_saga_resume_without_engine_raises_error(self):
        """Test that resuming workflow without engine raises error."""
        set_replay_engine(None)

        @workflow
        async def test_workflow(ctx: WorkflowContext) -> dict:
            return {}

        with pytest.raises(RuntimeError, match="Replay engine not initialized"):
            await test_workflow.resume(instance_id="test-id", event=None)


@pytest.mark.asyncio
class TestSagaIntegration:
    """Integration tests for Saga workflows."""

    @pytest.fixture
    def replay_engine(self, sqlite_storage):
        """Create and configure ReplayEngine."""
        engine = ReplayEngine(
            storage=sqlite_storage,
            service_name="test-service",
            worker_id="worker-integration-test",
        )
        set_replay_engine(engine)
        return engine

    async def test_complex_saga_workflow(self, replay_engine, sqlite_storage, create_test_instance):
        """Test a complex saga with multiple activities and logic."""

        @activity
        async def validate_input(ctx: WorkflowContext, value: int) -> dict:
            if value < 0:
                raise ValueError("Value must be positive")
            return {"valid": True, "value": value}

        @activity
        async def transform(ctx: WorkflowContext, value: int) -> dict:
            return {"transformed": value * 10}

        @activity
        async def persist(ctx: WorkflowContext, data: dict) -> dict:
            return {"persisted": True, "id": "123"}

        @workflow
        async def complex_saga(ctx: WorkflowContext, input_value: int) -> dict:
            validation = await validate_input(ctx, input_value)
            transformation = await transform(ctx, validation["value"])
            result = await persist(ctx, transformation)
            return {
                "validation": validation,
                "transformation": transformation,
                "persistence": result,
            }

        instance_id = await complex_saga.start(input_value=5)

        # Verify complete execution
        instance = await sqlite_storage.get_instance(instance_id)
        assert instance["status"] == "completed"

        # Verify all activities were recorded
        history = await sqlite_storage.get_history(instance_id)
        assert len(history) == 3

        # Verify final result
        output = instance["output_data"]["result"]
        assert output["validation"]["valid"] is True
        assert output["transformation"]["transformed"] == 50
        assert output["persistence"]["persisted"] is True

    async def test_saga_with_conditional_logic(
        self, replay_engine, sqlite_storage, create_test_instance
    ):
        """Test workflow with conditional activity execution."""

        @activity
        async def check_condition(ctx: WorkflowContext, value: int) -> dict:
            return {"should_process": value > 10}

        @activity
        async def process_high_value(ctx: WorkflowContext, value: int) -> dict:
            return {"processed": value * 2}

        @activity
        async def process_low_value(ctx: WorkflowContext, value: int) -> dict:
            return {"processed": value + 1}

        @workflow
        async def conditional_saga(ctx: WorkflowContext, number: int) -> dict:
            check = await check_condition(ctx, number)

            if check["should_process"]:
                result = await process_high_value(ctx, number)
            else:
                result = await process_low_value(ctx, number)

            return {"check": check, "result": result}

        # Test high value path
        instance_id1 = await conditional_saga.start(number=20)
        instance1 = await sqlite_storage.get_instance(instance_id1)
        assert instance1["output_data"]["result"]["result"]["processed"] == 40

        # Test low value path
        instance_id2 = await conditional_saga.start(number=5)
        instance2 = await sqlite_storage.get_instance(instance_id2)
        assert instance2["output_data"]["result"]["result"]["processed"] == 6
