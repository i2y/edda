"""
Tests for synchronous activities (sync functions with @activity decorator).

Sync activities are executed in a thread pool using anyio.to_thread.run_sync().
"""

import pytest

from edda import activity, workflow
from edda.context import WorkflowContext
from edda.exceptions import RetryExhaustedError, TerminalError
from edda.replay import ReplayEngine
from edda.retry import RetryPolicy
from edda.storage.sqlalchemy_storage import SQLAlchemyStorage


# Sync activity examples
@activity
def sync_greet(ctx: WorkflowContext, name: str) -> str:
    """Simple sync activity that returns a greeting."""
    return f"Hello, {name}!"


@activity
def sync_add(ctx: WorkflowContext, a: int, b: int) -> int:
    """Sync activity that adds two numbers."""
    return a + b


@activity
def sync_failing_activity(ctx: WorkflowContext, should_fail: bool) -> dict:
    """Sync activity that fails conditionally."""
    if should_fail:
        raise ValueError("Intentional failure")
    return {"status": "success"}


@activity
def sync_terminal_error_activity(ctx: WorkflowContext) -> dict:
    """Sync activity that raises a terminal error."""
    raise TerminalError("Terminal error - should not retry")


@activity(retry_policy=RetryPolicy(max_attempts=2, initial_interval=0.01))
def sync_retry_activity(ctx: WorkflowContext, attempt_tracker: list[int]) -> dict:
    """Sync activity with custom retry policy."""
    attempt_tracker.append(1)
    if len(attempt_tracker) < 2:
        raise ValueError("Retry me")
    return {"attempts": len(attempt_tracker)}


# Workflow using sync activities
@workflow
async def workflow_with_sync_activities(ctx: WorkflowContext, name: str, a: int, b: int) -> dict:
    """Workflow that uses sync activities."""
    greeting = await sync_greet(ctx, name, activity_id="sync_greet:1")
    sum_result = await sync_add(ctx, a, b, activity_id="sync_add:1")
    return {"greeting": greeting, "sum": sum_result}


@pytest.fixture
async def storage():
    """Create a SQLite storage instance for testing."""
    from sqlalchemy.ext.asyncio import create_async_engine

    engine = create_async_engine("sqlite+aiosqlite:///:memory:", echo=False)
    storage = SQLAlchemyStorage(engine)
    await storage.initialize()
    yield storage


@pytest.fixture
def replay_engine(storage):
    """Create and configure ReplayEngine."""
    from edda.workflow import set_replay_engine

    engine = ReplayEngine(
        storage=storage,
        service_name="test-service",
        worker_id="worker-sync-test",
    )
    set_replay_engine(engine)
    return engine


@pytest.mark.asyncio
async def test_sync_activity_basic(storage, replay_engine):
    """Test basic sync activity execution."""
    instance_id = await workflow_with_sync_activities.start(name="World", a=10, b=20)

    # Verify instance created
    instance = await storage.get_instance(instance_id)
    assert instance is not None
    assert instance["status"] == "completed"

    # Verify output (result is nested under "result" key)
    output = instance["output_data"]["result"]
    assert output["greeting"] == "Hello, World!"
    assert output["sum"] == 30


@pytest.mark.asyncio
async def test_sync_activity_replay(storage, replay_engine):
    """Test that sync activities work correctly during replay."""
    instance_id = await workflow_with_sync_activities.start(name="Replay", a=5, b=15)

    # First execution
    instance = await storage.get_instance(instance_id)
    assert instance["status"] == "completed"
    assert instance["output_data"]["result"]["sum"] == 20

    # Simulate replay by resuming the completed workflow
    await replay_engine.resume_workflow(instance_id, workflow_with_sync_activities)

    # Verify still completed with same result
    instance = await storage.get_instance(instance_id)
    assert instance["status"] == "completed"
    assert instance["output_data"]["result"]["sum"] == 20


@pytest.mark.asyncio
async def test_sync_activity_with_failure(storage):
    """Test sync activity that fails."""

    @workflow
    async def workflow_with_failing_sync(ctx: WorkflowContext) -> dict:
        result = await sync_failing_activity(ctx, should_fail=True, activity_id="fail:1")
        return result

    with pytest.raises(RetryExhaustedError):
        await workflow_with_failing_sync.start()


@pytest.mark.asyncio
async def test_sync_activity_with_terminal_error(storage):
    """Test sync activity that raises terminal error (no retry)."""

    @workflow
    async def workflow_with_terminal(ctx: WorkflowContext) -> dict:
        result = await sync_terminal_error_activity(ctx, activity_id="terminal:1")
        return result

    with pytest.raises(TerminalError):
        await workflow_with_terminal.start()


@pytest.mark.asyncio
async def test_sync_activity_with_custom_retry(storage, replay_engine):
    """Test sync activity with custom retry policy."""
    attempt_tracker: list[int] = []

    @workflow
    async def workflow_with_retry(ctx: WorkflowContext) -> dict:
        result = await sync_retry_activity(ctx, attempt_tracker, activity_id="retry:1")
        return result

    instance_id = await workflow_with_retry.start()

    instance = await storage.get_instance(instance_id)
    assert instance["status"] == "completed"
    assert instance["output_data"]["result"]["attempts"] == 2
    assert len(attempt_tracker) == 2


@pytest.mark.asyncio
async def test_mixed_sync_async_activities(storage, replay_engine):
    """Test workflow with both sync and async activities."""

    @activity
    async def async_activity(ctx: WorkflowContext, value: int) -> int:
        return value * 2

    @workflow
    async def mixed_workflow(ctx: WorkflowContext, value: int) -> dict:
        sync_result = await sync_add(ctx, value, 10, activity_id="sync:1")
        async_result = await async_activity(ctx, sync_result, activity_id="async:1")
        return {"sync": sync_result, "async": async_result}

    instance_id = await mixed_workflow.start(value=5)

    instance = await storage.get_instance(instance_id)
    assert instance["status"] == "completed"
    output = instance["output_data"]["result"]
    assert output["sync"] == 15  # 5 + 10
    assert output["async"] == 30  # 15 * 2


@pytest.mark.asyncio
async def test_sync_activity_with_pydantic(storage, replay_engine):
    """Test sync activity with Pydantic models."""
    from pydantic import BaseModel

    class UserInput(BaseModel):
        name: str
        age: int

    class UserResult(BaseModel):
        greeting: str
        is_adult: bool

    @activity
    def sync_process_user(ctx: WorkflowContext, user: UserInput) -> UserResult:
        return UserResult(greeting=f"Hi {user.name}!", is_adult=user.age >= 18)

    @workflow
    async def user_workflow(ctx: WorkflowContext, user: UserInput) -> UserResult:
        result = await sync_process_user(ctx, user, activity_id="process:1")
        return result

    user = UserInput(name="Alice", age=25)
    instance_id = await user_workflow.start(user=user)

    instance = await storage.get_instance(instance_id)
    assert instance["status"] == "completed"
    output = instance["output_data"]["result"]
    assert output["greeting"] == "Hi Alice!"
    assert output["is_adult"] is True
