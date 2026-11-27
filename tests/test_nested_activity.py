"""An activity called inside another activity's body is part of the outer one
(SEMANTICS §2): it runs directly, in the outer attempt's transaction, takes no
ID and records nothing, and its own retry policy and compensation do not
apply."""

import contextlib
import logging

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

from edda.activity import activity
from edda.compensation import compensation, on_failure
from edda.context import WorkflowContext
from edda.exceptions import RetryExhaustedError, TerminalError
from edda.replay import ReplayEngine
from edda.retry import RetryPolicy
from edda.storage.sqlalchemy_storage import SQLAlchemyStorage

FAST = {"initial_interval": 0.001, "backoff_coefficient": 1.0}


@pytest.fixture
async def storage(tmp_path):
    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path / 'nested.db'}")
    storage = SQLAlchemyStorage(engine)
    await storage.initialize()
    async with engine.begin() as conn:
        await conn.execute(text("CREATE TABLE nested_rows (who TEXT)"))
    yield storage
    await storage.close()


async def _completed_ids(storage, instance_id):
    return [
        h["activity_id"]
        for h in await storage.get_history(instance_id)
        if h["event_type"] == "ActivityCompleted"
    ]


async def _rows(storage):
    async with storage.engine.connect() as conn:
        return sorted(r[0] for r in (await conn.execute(text("SELECT who FROM nested_rows"))).all())


async def _new_context(storage, instance_id, **kwargs):
    await storage.upsert_workflow_definition(
        workflow_name="nested", source_hash="h", source_code="pass"
    )
    if await storage.get_instance(instance_id) is None:
        await storage.create_instance(
            instance_id=instance_id,
            workflow_name="nested",
            source_hash="h",
            owner_service="s",
            input_data={},
        )
    context = WorkflowContext(
        instance_id=instance_id, workflow_name="nested", storage=storage, worker_id="w", **kwargs
    )
    if context.is_replaying:
        await context._load_history()
    return context


async def test_replay_gives_a_later_top_level_call_its_own_result(storage):
    @activity
    async def inner(ctx: WorkflowContext, tag: str) -> str:
        return tag

    @activity
    async def outer(ctx: WorkflowContext) -> str:
        return await inner(ctx, "nested")

    async def workflow(ctx: WorkflowContext) -> list[str]:
        return [await outer(ctx), await inner(ctx, "top-level")]

    first = await workflow(await _new_context(storage, "replay-1"))
    assert first == ["nested", "top-level"]
    # The nested call took no ID and recorded nothing.
    assert sorted(await _completed_ids(storage, "replay-1")) == ["inner:1", "outer:1"]

    replayed = await workflow(await _new_context(storage, "replay-1", is_replaying=True))
    assert replayed == ["nested", "top-level"]


async def test_a_manual_id_on_a_nested_call_is_ignored(storage, caplog):
    @activity
    async def inner(ctx: WorkflowContext, tag: str) -> str:
        return tag

    @activity
    async def outer(ctx: WorkflowContext) -> str:
        return await inner(ctx, "nested", activity_id="lookup:1")

    async def workflow(ctx: WorkflowContext) -> str:
        await outer(ctx)
        return await inner(ctx, "top-level", activity_id="lookup:1")

    with caplog.at_level(logging.WARNING, logger="edda.context"):
        assert await workflow(await _new_context(storage, "manual-1")) == "top-level"
    assert "used twice" not in caplog.text


async def test_the_nested_call_writes_in_the_outer_transaction(storage):
    @activity
    async def inner(ctx: WorkflowContext) -> None:
        # The first write of the outer attempt's transaction.
        await ctx.session.execute(text("INSERT INTO nested_rows VALUES ('inner')"))

    @activity
    async def outer(ctx: WorkflowContext, fail: bool) -> None:
        await inner(ctx)
        await ctx.session.execute(text("INSERT INTO nested_rows VALUES ('outer')"))
        if fail:
            raise TerminalError("the outer activity fails after the inner call")

    async def workflow(ctx: WorkflowContext, fail: bool) -> None:
        with contextlib.suppress(TerminalError):
            await outer(ctx, fail)

    engine = ReplayEngine(storage=storage, service_name="nested", worker_id="nested-w")
    failed = await engine.start_workflow("nested_fail", workflow, {"fail": True})
    assert await _rows(storage) == []
    assert await _completed_ids(storage, failed) == []

    committed = await engine.start_workflow("nested_commit", workflow, {"fail": False})
    assert await _rows(storage) == ["inner", "outer"]
    assert await _completed_ids(storage, committed) == ["outer:1"]


async def test_an_error_of_the_nested_call_is_the_outer_attempts(storage):
    runs = {"inner": 0, "outer": 0}

    @activity(retry_policy=RetryPolicy(max_attempts=5, **FAST))
    async def inner(ctx: WorkflowContext) -> int:
        runs["inner"] += 1
        if runs["inner"] == 1:
            raise ValueError("transient")
        return runs["inner"]

    @activity(retry_policy=RetryPolicy(max_attempts=2, **FAST))
    async def outer(ctx: WorkflowContext) -> int:
        runs["outer"] += 1
        return await inner(ctx)

    async def workflow(ctx: WorkflowContext) -> int:
        return await outer(ctx)

    engine = ReplayEngine(storage=storage, service_name="nested", worker_id="nested-w")
    instance_id = await engine.start_workflow("nested_retry", workflow, {})

    # The outer policy ran the whole outer attempt again; the inner policy
    # (5 attempts) did not apply.
    assert runs == {"inner": 2, "outer": 2}
    assert (await storage.get_instance(instance_id))["status"] == "completed"
    assert await _completed_ids(storage, instance_id) == ["outer:1"]


async def test_the_outer_policy_decides_when_the_nested_call_keeps_failing(storage):
    inner_runs = []

    @activity(retry_policy=RetryPolicy(max_attempts=5, **FAST))
    async def inner(ctx: WorkflowContext) -> None:
        inner_runs.append(1)
        raise ValueError("always")

    @activity(retry_policy=RetryPolicy(max_attempts=1))
    async def outer(ctx: WorkflowContext) -> None:
        await inner(ctx)

    async def workflow(ctx: WorkflowContext) -> None:
        await outer(ctx)

    engine = ReplayEngine(storage=storage, service_name="nested", worker_id="nested-w")
    with pytest.raises(RetryExhaustedError) as raised:
        await engine.start_workflow("nested_gives_up", workflow, {})
    assert "outer" in str(raised.value)
    assert len(inner_runs) == 1  # the inner policy's 5 attempts do not apply


async def test_the_nested_call_registers_no_compensation(storage):
    undone: list[str] = []

    @compensation
    async def undo_inner(ctx: WorkflowContext) -> None:
        undone.append("inner")

    @activity
    @on_failure(undo_inner)
    async def inner(ctx: WorkflowContext) -> str:
        return "inner done"

    @activity
    async def outer(ctx: WorkflowContext) -> str:
        return await inner(ctx)

    async def workflow(ctx: WorkflowContext) -> None:
        await outer(ctx)
        raise ValueError("the workflow fails later")

    engine = ReplayEngine(storage=storage, service_name="nested", worker_id="nested-w")
    with pytest.raises(ValueError):
        await engine.start_workflow("nested_comp", workflow, {})
    assert undone == []


async def test_the_nested_call_runs_no_hooks(storage):
    started: list[str] = []

    class Hooks:
        async def on_activity_start(self, instance_id, activity_id, name, is_replaying):
            started.append(activity_id)

    @activity
    async def inner(ctx: WorkflowContext) -> str:
        return "inner done"

    @activity
    async def outer(ctx: WorkflowContext) -> str:
        return await inner(ctx)

    ctx = await _new_context(storage, "hooks-1", hooks=Hooks())
    assert await outer(ctx) == "inner done"
    assert started == ["outer:1"]
