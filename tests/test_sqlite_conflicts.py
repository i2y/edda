"""S11: SQLite write conflicts restart the activity attempt; file databases use WAL."""

import uuid

import pytest
import pytest_asyncio
from sqlalchemy import event, text
from sqlalchemy.exc import OperationalError
from sqlalchemy.ext.asyncio import create_async_engine

from edda.activity import activity
from edda.context import WorkflowContext
from edda.exceptions import RetryExhaustedError
from edda.replay import ReplayEngine
from edda.retry import RetryPolicy
from edda.storage.migrations import apply_dbmate_migrations
from edda.storage.sqlalchemy_storage import SQLAlchemyStorage, is_write_conflict
from tests.conftest import SCHEMA_DIR


@pytest_asyncio.fixture
async def file_storage(tmp_path):
    url = f"sqlite+aiosqlite:///{tmp_path / 'edda.db'}"
    engine = create_async_engine(url)
    storage = SQLAlchemyStorage(engine)
    # A short busy timeout keeps a lost race fast.
    event.listen(
        engine.sync_engine, "connect", lambda c, _: c.cursor().execute("PRAGMA busy_timeout = 50")
    )
    await apply_dbmate_migrations(engine, SCHEMA_DIR)
    # Another connection, as a second worker (or any writer) would use.
    writer = create_async_engine(url, connect_args={"timeout": 0.2})
    storage.test_writer = writer  # type: ignore[attr-defined]
    yield storage
    await writer.dispose()
    await storage.close()


async def _write_while_another_connection_writes(storage, ctx: WorkflowContext) -> None:
    """The activity's own write while another connection holds the write
    lock: SQLite answers "database is locked" once busy_timeout is spent."""
    conflict: Exception | None = None
    async with storage.test_writer.begin() as conn:
        await conn.execute(
            text(
                "INSERT INTO workflow_definitions (workflow_name, source_hash, source_code) "
                "VALUES (:n, 'h', 'c')"
            ),
            {"n": f"writer-def-{uuid.uuid4()}"},
        )
        try:
            await _update_own_instance(ctx)
        except OperationalError as error:
            conflict = error
    if conflict is not None:
        raise conflict


async def _update_own_instance(ctx: WorkflowContext) -> None:
    await ctx.session.execute(
        text("UPDATE workflow_instances SET owner_service = 'x' WHERE instance_id = :i"),
        {"i": ctx.instance_id},
    )


async def _history(storage, instance_id):
    return await storage.get_history(instance_id)


async def test_read_first_activity_survives_a_concurrent_writer(file_storage):
    runs = []

    @activity
    async def read_first(ctx: WorkflowContext) -> int:
        runs.append(1)
        await ctx.session.execute(text("SELECT count(*) FROM workflow_definitions"))
        if len(runs) == 1:
            await _write_while_another_connection_writes(file_storage, ctx)
        else:
            await _update_own_instance(ctx)
        return len(runs)

    async def workflow(ctx: WorkflowContext) -> int:
        return await read_first(ctx)

    engine = ReplayEngine(storage=file_storage, service_name="s11", worker_id="s11-worker")
    instance_id = await engine.start_workflow("s11_race", workflow, {})

    instance = await file_storage.get_instance(instance_id)
    assert instance["status"] == "completed"
    assert instance["output_data"]["result"] == 2
    assert len(runs) == 2

    # The restart is not a retry: no retry metadata on the recorded result.
    completed = [
        h
        for h in await _history(file_storage, instance_id)
        if h["event_type"] == "ActivityCompleted"
    ]
    assert len(completed) == 1
    assert "retry_metadata" not in completed[0]["event_data"]


async def test_nested_activity_conflict_restarts_only_the_outer_attempt(file_storage):
    inner_runs: list[int] = []
    outer_runs: list[int] = []

    @activity
    async def inner(ctx: WorkflowContext) -> None:
        inner_runs.append(1)
        if len(inner_runs) == 1:
            await _write_while_another_connection_writes(file_storage, ctx)
        else:
            await _update_own_instance(ctx)

    @activity
    async def outer(ctx: WorkflowContext) -> None:
        outer_runs.append(1)
        await inner(ctx)

    async def workflow(ctx: WorkflowContext) -> None:
        await outer(ctx)

    engine = ReplayEngine(storage=file_storage, service_name="s11", worker_id="s11-worker")
    instance_id = await engine.start_workflow("s11_nested", workflow, {})

    assert (await file_storage.get_instance(instance_id))["status"] == "completed"
    # The inner activity is part of the outer attempt: the conflict restarted
    # the outer attempt, which ran the inner once more.
    assert len(outer_runs) == 2
    assert len(inner_runs) == 2


async def test_a_conflict_that_never_clears_stops_after_ten_restarts(file_storage):
    runs = []

    @activity(retry_policy=RetryPolicy(max_attempts=1))
    async def always_conflicts(ctx: WorkflowContext) -> None:
        runs.append(1)
        await _write_while_another_connection_writes(file_storage, ctx)

    async def workflow(ctx: WorkflowContext) -> None:
        await always_conflicts(ctx)

    engine = ReplayEngine(storage=file_storage, service_name="s11", worker_id="s11-worker")
    with pytest.raises(RetryExhaustedError) as raised:
        await engine.start_workflow("s11_forever", workflow, {})
    assert is_write_conflict(raised.value)
    # 1 first run + 10 restarts, then the error is treated as an ordinary failure.
    assert len(runs) == 11


async def test_file_database_uses_wal_and_memory_does_not(file_storage):
    async with file_storage.engine.connect() as conn:
        assert (await conn.execute(text("PRAGMA journal_mode"))).scalar() == "wal"
        assert (await conn.execute(text("PRAGMA busy_timeout"))).scalar() > 0

    memory = SQLAlchemyStorage(create_async_engine("sqlite+aiosqlite:///:memory:"))
    async with memory.engine.connect() as conn:
        assert (await conn.execute(text("PRAGMA journal_mode"))).scalar() == "memory"
    await memory.close()
