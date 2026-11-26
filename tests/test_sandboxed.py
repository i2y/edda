"""Phase B sandboxed actor enforcement tests."""

import asyncio

import pytest

from edda import actors
from edda.context import WorkflowContext
from edda.exceptions import SandboxedError
from edda.replay import ReplayEngine
from edda.workflow import set_replay_engine, workflow

pytestmark = pytest.mark.asyncio


@pytest.fixture
def replay_engine(sqlite_storage):
    engine = ReplayEngine(
        storage=sqlite_storage,
        service_name="sandboxed-test",
        worker_id="sandboxed-worker",
    )
    set_replay_engine(engine)
    yield engine
    set_replay_engine(None)


async def test_sandboxed_issue_capability_denied(sqlite_storage, replay_engine):
    @workflow
    async def sandboxed_issue_wf(ctx: WorkflowContext):
        try:
            await actors.issue_capability(ctx, "channel_pub", "orders")
        except SandboxedError:
            return {"outcome": "denied"}
        return {"outcome": "allowed"}

    instance_id = await sandboxed_issue_wf.start(sandboxed=True)
    instance = await sqlite_storage.get_instance(instance_id)
    assert instance is not None
    assert instance["sandboxed"] is True
    assert instance["status"] == "completed"
    assert instance["output_data"]["result"] == {"outcome": "denied"}

    # Control: an unsandboxed instance may mint capabilities.
    instance_id = await sandboxed_issue_wf.start()
    instance = await sqlite_storage.get_instance(instance_id)
    assert instance is not None
    assert instance["sandboxed"] is False
    assert instance["output_data"]["result"] == {"outcome": "allowed"}


async def test_sandboxed_inherited_by_workflow_spawn(sqlite_storage, replay_engine):
    @workflow
    async def sandboxed_child_wf(ctx: WorkflowContext):
        return {"ok": True}

    @workflow
    async def sandboxed_parent_wf(ctx: WorkflowContext):
        child_id = await sandboxed_child_wf.start()
        return {"child_id": child_id}

    parent_id = await sandboxed_parent_wf.start(sandboxed=True)
    parent = await sqlite_storage.get_instance(parent_id)
    assert parent is not None
    child_id = parent["output_data"]["result"]["child_id"]

    child = await sqlite_storage.get_instance(child_id)
    assert child is not None
    assert child["sandboxed"] is True


async def test_sandboxed_explicit_spawn_option_rejected(sqlite_storage, replay_engine):
    @workflow
    async def explicit_child_wf(ctx: WorkflowContext):
        return {"ok": True}

    @workflow
    async def explicit_parent_wf(ctx: WorkflowContext):
        try:
            await explicit_child_wf.start(sandboxed=True)
        except SandboxedError:
            return {"outcome": "denied"}
        return {"outcome": "allowed"}

    parent_id = await explicit_parent_wf.start(sandboxed=True)
    parent = await sqlite_storage.get_instance(parent_id)
    assert parent is not None
    assert parent["output_data"]["result"] == {"outcome": "denied"}

    # Control: an unsandboxed parent may explicitly request a sandboxed child.
    parent_id = await explicit_parent_wf.start()
    parent = await sqlite_storage.get_instance(parent_id)
    assert parent is not None
    assert parent["output_data"]["result"] == {"outcome": "allowed"}


async def test_sandboxed_inherited_across_recur(sqlite_storage, replay_engine):
    @workflow
    async def sandboxed_recur_wf(ctx: WorkflowContext, count: int = 0):
        if count == 0:
            await ctx.recur(count=1)
        return {"count": count}

    final_id = await sandboxed_recur_wf.start(sandboxed=True)
    final = await sqlite_storage.get_instance(final_id)
    assert final is not None
    assert final["sandboxed"] is True
    assert final["status"] == "completed"

    rows = await sqlite_storage.list_instances(workflow_name_filter="sandboxed_recur_wf")
    instances = rows if isinstance(rows, list) else rows.get("instances", [])
    assert instances
    for row in instances:
        inst = await sqlite_storage.get_instance(row["instance_id"])
        assert inst is not None
        assert inst["sandboxed"] is True


async def test_sandboxed_inherited_by_supervised_child(tmp_path):
    from edda.app import EddaApp as App
    from edda.supervisor import ChildSpec, RestartMode, Strategy, define_supervisor

    @workflow
    async def sandboxed_sup_child(ctx: WorkflowContext):
        return {"done": True}

    app = App(service_name="sandboxed-sup-test", db_url=f"sqlite:///{tmp_path}/sup.db")
    startup = asyncio.Event()
    shutdown = asyncio.Event()

    async def receive():
        if not startup.is_set():
            return {"type": "lifespan.startup"}
        await shutdown.wait()
        return {"type": "lifespan.shutdown"}

    async def send(message):
        if message["type"] == "lifespan.startup.complete":
            startup.set()

    task = asyncio.create_task(app({"type": "lifespan", "asgi": {"version": "3.0"}}, receive, send))
    await asyncio.wait_for(startup.wait(), timeout=5.0)
    try:
        sup = define_supervisor(
            "sandboxed-sup",
            strategy=Strategy.ONE_FOR_ONE,
            max_restarts=2,
            within_seconds=60,
            poll_seconds=1,
            children=[
                ChildSpec(
                    name="a",
                    workflow="sandboxed_sup_child",
                    input={},
                    restart=RestartMode.TEMPORARY,
                )
            ],
        )
        sup_id = await sup.start(sandboxed=True)
        child_id = f"{sup_id}:a:g1"

        deadline = asyncio.get_event_loop().time() + 10
        child = None
        while asyncio.get_event_loop().time() < deadline:
            child = await app.storage.get_instance(child_id)
            if child is not None:
                break
            await asyncio.sleep(0.1)
        assert child is not None, "supervised child row was not created"
        assert child["sandboxed"] is True
    finally:
        shutdown.set()
        await asyncio.wait_for(task, timeout=5.0)
