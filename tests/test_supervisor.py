"""End-to-end tests for the supervisor library."""

import asyncio

import pytest
import pytest_asyncio

from edda.supervisor import ChildSpec, RestartMode, Strategy, define_supervisor
from edda.workflow import workflow

pytestmark = pytest.mark.asyncio


@workflow
async def sup_child_genaware(ctx):
    """Fails on generation 1, completes on later generations."""
    if ctx.instance_id.endswith(":g1"):
        raise RuntimeError("first generation always fails")
    return {"ok": True}


@workflow
async def sup_child_always_fails(ctx):
    raise RuntimeError("always fails")


@workflow
async def sup_child_quick(ctx):
    return {"done": True}


async def boot_app(app):
    """Boot an EddaApp through its ASGI lifespan; return a closer."""
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

    async def close():
        shutdown.set()
        await asyncio.wait_for(task, timeout=5.0)

    return close


@pytest_asyncio.fixture
async def sup_app(tmp_path):
    from edda.app import EddaApp as App

    app = App(
        service_name="sup-test",
        db_url=f"sqlite:///{tmp_path}/sup.db",
    )
    close = await boot_app(app)
    yield app
    await close()


async def wait_status(app, instance_id, want, timeout=60.0):
    deadline = asyncio.get_event_loop().time() + timeout
    while asyncio.get_event_loop().time() < deadline:
        inst = await app.storage.get_instance(instance_id)
        if inst and inst["status"] == want:
            return inst
        await asyncio.sleep(0.1)
    return None


async def wait_supervisor_status(app, wf_name, want, timeout=60.0):
    """edda recur continues on NEW instances (continued_from chain) — watch
    the latest instance of the supervisor workflow instead of the start id."""
    deadline = asyncio.get_event_loop().time() + timeout
    while asyncio.get_event_loop().time() < deadline:
        rows = await app.storage.list_instances(workflow_name_filter=wf_name)
        insts = (
            rows if isinstance(rows, list) else (rows.get("instances") or rows.get("items") or [])
        )
        mine = [i for i in insts if i.get("workflow_name") == wf_name]
        if mine:
            latest = max(mine, key=lambda i: str(i.get("started_at")))
            if latest["status"] == want:
                return latest
        await asyncio.sleep(0.2)
    return None


async def find_child(app, wf_name, child_name, gen):
    rows = await app.storage.list_instances()
    insts = rows if isinstance(rows, list) else (rows.get("instances") or rows.get("items") or [])
    suffix = f":{child_name}:g{gen}"
    for i in insts:
        if i.get("instance_id", "").endswith(suffix) and wf_name.split("-")[0] in i.get(
            "instance_id", ""
        ):
            return i
    for i in insts:
        if i.get("instance_id", "").endswith(suffix):
            return i
    return None


async def test_supervisor_transient_restart(sup_app):
    """transient child fails at g1, restarts, completes at g2 — supervisor completes."""
    sup = define_supervisor(
        "sup-t1",
        strategy=Strategy.ONE_FOR_ONE,
        max_restarts=10,
        within_seconds=60,
        poll_seconds=1,
        children=[
            ChildSpec(
                name="a", workflow="sup_child_genaware", input={}, restart=RestartMode.TRANSIENT
            ),
        ],
    )
    await sup.start()

    inst = await wait_supervisor_status(sup_app, "sup-t1", "completed")
    assert inst is not None, "supervisor did not complete"
    # generation 2 must have been spawned and completed
    g2 = await find_child(sup_app, "sup-t1", "a", 2)
    assert g2 is not None and g2["status"] == "completed"


async def test_supervisor_permanent_intensity(sup_app):
    """permanent child always completes → restarted forever → intensity cap kills the supervisor."""
    sup = define_supervisor(
        "sup-t2",
        strategy=Strategy.ONE_FOR_ONE,
        max_restarts=2,
        within_seconds=60,
        poll_seconds=1,
        children=[
            ChildSpec(
                name="a", workflow="sup_child_quick", input={}, restart=RestartMode.PERMANENT
            ),
        ],
    )
    await sup.start()

    inst = await wait_supervisor_status(sup_app, "sup-t2", "failed")
    assert inst is not None, "supervisor did not hit the intensity cap"
    g2 = await find_child(sup_app, "sup-t2", "a", 2)
    assert g2 is not None, "child was never restarted"


async def test_supervisor_temporary_no_restart(sup_app):
    """temporary child fails → terminal, no restart → supervisor completes."""
    sup = define_supervisor(
        "sup-t3",
        strategy=Strategy.ONE_FOR_ONE,
        max_restarts=10,
        within_seconds=60,
        poll_seconds=1,
        children=[
            ChildSpec(
                name="a", workflow="sup_child_always_fails", input={}, restart=RestartMode.TEMPORARY
            ),
        ],
    )
    await sup.start()

    inst = await wait_supervisor_status(sup_app, "sup-t3", "completed")
    assert inst is not None, "supervisor did not complete"
    g2 = await find_child(sup_app, "sup-t3", "a", 2)
    assert g2 is None, "temporary child must not be restarted"
