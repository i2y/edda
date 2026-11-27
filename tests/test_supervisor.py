"""End-to-end tests for the supervisor library."""

import asyncio

import pytest
import pytest_asyncio

from edda.channels import sleep
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


@workflow
async def sup_child_sleeper(ctx):
    await sleep(ctx, 3600)
    return {"slept": True}


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


async def test_supervisor_cascade_stops_temporary_siblings(sup_app):
    """one_for_all: a temporary sibling caught in the cascade is stopped
    (cancelled, terminal), never restarted."""
    sup = define_supervisor(
        "sup-t4",
        strategy=Strategy.ONE_FOR_ALL,
        max_restarts=10,
        within_seconds=60,
        poll_seconds=1,
        children=[
            ChildSpec(
                name="a", workflow="sup_child_genaware", input={}, restart=RestartMode.TRANSIENT
            ),
            ChildSpec(
                name="t", workflow="sup_child_sleeper", input={}, restart=RestartMode.TEMPORARY
            ),
        ],
    )
    await sup.start()

    inst = await wait_supervisor_status(sup_app, "sup-t4", "completed", timeout=30)
    assert inst is not None, "supervisor did not complete"
    t1 = await find_child(sup_app, "sup-t4", "t", 1)
    assert t1 is not None and t1["status"] == "cancelled"
    assert await find_child(sup_app, "sup-t4", "t", 2) is None, "temporary sibling restarted"


@pytest.mark.multidb
async def test_supervisor_child_hash_for_unregistered_workflow(sqlite_storage):
    """A child workflow registered only in another process (or another
    framework) gets the hash of its stored definition, not "" — an instance
    must join workflow_definitions to be visible at all."""
    from edda.exceptions import RetryExhaustedError
    from edda.replay import ReplayEngine
    from edda.retry import RetryPolicy
    from edda.workflow import set_replay_engine

    set_replay_engine(
        ReplayEngine(
            storage=sqlite_storage,
            service_name="sup-hash",
            worker_id="w",
            default_retry_policy=RetryPolicy(max_attempts=1),
        )
    )
    try:
        await sqlite_storage.upsert_workflow_definition(
            workflow_name="sup_remote_only_wf",
            source_hash="remote-hash",
            source_code="# defined by another worker",
        )
        sup = define_supervisor(
            "sup-remote-child",
            poll_seconds=3600,
            children=[
                ChildSpec(name="r", workflow="sup_remote_only_wf", restart=RestartMode.TEMPORARY)
            ],
        )
        sup_id = await sup.start()
        child = await sqlite_storage.get_instance(f"{sup_id}:r:g1")
        assert child is not None
        assert child["source_hash"] == "remote-hash"

        # No definition anywhere: refuse to spawn an orphan row.
        orphan_sup = define_supervisor(
            "sup-undefined-child",
            poll_seconds=3600,
            children=[
                ChildSpec(name="x", workflow="sup_never_defined_wf", restart=RestartMode.TEMPORARY)
            ],
        )
        with pytest.raises(RetryExhaustedError) as excinfo:
            await orphan_sup.start()
        assert "sup_never_defined_wf" in str(excinfo.value.__cause__)
    finally:
        set_replay_engine(None)


@pytest.mark.multidb
async def test_supervisor_cancel_race_uses_actual_status(sqlite_storage, monkeypatch):
    """A cascade cancel that finds a child no longer cancellable applies the
    child's restart mode to its actual status -- a transient child that
    completed meanwhile stays done, a permanent one restarts -- and a
    compensating child is not cancelled but left for the next sync."""
    from sqlalchemy import text

    from edda.replay import ReplayEngine
    from edda.workflow import set_replay_engine

    engine = ReplayEngine(storage=sqlite_storage, service_name="sup-race", worker_id="w")
    set_replay_engine(engine)
    try:
        sup = define_supervisor(
            "sup-cancel-race",
            strategy=Strategy.ONE_FOR_ALL,
            max_restarts=10,
            within_seconds=60,
            poll_seconds=3600,
            children=[
                ChildSpec(
                    name="dead", workflow="sup_child_always_fails", restart=RestartMode.TRANSIENT
                ),
                ChildSpec(name="done", workflow="sup_child_quick", restart=RestartMode.TRANSIENT),
                ChildSpec(name="again", workflow="sup_child_quick", restart=RestartMode.PERMANENT),
                ChildSpec(name="comp", workflow="sup_child_quick", restart=RestartMode.TRANSIENT),
            ],
        )
        sup_id = await sup.start()  # first sync: spawns every child, then sleeps

        async def set_status(suffix, status):
            async with sqlite_storage.engine.begin() as conn:
                await conn.execute(
                    text("UPDATE workflow_instances SET status = :s WHERE instance_id = :i"),
                    {"s": status, "i": f"{sup_id}:{suffix}"},
                )

        await set_status("dead:g1", "failed")  # triggers the one_for_all cascade
        await set_status("comp:g1", "compensating")
        orig_cancel = sqlite_storage.cancel_instance

        async def cancel_after_the_child_finished(instance_id, cancelled_by, **kwargs):
            if instance_id.endswith((":done:g1", ":again:g1")):
                # The child completed between the sync's read and the cancel.
                await set_status(instance_id.split(":", 1)[1], "completed")
            return await orig_cancel(instance_id, cancelled_by, **kwargs)

        monkeypatch.setattr(sqlite_storage, "cancel_instance", cancel_after_the_child_finished)

        # Second sync: fire the poll timer and resume (it recurs into the sync).
        async with sqlite_storage.engine.begin() as conn:
            timer_id, activity_id = (
                await conn.execute(
                    text(
                        "SELECT timer_id, activity_id FROM workflow_timer_subscriptions "
                        "WHERE instance_id = :i"
                    ),
                    {"i": sup_id},
                )
            ).one()
        await sqlite_storage.append_history(
            sup_id, activity_id, "TimerExpired", {"result": None, "timer_id": timer_id}
        )
        await sqlite_storage.remove_timer_subscription(sup_id, timer_id)
        await sqlite_storage.update_instance_status(sup_id, "running")
        await engine.resume_by_name(sup_id, "sup-cancel-race")
        monkeypatch.undo()

        async with sqlite_storage.engine.begin() as conn:
            rows = (
                await conn.execute(
                    text(
                        "SELECT instance_id, status FROM workflow_instances "
                        "WHERE instance_id LIKE :p"
                    ),
                    {"p": f"{sup_id}%"},
                )
            ).all()
            replacements = (
                await conn.execute(
                    text("SELECT instance_id FROM workflow_instances WHERE instance_id LIKE :p"),
                    {"p": "%:g2"},
                )
            ).all()
        g1 = {r[0].split(":", 1)[1]: r[1] for r in rows if r[0].endswith(":g1")}
        g2 = sorted(r[0].rsplit(":", 2)[1] for r in replacements)
        assert g1 == {
            "dead:g1": "failed",
            "done:g1": "completed",
            "again:g1": "completed",
            "comp:g1": "compensating",
        }
        assert g2 == ["again", "dead"]
    finally:
        set_replay_engine(None)


@workflow
async def sup_child_recur_once(ctx, phase: int = 0):
    if phase == 0:
        await ctx.recur(phase=1)
    return {"phase": phase}


async def test_supervisor_recur_keeps_its_id_and_children(sup_app):
    """The supervisor loops by recur, but its instance id and deterministic
    child ids are stable across those iterations."""
    sup = define_supervisor(
        "sup-recur-stable",
        strategy=Strategy.ONE_FOR_ONE,
        max_restarts=10,
        within_seconds=60,
        poll_seconds=1,
        children=[
            ChildSpec(name="a", workflow="sup_child_quick", restart=RestartMode.TEMPORARY),
        ],
    )
    sup_id = await sup.start()

    inst = await wait_status(sup_app, sup_id, "completed", timeout=15)
    assert inst is not None, "supervisor did not complete on its original instance id"
    rows = await sup_app.storage.list_instances(workflow_name_filter="sup-recur-stable")
    insts = rows if isinstance(rows, list) else (rows.get("instances") or rows.get("items") or [])
    assert [i["instance_id"] for i in insts] == [sup_id]
    g1 = await sup_app.storage.get_instance(f"{sup_id}:a:g1")
    assert g1 is not None and g1["status"] == "completed"
    assert await sup_app.storage.get_instance(f"{sup_id}:a:g2") is None


async def test_supervised_child_that_recurs_is_the_same_live_child(sup_app):
    """A child in recurred/running iteration is alive; no replacement generation
    is spawned, and the deterministic child id is the row that completes."""
    sup = define_supervisor(
        "sup-child-recur",
        strategy=Strategy.ONE_FOR_ONE,
        max_restarts=10,
        within_seconds=60,
        poll_seconds=1,
        children=[
            ChildSpec(
                name="r",
                workflow="sup_child_recur_once",
                input={"phase": 0},
                restart=RestartMode.TEMPORARY,
            ),
        ],
    )
    sup_id = await sup.start()

    inst = await wait_status(sup_app, sup_id, "completed", timeout=15)
    assert inst is not None, "supervisor did not complete"
    child_id = f"{sup_id}:r:g1"
    child = await sup_app.storage.get_instance(child_id)
    assert child is not None
    assert child["status"] == "completed"
    assert child["input_data"] == {"phase": 1}
    rows = await sup_app.storage.list_instances(workflow_name_filter="sup_child_recur_once")
    insts = rows if isinstance(rows, list) else (rows.get("instances") or rows.get("items") or [])
    assert [i["instance_id"] for i in insts] == [child_id]
