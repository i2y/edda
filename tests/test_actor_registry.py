"""Actor registry tests: register/whereis, monitors, terminal DOWN sweep."""

import asyncio

import pytest
from sqlalchemy import text

from edda import actors
from edda.channels import sleep
from edda.context import WorkflowContext
from edda.replay import ReplayEngine
from edda.workflow import set_replay_engine, workflow

pytestmark = pytest.mark.multidb


@pytest.mark.asyncio
async def test_register_whereis_unregister(sqlite_storage, create_test_instance):
    iid = await create_test_instance("wf-actor-1", "wf", {})
    other = await create_test_instance("wf-actor-2", "wf", {})

    await sqlite_storage.register_actor_name("order-actor", iid)
    assert await sqlite_storage.whereis_actor("order-actor") == iid
    assert await sqlite_storage.whereis_actor("nope") is None

    # registering again is a no-op for the holder; another instance is refused
    await sqlite_storage.register_actor_name("order-actor", iid)
    with pytest.raises(ValueError, match="already registered"):
        await sqlite_storage.register_actor_name("order-actor", other)
    assert await sqlite_storage.whereis_actor("order-actor") == iid

    await sqlite_storage.unregister_actor_name("order-actor")
    await sqlite_storage.register_actor_name("order-actor", other)
    assert await sqlite_storage.whereis_actor("order-actor") == other


@pytest.mark.asyncio
async def test_monitor_down_delivery(sqlite_storage, create_test_instance):
    watcher = await create_test_instance("wf-watcher", "wf", {})
    target = await create_test_instance("wf-target", "wf", {})

    await sqlite_storage.monitor_actor(watcher, target, "mon:wf-watcher")
    # idempotent re-monitor
    await sqlite_storage.monitor_actor(watcher, target, "mon:wf-watcher")

    # not terminal yet — nothing emitted
    assert await sqlite_storage.emit_actor_down_for_terminated() == 0

    await sqlite_storage.update_instance_status(target, "completed")
    assert await sqlite_storage.emit_actor_down_for_terminated() == 1

    # DOWN message delivered to the monitor channel
    from sqlalchemy import text

    async with sqlite_storage.engine.begin() as conn:
        rows = (
            await conn.execute(
                text("SELECT data FROM channel_messages WHERE channel = 'mon:wf-watcher'")
            )
        ).all()
    assert len(rows) == 1
    import json

    payload = json.loads(rows[0][0])
    assert payload == {"type": "DOWN", "actor": target, "status": "completed"}

    # monitor row consumed — re-emit is a no-op
    assert await sqlite_storage.emit_actor_down_for_terminated() == 0

    await sqlite_storage.monitor_actor(watcher, target, "mon:wf-watcher")
    await sqlite_storage.unmonitor_actor(watcher, target)
    assert await sqlite_storage.emit_actor_down_for_terminated() == 0


@pytest.mark.asyncio
async def test_name_freed_on_terminal(sqlite_storage, create_test_instance):
    watcher = await create_test_instance("wf-w", "wf", {})
    target = await create_test_instance("wf-t", "wf", {})

    await sqlite_storage.register_actor_name("named", target)
    await sqlite_storage.monitor_actor(watcher, target, "mon:wf-w")
    await sqlite_storage.update_instance_status(target, "failed")
    await sqlite_storage.emit_actor_down_for_terminated()

    assert await sqlite_storage.whereis_actor("named") is None


@pytest.mark.asyncio
async def test_sweep_frees_the_names_of_all_terminated_instances(
    sqlite_storage, create_test_instance
):
    """Names are freed for every terminated instance, monitored or not. A
    recurred instance is not terminated (its continuation runs on)."""
    done = await create_test_instance("wf-name-done", "wf", {})
    recurred = await create_test_instance("wf-name-recurred", "wf", {})
    alive = await create_test_instance("wf-name-alive", "wf", {})
    for name, iid in (("n-done", done), ("n-recurred", recurred), ("n-alive", alive)):
        await sqlite_storage.register_actor_name(name, iid)
    await sqlite_storage.update_instance_status(done, "cancelled")
    await sqlite_storage.update_instance_status(recurred, "recurred")

    assert await sqlite_storage.emit_actor_down_for_terminated() == 0
    assert await sqlite_storage.whereis_actor("n-done") is None
    assert await sqlite_storage.whereis_actor("n-recurred") == recurred
    assert await sqlite_storage.whereis_actor("n-alive") == alive


@pytest.mark.asyncio
async def test_overlapping_sweeps_emit_one_down(sqlite_storage, create_test_instance):
    watcher = await create_test_instance("wf-once-watcher", "wf", {})
    target = await create_test_instance("wf-once-target", "wf", {})
    await sqlite_storage.monitor_actor(watcher, target, "mon:wf-once-watcher")
    await sqlite_storage.update_instance_status(target, "failed")

    emitted = await asyncio.gather(
        sqlite_storage.emit_actor_down_for_terminated(),
        sqlite_storage.emit_actor_down_for_terminated(),
    )

    assert sum(emitted) == 1
    async with sqlite_storage.engine.begin() as conn:
        rows = (
            await conn.execute(
                text("SELECT data FROM channel_messages WHERE channel = 'mon:wf-once-watcher'")
            )
        ).all()
    assert len(rows) == 1


@pytest.mark.asyncio
async def test_taken_name_keeps_the_enclosing_transaction_usable(
    sqlite_storage, create_test_instance
):
    """A refused registration must not abort the caller's transaction (on
    PostgreSQL a caught unique violation would)."""
    holder = await create_test_instance("wf-name-holder", "wf", {})
    other = await create_test_instance("wf-name-other", "wf", {})
    await sqlite_storage.register_actor_name("taken", holder)

    await sqlite_storage.begin_transaction()
    try:
        await sqlite_storage.append_history(
            other, activity_id="before:1", event_type="ActivityCompleted", event_data={"r": 1}
        )
        with pytest.raises(ValueError, match="already registered"):
            await sqlite_storage.register_actor_name("taken", other)
        await sqlite_storage.append_history(
            other, activity_id="after:1", event_type="ActivityCompleted", event_data={"r": 2}
        )
        await sqlite_storage.commit_transaction()
    except BaseException:
        if sqlite_storage.in_transaction():
            await sqlite_storage.rollback_transaction()
        raise
    history = await sqlite_storage.get_history(other)
    assert sorted(h["activity_id"] for h in history) == ["after:1", "before:1"]
    assert await sqlite_storage.whereis_actor("taken") == holder


@pytest.fixture
def replay_engine(sqlite_storage):
    engine = ReplayEngine(storage=sqlite_storage, service_name="reg-test", worker_id="reg-worker")
    set_replay_engine(engine)
    yield engine
    set_replay_engine(None)


async def _fire_timer_and_resume(storage, engine, iid, wf):
    """Expire the instance's pending timer and resume it, as EddaApp does."""
    async with storage.engine.begin() as conn:
        timer_id, activity_id = (
            await conn.execute(
                text(
                    "SELECT timer_id, activity_id FROM workflow_timer_subscriptions "
                    "WHERE instance_id = :i"
                ),
                {"i": iid},
            )
        ).one()
    await storage.append_history(
        iid,
        activity_id=activity_id,
        event_type="TimerExpired",
        event_data={"result": None, "timer_id": timer_id},
    )
    await storage.remove_timer_subscription(iid, timer_id)
    await storage.update_instance_status(iid, "running")
    await engine.resume_workflow(iid, wf.func)


@pytest.mark.asyncio
async def test_register_name_is_a_recorded_step(sqlite_storage, replay_engine):
    """Registration runs once and replays from history: the holder's replay
    does not hit the unique name again, and a refused registration replays
    as the same ValueError."""

    @workflow
    async def billing_actor(ctx: WorkflowContext) -> dict:
        await actors.register_name(ctx, "billing")
        await actors.register_name(ctx, "billing")  # idempotent for the holder
        await sleep(ctx, 1)
        return {"me": await actors.whereis(ctx, "billing")}

    @workflow
    async def name_thief(ctx: WorkflowContext) -> dict:
        try:
            await actors.register_name(ctx, "billing")
            taken = False
        except ValueError:
            taken = True
        await sleep(ctx, 1)
        return {"taken": taken}

    owner = await billing_actor.start()
    thief = await name_thief.start()
    await _fire_timer_and_resume(sqlite_storage, replay_engine, owner, billing_actor)
    await _fire_timer_and_resume(sqlite_storage, replay_engine, thief, name_thief)

    assert (await sqlite_storage.get_instance(owner))["output_data"]["result"] == {"me": owner}
    assert (await sqlite_storage.get_instance(thief))["output_data"]["result"] == {"taken": True}
    assert await sqlite_storage.whereis_actor("billing") == owner


@pytest.mark.asyncio
async def test_monitor_replay_does_not_rearm(sqlite_storage, replay_engine, create_test_instance):
    """A replayed monitor() must not re-insert the monitor row the DOWN sweep
    consumed — that would deliver a second DOWN for the same death."""
    target = await create_test_instance("wf-mon-target", "wf", {})

    @workflow
    async def watcher_wf(ctx: WorkflowContext, target: str) -> dict:
        await actors.monitor(ctx, target, "mon")
        await sleep(ctx, 1)
        return {}

    watcher = await watcher_wf.start(target=target)
    await sqlite_storage.update_instance_status(target, "completed")
    assert await sqlite_storage.emit_actor_down_for_terminated() == 1

    await _fire_timer_and_resume(sqlite_storage, replay_engine, watcher, watcher_wf)
    assert (await sqlite_storage.get_instance(watcher))["status"] == "completed"
    assert await sqlite_storage.emit_actor_down_for_terminated() == 0
