"""Task primitive + cursor-based channel reads (Vercel Board-style)."""

import asyncio
import json
import time
from datetime import UTC, datetime

import pytest
from sqlalchemy import text

from edda import tasks
from edda.activity import activity
from edda.channels import sleep
from edda.context import WorkflowContext
from edda.exceptions import ChannelPermissionError, CursorNotFoundError, TaskConflictError
from edda.replay import ReplayEngine
from edda.workflow import set_replay_engine, workflow

pytestmark = pytest.mark.multidb


@pytest.mark.asyncio
async def test_task_claim_and_complete(sqlite_storage, create_test_instance):
    w1 = await create_test_instance("wf-w1", "wf", {})
    w2 = await create_test_instance("wf-w2", "wf", {})

    await sqlite_storage.post_task("t1", "tasks:x", '{"n":1}')
    await sqlite_storage.post_task("t2", "tasks:x", '{"n":2}')
    await sqlite_storage.post_task("t3", "tasks:y", '{"n":3}')

    task = await sqlite_storage.claim_task("tasks:x", w1)
    assert task is not None
    assert task["id"] == "t1"
    assert task["state"] == "claimed"
    assert task["claimed_by"] == w1

    task2 = await sqlite_storage.claim_task("tasks:x", w2)
    assert task2 is not None and task2["id"] == "t2"

    # none left on tasks:x
    assert await sqlite_storage.claim_task("tasks:x", "wf-w3") is None

    # only claimant may complete
    with pytest.raises(TaskConflictError):
        await sqlite_storage.complete_task("t1", w2, '{"ok":true}', failed=False)
    await sqlite_storage.complete_task("t1", w1, '{"ok":true}', failed=False)
    await sqlite_storage.complete_task("t2", w2, '{"err":"x"}', failed=True)

    tasks = await sqlite_storage.list_tasks("tasks:x")
    assert [t["state"] for t in tasks] == ["done", "failed"]
    assert await sqlite_storage.list_tasks("tasks:x", "open") == []
    assert len(await sqlite_storage.list_tasks("tasks:y", "open")) == 1


@pytest.mark.asyncio
async def test_channel_messages_since(sqlite_storage, create_test_instance):
    await create_test_instance("wf-cur", "wf", {})

    ids = [await sqlite_storage.publish_to_channel("cur", {"i": n}) for n in (1, 2, 3)]
    all_msgs = await sqlite_storage.channel_messages_since("cur", None)
    assert len(all_msgs) == 3
    assert all_msgs[0]["message_id"] == ids[0]

    rest = await sqlite_storage.channel_messages_since("cur", ids[0])
    assert len(rest) == 2
    assert rest[0]["message_id"] == ids[1]

    end = await sqlite_storage.channel_messages_since("cur", ids[2])
    assert end == []

    # an unknown cursor is an error, not "read everything"
    with pytest.raises(CursorNotFoundError):
        await sqlite_storage.channel_messages_since("cur", "no-such-id")


@pytest.mark.asyncio
async def test_task_event_stream_and_get_task(sqlite_storage, create_test_instance):
    w1 = await create_test_instance("wf-w1", "wf", {})

    await sqlite_storage.post_task("t1", "tasks:x", '{"n":1}')
    msgs = await sqlite_storage.channel_messages_since("tasks:x:tasks", None)
    assert len(msgs) == 1
    import json as _json

    assert _json.loads(msgs[0]["data"]) == {
        "type": "task_posted",
        "task_id": "t1",
        "channel": "tasks:x",
    }

    got = await sqlite_storage.get_task("t1")
    assert got is not None and got["state"] == "open"
    assert await sqlite_storage.get_task("nope") is None

    task = await sqlite_storage.claim_task("tasks:x", w1)
    assert task["id"] == "t1"
    await sqlite_storage.complete_task("t1", w1, '{"ok":true}', failed=False)

    msgs = await sqlite_storage.channel_messages_since("tasks:x:tasks", msgs[0]["message_id"])
    assert len(msgs) == 2
    assert _json.loads(msgs[0]["data"])["type"] == "task_claimed"
    assert _json.loads(msgs[1]["data"]) == {
        "type": "task_state",
        "task_id": "t1",
        "state": "done",
    }


@pytest.mark.asyncio
async def test_reclaim_stale_tasks(sqlite_storage, create_test_instance):
    await create_test_instance("wf-r1", "wf", {})
    await create_test_instance("wf-r2", "wf", {})

    await sqlite_storage.post_task("tr1", "tasks:r", "{}")
    await sqlite_storage.post_task("tr2", "tasks:r", "{}")
    assert (await sqlite_storage.claim_task("tasks:r", "wf-r1"))["id"] == "tr1"
    assert (await sqlite_storage.claim_task("tasks:r", "wf-r2"))["id"] == "tr2"

    # attempts incremented on claim
    assert (await sqlite_storage.get_task("tr1"))["attempts"] == 1

    # fresh claims are not stale
    assert await sqlite_storage.reclaim_stale_tasks(3600) == 0

    # backdate tr1's claimed_at to simulate an orphaned claim
    from sqlalchemy import text

    session = sqlite_storage._get_session_for_operation()
    async with sqlite_storage._session_scope(session) as session:
        await session.execute(
            text("UPDATE actor_tasks SET claimed_at = :c WHERE id = 'tr1'"),
            {"c": sqlite_storage._ts_param(datetime(2000, 1, 1, tzinfo=UTC), tz=True)},
        )
        await session.commit()

    assert await sqlite_storage.reclaim_stale_tasks(3600) == 1
    t = await sqlite_storage.get_task("tr1")
    assert t["state"] == "open" and t["claimed_by"] is None
    # fresh claim untouched
    t2 = await sqlite_storage.get_task("tr2")
    assert t2["state"] == "claimed" and t2["claimed_by"] == "wf-r2"


@pytest.fixture
def replay_engine(sqlite_storage):
    engine = ReplayEngine(storage=sqlite_storage, service_name="task-test", worker_id="task-worker")
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


async def _fire_message_timeout(storage, engine, iid, wf):
    """Time out the instance's pending receive and resume it, as EddaApp does."""
    async with storage.engine.begin() as conn:
        channel, activity_id = (
            await conn.execute(
                text(
                    "SELECT channel, activity_id FROM channel_subscriptions "
                    "WHERE instance_id = :i AND activity_id IS NOT NULL"
                ),
                {"i": iid},
            )
        ).one()
    await storage.append_history(
        iid,
        activity_id=activity_id,
        event_type="MessageTimeout",
        event_data={
            "_error": True,
            "error_type": "TimeoutError",
            "error_message": f"Message on channel '{channel}' did not arrive within timeout",
            "channel": channel,
        },
    )
    await storage.remove_message_subscription(iid, channel)
    await storage.update_instance_status(iid, "running")
    await engine.resume_workflow(iid, wf.func)


async def _wait_timeout_at(storage, iid):
    async with storage.engine.begin() as conn:
        value = (
            await conn.execute(
                text(
                    "SELECT timeout_at FROM channel_subscriptions "
                    "WHERE instance_id = :i AND activity_id IS NOT NULL"
                ),
                {"i": iid},
            )
        ).scalar_one()
    return datetime.fromisoformat(str(value))


@pytest.mark.asyncio
async def test_claim_task_is_a_recorded_step(sqlite_storage, replay_engine):
    """A replayed claim returns the recorded task instead of claiming another
    one (which would then be completed with the first task's result)."""

    @activity
    async def do_work(ctx: WorkflowContext, data: str) -> dict:
        return {"processed": data}

    @workflow
    async def claim_worker(ctx: WorkflowContext) -> dict:
        task = await tasks.claim_task(ctx, "jobs")
        result = await do_work(ctx, task["data"])
        await tasks.complete_task(ctx, task["id"], result)
        await sleep(ctx, 1)
        return {"task": task["id"]}

    await sqlite_storage.post_task("A", "jobs", "payload-A")
    iid = await claim_worker.start()
    await sqlite_storage.post_task("B", "jobs", "payload-B")
    await _fire_timer_and_resume(sqlite_storage, replay_engine, iid, claim_worker)

    assert (await sqlite_storage.get_instance(iid))["output_data"]["result"] == {"task": "A"}
    a = await sqlite_storage.get_task("A")
    b = await sqlite_storage.get_task("B")
    assert a["state"] == "done" and json.loads(a["result"]) == {"processed": "payload-A"}
    assert b["state"] == "open" and b["claimed_by"] is None


@pytest.mark.asyncio
async def test_post_task_is_a_recorded_step(sqlite_storage, replay_engine):
    @workflow
    async def poster_wf(ctx: WorkflowContext) -> dict:
        task_id = await tasks.post_task(ctx, "jobs:post", {"n": 1})
        await sleep(ctx, 1)
        return {"task_id": task_id}

    iid = await poster_wf.start()
    await _fire_timer_and_resume(sqlite_storage, replay_engine, iid, poster_wf)

    task_id = (await sqlite_storage.get_instance(iid))["output_data"]["result"]["task_id"]
    assert [t["id"] for t in await sqlite_storage.list_tasks("jobs:post")] == [task_id]


@pytest.mark.asyncio
async def test_task_conflict_replays_as_task_conflict(sqlite_storage, replay_engine):
    await sqlite_storage.post_task("C", "jobs:conflict", "{}")
    assert (await sqlite_storage.claim_task("jobs:conflict", "wf-other"))["id"] == "C"

    @workflow
    async def intruder_wf(ctx: WorkflowContext) -> dict:
        try:
            await tasks.complete_task(ctx, "C", {"stolen": True})
            outcome = "completed"
        except TaskConflictError:
            outcome = "conflict"
        await sleep(ctx, 1)
        return {"outcome": outcome}

    iid = await intruder_wf.start()
    await _fire_timer_and_resume(sqlite_storage, replay_engine, iid, intruder_wf)
    inst = await sqlite_storage.get_instance(iid)
    assert inst["status"] == "completed"
    assert inst["output_data"]["result"] == {"outcome": "conflict"}


@pytest.mark.asyncio
async def test_wait_task_returns_when_task_settles(sqlite_storage, replay_engine):
    await sqlite_storage.post_task("W", "jobs:ok", "{}")

    @workflow
    async def settle_waiter(ctx: WorkflowContext) -> dict:
        task = await tasks.wait_task(ctx, "W")
        return {"state": task["state"], "result": task["result"]}

    iid = await settle_waiter.start()
    assert (await sqlite_storage.get_instance(iid))["status"] == "waiting_for_message"

    assert (await sqlite_storage.claim_task("jobs:ok", "wf-worker"))["id"] == "W"
    await sqlite_storage.complete_task("W", "wf-worker", '{"ok": true}', failed=False)
    assert await sqlite_storage.requeue_channel_waits_with_pending() == 1
    await replay_engine.resume_workflow(iid, settle_waiter.func)

    inst = await sqlite_storage.get_instance(iid)
    assert inst["status"] == "completed"
    assert inst["output_data"]["result"] == {"state": "done", "result": '{"ok": true}'}


@pytest.mark.asyncio
async def test_wait_task_recorded_timeout_ends_the_wait(sqlite_storage, replay_engine):
    await sqlite_storage.post_task("T", "jobs:wait", "{}")  # nobody completes it

    @workflow
    async def task_waiter(ctx: WorkflowContext) -> dict:
        try:
            await tasks.wait_task(ctx, "T", timeout_seconds=1)
        except TimeoutError:
            return {"outcome": "timed_out"}
        return {"outcome": "done"}

    iid = await task_waiter.start()
    assert (await sqlite_storage.get_instance(iid))["status"] == "waiting_for_message"
    await asyncio.sleep(1.1)
    await _fire_message_timeout(sqlite_storage, replay_engine, iid, task_waiter)

    inst = await sqlite_storage.get_instance(iid)
    assert inst["status"] == "completed"
    assert inst["output_data"]["result"] == {"outcome": "timed_out"}


@pytest.mark.asyncio
async def test_wait_any_task_recorded_timeout_ends_the_wait(sqlite_storage, replay_engine):
    await sqlite_storage.post_task("U", "jobs:any", "{}")  # nobody completes it

    @workflow
    async def any_waiter(ctx: WorkflowContext) -> dict:
        try:
            await tasks.wait_any_task(ctx, "jobs:any", timeout_seconds=1)
        except TimeoutError:
            return {"outcome": "timed_out"}
        return {"outcome": "done"}

    iid = await any_waiter.start()
    assert (await sqlite_storage.get_instance(iid))["status"] == "waiting_for_message"
    await asyncio.sleep(1.1)
    await _fire_message_timeout(sqlite_storage, replay_engine, iid, any_waiter)

    inst = await sqlite_storage.get_instance(iid)
    assert inst["status"] == "completed"
    assert inst["output_data"]["result"] == {"outcome": "timed_out"}


@pytest.mark.asyncio
async def test_wait_task_deadline_is_fixed_across_wakes(sqlite_storage, replay_engine, monkeypatch):
    """Each timed receive waits toward the deadline recorded by the first run,
    not a fresh full timeout computed again by every resumed run."""
    await sqlite_storage.post_task("D", "jobs:deadline", "{}")

    @workflow
    async def deadline_waiter(ctx: WorkflowContext) -> dict:
        try:
            task = await tasks.wait_task(ctx, "D", timeout_seconds=3600)
        except TimeoutError:
            return {"outcome": "timed_out"}
        return {"outcome": task["state"]}

    iid = await deadline_waiter.start()
    first_timeout_at = await _wait_timeout_at(sqlite_storage, iid)

    # A non-terminal notice (task_claimed) wakes the waiter 30 minutes later.
    real_time = time.time
    monkeypatch.setattr(time, "time", lambda: real_time() + 1800)
    assert (await sqlite_storage.claim_task("jobs:deadline", "wf-worker"))["id"] == "D"
    assert await sqlite_storage.requeue_channel_waits_with_pending() == 1
    await replay_engine.resume_workflow(iid, deadline_waiter.func)

    assert (await sqlite_storage.get_instance(iid))["status"] == "waiting_for_message"
    second_timeout_at = await _wait_timeout_at(sqlite_storage, iid)
    # Re-registered with the ~1800 s left, not another 3600 s.
    assert (first_timeout_at - second_timeout_at).total_seconds() > 1000


@pytest.mark.asyncio
async def test_claim_task_lost_race_takes_next_task(sqlite_storage):
    """A claimant that loses the race for the oldest task moves on to the next
    open task instead of reporting the channel empty."""
    storage = sqlite_storage
    if storage.engine.dialect.name == "sqlite":
        pytest.skip("needs two concurrent writers")
    await storage.post_task("ra", "tasks:race", "{}")
    await storage.post_task("rb", "tasks:race", "{}")
    a_claimed = asyncio.Event()

    async def first_claimant():
        await storage.begin_transaction()
        try:
            task = await storage.claim_task("tasks:race", "wf-a")
            a_claimed.set()
            await asyncio.sleep(0.5)  # the second claimant now waits on our row
            await storage.commit_transaction()
            return task
        except BaseException:
            await storage.rollback_transaction()
            raise

    async def second_claimant():
        await a_claimed.wait()
        return await storage.claim_task("tasks:race", "wf-b")

    a, b = await asyncio.gather(first_claimant(), second_claimant())
    assert a is not None and b is not None
    assert {a["id"], b["id"]} == {"ra", "rb"}
    assert b["claimed_by"] == "wf-b"
    assert (await storage.get_task(b["id"]))["claimed_by"] == "wf-b"


@pytest.mark.asyncio
async def test_read_channel_since_enforces_channel_sub(sqlite_storage, create_test_instance):
    from edda.actors import _hash_secret

    iid = await create_test_instance("wf-reader", "wf", {})
    ctx = WorkflowContext(
        instance_id=iid, workflow_name="wf", storage=sqlite_storage, worker_id="w1"
    )
    await sqlite_storage.publish_to_channel("feed", {"n": 1})
    await sqlite_storage.grant_capability(
        capability_id="cap-feed-sub",
        kind="channel_sub",
        target="feed",
        secret_hash=_hash_secret("s3cret"),
    )

    with pytest.raises(ChannelPermissionError):
        await tasks.read_channel_since(ctx, "feed")
    with pytest.raises(ChannelPermissionError):
        await tasks.read_channel_since(ctx, "feed", capability_token="cap:channel_sub:feed:wrong")
    msgs = await tasks.read_channel_since(
        ctx, "feed", capability_token="cap:channel_sub:feed:s3cret"
    )
    assert [json.loads(m["data"]) for m in msgs] == [{"n": 1}]


@pytest.mark.asyncio
async def test_read_channel_since_is_a_recorded_step(sqlite_storage, replay_engine):
    await sqlite_storage.publish_to_channel("feed2", {"n": 1})
    seen = []

    @workflow
    async def reader_wf(ctx: WorkflowContext) -> dict:
        msgs = await tasks.read_channel_since(ctx, "feed2")
        seen.append(msgs)
        await sleep(ctx, 1)
        return {"count": len(msgs)}

    iid = await reader_wf.start()
    await sqlite_storage.publish_to_channel("feed2", {"n": 2})
    await _fire_timer_and_resume(sqlite_storage, replay_engine, iid, reader_wf)

    assert (await sqlite_storage.get_instance(iid))["output_data"]["result"] == {"count": 1}
    assert len(seen) == 2 and seen[0] == seen[1]
    assert isinstance(seen[1][0]["published_at"], datetime)


@pytest.mark.asyncio
async def test_settling_a_task_again_is_idempotent_for_its_claimant(
    sqlite_storage, create_test_instance
):
    """A retried complete/fail by the claimant into the same state succeeds
    without a second notice; anything else is still a conflict."""
    claimant = await create_test_instance("wf-settle-a", "wf", {})
    other = await create_test_instance("wf-settle-b", "wf", {})
    await sqlite_storage.post_task("task-settle", "settle", json.dumps({"n": 1}))
    assert (await sqlite_storage.claim_task("settle", claimant))["id"] == "task-settle"

    await sqlite_storage.complete_task("task-settle", claimant, '{"ok": true}', failed=False)
    await sqlite_storage.complete_task("task-settle", claimant, '{"ok": true}', failed=False)
    with pytest.raises(TaskConflictError):
        await sqlite_storage.complete_task("task-settle", claimant, None, failed=True)
    with pytest.raises(TaskConflictError):
        await sqlite_storage.complete_task("task-settle", other, None, failed=False)

    assert (await sqlite_storage.get_task("task-settle"))["state"] == "done"
    notices = await sqlite_storage.channel_messages_since(tasks.tasks_channel("settle"), None)
    states = [json.loads(m["data"]) if isinstance(m["data"], str) else m["data"] for m in notices]
    assert [n.get("state") for n in states if n["type"] == "task_state"] == ["done"]
