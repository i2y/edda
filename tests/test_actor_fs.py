"""Tests for the shared actor filesystem (CAS + fs_write capability)."""

import asyncio

import pytest
from sqlalchemy import text

from edda import actors, fs
from edda.activity import activity
from edda.channels import sleep
from edda.context import WorkflowContext
from edda.exceptions import FSConflictError, FSForbiddenError, FSOutsideActivityError
from edda.replay import ReplayEngine
from edda.retry import RetryPolicy
from edda.workflow import set_replay_engine, workflow

pytestmark = pytest.mark.multidb


def _ctx(storage, iid="fs-actor-1"):
    return WorkflowContext(instance_id=iid, workflow_name="wf", storage=storage, worker_id="w1")


def _in_activity(ctx):
    ctx._activity_depth = 1
    return ctx


@pytest.mark.asyncio
async def test_fs_cas_roundtrip(sqlite_storage):
    assert (await sqlite_storage.fs_get("/shared/x")) is None
    await sqlite_storage.fs_put("/shared/x", b"v1", "wf-1", 0)
    with pytest.raises(FSConflictError):
        await sqlite_storage.fs_put("/shared/x", b"x", "wf-1", 0)
    obj = await sqlite_storage.fs_get("/shared/x")
    assert obj["data"] == b"v1" and obj["version"] == 1
    with pytest.raises(FSConflictError):
        await sqlite_storage.fs_put("/shared/x", b"v2", "wf-1", 9)
    await sqlite_storage.fs_put("/shared/x", b"v2", "wf-1", 1)
    assert (await sqlite_storage.fs_get("/shared/x"))["version"] == 2
    with pytest.raises(FSConflictError):
        await sqlite_storage.fs_delete("/shared/x", 1)
    await sqlite_storage.fs_delete("/shared/x", 2)
    assert (await sqlite_storage.fs_get("/shared/x")) is None


@pytest.mark.asyncio
async def test_fs_outside_activity_rejected(sqlite_storage):
    ctx = _ctx(sqlite_storage)
    with pytest.raises(FSOutsideActivityError):
        await fs.fs_write(ctx, "/actor/fs-actor-1/f", b"d", 0)
    with pytest.raises(FSOutsideActivityError):
        await fs.fs_read(ctx, "/shared/f")


@pytest.mark.asyncio
async def test_fs_permissions(sqlite_storage):
    me = _in_activity(_ctx(sqlite_storage, "actor-a"))
    other = _in_activity(_ctx(sqlite_storage, "actor-b"))

    # own private path OK
    await fs.fs_write(me, "/actor/actor-a/notes", b"n", 0)
    data, ver = await fs.fs_read(me, "/actor/actor-a/notes")
    assert data == b"n" and ver == 1

    # other actor's private path forbidden (read + write)
    with pytest.raises(FSForbiddenError):
        await fs.fs_write(other, "/actor/actor-a/notes", b"x", 1)
    with pytest.raises(FSForbiddenError):
        await fs.fs_read(other, "/actor/actor-a/notes")

    # shared write requires capability
    with pytest.raises(FSForbiddenError):
        await fs.fs_write(me, "/shared/doc", b"s", 0)
    await sqlite_storage.upsert_workflow_definition(
        workflow_name="wf", source_hash="wf-hash", source_code="async def wf(ctx): pass"
    )
    await sqlite_storage.create_instance(
        instance_id="actor-a",
        workflow_name="wf",
        source_hash="wf-hash",
        owner_service="test",
        input_data={},
    )
    token, _cap_id = await actors.issue_capability(me, "fs_write", "/shared/")
    await fs.fs_write(me, "/shared/doc", b"s", 0, capability_token=token)
    data, _ = await fs.fs_read(other, "/shared/doc")  # shared read open
    assert data == b"s"

    # bad path rejected
    with pytest.raises(FSForbiddenError):
        await fs.fs_write(me, "/etc/passwd", b"x", 0)


@pytest.mark.asyncio
async def test_fs_list(sqlite_storage):
    await sqlite_storage.fs_put("/actor/wf-1/a.txt", b"x", "wf-1", 0)
    await sqlite_storage.fs_put("/actor/wf-1/b.txt", b"x", "wf-1", 0)
    await sqlite_storage.fs_put("/actor/wf-2/c.txt", b"x", "wf-2", 0)
    await sqlite_storage.fs_put("/shared/pub/x.txt", b"x", "wf-1", 0)

    lst = await sqlite_storage.fs_list("/actor/wf-1/")
    assert [o["path"] for o in lst] == ["/actor/wf-1/a.txt", "/actor/wf-1/b.txt"]
    assert lst[0]["version"] == 1 and lst[0]["owner"] == "wf-1"
    assert "data" not in lst[0]

    assert len(await sqlite_storage.fs_list("/shared/")) == 1
    assert await sqlite_storage.fs_list("/actor/nope/") == []

    # LIKE metachars in prefix are matched literally
    await sqlite_storage.fs_put("/shared/100%/real", b"x", "wf-1", 0)
    lst = await sqlite_storage.fs_list("/shared/100%/")
    assert [o["path"] for o in lst] == ["/shared/100%/real"]


@pytest.fixture
def replay_engine(sqlite_storage):
    engine = ReplayEngine(storage=sqlite_storage, service_name="fs-test", worker_id="fs-worker")
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
async def test_fs_inside_real_activity(sqlite_storage, replay_engine):
    """FS calls are allowed in a real @activity body, including one that first
    executes in a resumed (replaying) run."""

    @activity(retry_policy=RetryPolicy(max_attempts=1))
    async def fs_bump(ctx: WorkflowContext, data: str) -> dict:
        path = f"/actor/{ctx.instance_id}/notes"
        _, version = await fs.fs_read(ctx, path)
        await fs.fs_write(ctx, path, data.encode(), version)
        return {"version": version + 1}

    @workflow
    async def fs_activity_wf(ctx: WorkflowContext) -> dict:
        first = await fs_bump(ctx, "v1")
        await sleep(ctx, 1)
        second = await fs_bump(ctx, "v2")
        return {"versions": [first["version"], second["version"]]}

    iid = await fs_activity_wf.start()
    assert (await sqlite_storage.get_instance(iid))["status"] == "waiting_for_timer"
    await _fire_timer_and_resume(sqlite_storage, replay_engine, iid, fs_activity_wf)

    inst = await sqlite_storage.get_instance(iid)
    assert inst["status"] == "completed"
    assert inst["output_data"]["result"] == {"versions": [1, 2]}
    assert (await sqlite_storage.fs_get(f"/actor/{iid}/notes"))["data"] == b"v2"


@pytest.mark.asyncio
async def test_fs_list_stays_inside_owner_boundary(sqlite_storage):
    """A prefix is normalized to the owner boundary: "/actor/sup-1" must not
    list "/actor/sup-1:a:g1/...", and "/shared" must not list "/sharedX"."""
    me = _in_activity(_ctx(sqlite_storage, "sup-1"))
    await sqlite_storage.fs_put("/actor/sup-1/own.txt", b"x", "sup-1", 0)
    await sqlite_storage.fs_put("/actor/sup-1:a:g1/child.txt", b"x", "sup-1:a:g1", 0)
    await sqlite_storage.fs_put("/shared/pub.txt", b"x", "sup-1", 0)
    await sqlite_storage.fs_put("/sharedX/leak.txt", b"x", "sup-1", 0)

    async def listed(prefix):
        return [o["path"] for o in await fs.fs_list(me, prefix)]

    assert await listed("/actor/sup-1") == ["/actor/sup-1/own.txt"]
    assert await listed("/actor/sup-1/") == ["/actor/sup-1/own.txt"]
    assert await listed("/actor/sup-1/o") == ["/actor/sup-1/own.txt"]
    assert await listed("/shared") == ["/shared/pub.txt"]
    assert await listed("/shared/") == ["/shared/pub.txt"]

    for prefix in ("", "/", "/actor", "/actor/", "/actor/sup", "/actor/sup-1:a:g1/", "/sharedX"):
        with pytest.raises(FSForbiddenError):
            await fs.fs_list(me, prefix)


@pytest.mark.asyncio
async def test_fs_put_conflict_keeps_enclosing_transaction_usable(sqlite_storage):
    """Losing a create race inside a caller transaction maps to
    FSConflictError without aborting that transaction."""
    storage = sqlite_storage
    if storage.engine.dialect.name == "sqlite":
        pytest.skip("needs two concurrent writers")
    inserted = asyncio.Event()
    release = asyncio.Event()

    async def winner():
        await storage.begin_transaction()
        await storage.fs_put("/shared/race", b"a", "wf-1", 0)
        inserted.set()
        await release.wait()
        await storage.commit_transaction()

    async def loser():
        await inserted.wait()
        await storage.begin_transaction()
        try:
            with pytest.raises(FSConflictError):
                await storage.fs_put("/shared/race", b"b", "wf-2", 0)
            await storage.fs_put("/shared/after", b"c", "wf-2", 0)
            await storage.commit_transaction()
        except BaseException:
            await storage.rollback_transaction()
            raise

    racers = [asyncio.create_task(winner()), asyncio.create_task(loser())]
    await asyncio.sleep(0.5)  # the loser's INSERT now waits on the winner's row
    release.set()
    await asyncio.gather(*racers)
    assert (await storage.fs_get("/shared/race"))["data"] == b"a"
    assert (await storage.fs_get("/shared/after"))["data"] == b"c"
