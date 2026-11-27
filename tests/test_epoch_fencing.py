"""Regression tests for epoch-based lock fencing.

A lock-holder identity is ``worker_id#<epoch>`` — the same worker process
acquiring the same instance in two epochs writes under different identities.
Once a new holder owns the lock, all writes made under the old identity must
be rejected.
"""

import asyncio
from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from edda.activity import activity
from edda.channels import receive, sleep, subscribe
from edda.context import WorkflowContext
from edda.locking import LockLostError, new_lock_identity
from edda.replay import ReplayEngine
from edda.retry import RetryPolicy
from edda.storage.sqlalchemy_storage import SQLAlchemyStorage
from edda.workflow import set_replay_engine, workflow

pytestmark = pytest.mark.multidb


@pytest.mark.asyncio
async def test_epoch_identity_unique():
    wid = "worker-1"
    assert new_lock_identity(wid) != new_lock_identity(wid)
    assert new_lock_identity(wid).startswith(f"{wid}#")


@pytest.mark.asyncio
async def test_stale_epoch_cannot_update_status(sqlite_storage, create_test_instance):
    iid = await create_test_instance("wf-fence", "wf", {})

    epoch_a = new_lock_identity("worker-A")
    epoch_b = new_lock_identity("worker-B")

    assert await sqlite_storage.try_acquire_lock(iid, epoch_a)
    # Take over: release A's lease and re-acquire under B's epoch
    await sqlite_storage.release_lock(iid, epoch_a)
    assert await sqlite_storage.try_acquire_lock(iid, epoch_b)

    with pytest.raises(LockLostError):
        await sqlite_storage.update_instance_status(iid, "failed", lock_id=epoch_a)
    # New holder can still write
    await sqlite_storage.update_instance_status(iid, "running", lock_id=epoch_b)


@pytest.mark.asyncio
async def test_stale_epoch_cannot_append_history(sqlite_storage, create_test_instance):
    iid = await create_test_instance("wf-fence-hist", "wf", {})

    epoch_a = new_lock_identity("worker-A")
    epoch_b = new_lock_identity("worker-A")  # same worker, new epoch

    assert await sqlite_storage.try_acquire_lock(iid, epoch_a)
    await sqlite_storage.release_lock(iid, epoch_a)
    assert await sqlite_storage.try_acquire_lock(iid, epoch_b)

    with pytest.raises(LockLostError):
        await sqlite_storage.append_history(
            iid, "a:1", "ActivityCompleted", {"r": 1}, lock_id=epoch_a
        )
    await sqlite_storage.append_history(iid, "a:1", "ActivityCompleted", {"r": 1}, lock_id=epoch_b)


@pytest.mark.asyncio
async def test_stale_epoch_cannot_update_activity(sqlite_storage, create_test_instance):
    iid = await create_test_instance("wf-fence-act", "wf", {})

    epoch_a = new_lock_identity("worker-A")
    await sqlite_storage.release_lock(iid, "plain-holder")
    assert await sqlite_storage.try_acquire_lock(iid, epoch_a)
    await sqlite_storage.release_lock(iid, epoch_a)

    with pytest.raises(LockLostError):
        await sqlite_storage.update_instance_activity(iid, "x:1", lock_id=epoch_a)


@pytest.mark.asyncio
async def test_reacquire_same_worker_different_epoch(sqlite_storage, create_test_instance):
    """Same worker losing then re-acquiring gets a different holder identity."""
    iid = await create_test_instance("wf-reacq", "wf", {})

    epoch_1 = new_lock_identity("worker-A")
    assert await sqlite_storage.try_acquire_lock(iid, epoch_1)
    await sqlite_storage.release_lock(iid, epoch_1)

    epoch_2 = new_lock_identity("worker-A")
    assert await sqlite_storage.try_acquire_lock(iid, epoch_2)
    # Old epoch fenced, new epoch writes
    with pytest.raises(LockLostError):
        await sqlite_storage.update_instance_status(iid, "failed", lock_id=epoch_1)
    await sqlite_storage.update_instance_status(iid, "running", lock_id=epoch_2)


@pytest.mark.asyncio
async def test_claim_resumable_atomic(sqlite_storage, create_test_instance):
    """claim_resumable_workflows claims and locks in one step."""
    iid = await create_test_instance("wf-claim", "wf", {})
    other = await create_test_instance("wf-claim2", "wf", {})
    # claim_resumable_workflows only picks up status='running'
    await sqlite_storage.update_instance_status(iid, "running")
    await sqlite_storage.update_instance_status(other, "running")

    epoch = new_lock_identity("worker-A")
    claimed = await sqlite_storage.claim_resumable_workflows(worker_id=epoch, limit=10)
    ids = {r["instance_id"] for r in claimed}
    assert iid in ids and other in ids

    # Rows are already locked under our epoch — a second claimer gets nothing
    claimed2 = await sqlite_storage.claim_resumable_workflows(
        worker_id=new_lock_identity("worker-B"), limit=10
    )
    assert claimed2 == []

    # Fenced write works under the claim identity; release frees it
    await sqlite_storage.update_instance_status(iid, "running", lock_id=epoch)
    await sqlite_storage.release_lock(iid, epoch)
    claimed3 = await sqlite_storage.claim_resumable_workflows(
        worker_id=new_lock_identity("worker-B"), limit=10
    )
    assert [r["instance_id"] for r in claimed3] == [iid]


@pytest.fixture
def replay_engine(sqlite_storage):
    engine = ReplayEngine(storage=sqlite_storage, service_name="fence-test", worker_id="fence-w")
    set_replay_engine(engine)
    yield engine
    set_replay_engine(None)


async def _expire_timer(storage, iid):
    """Record the pending timer as expired and make the instance resumable."""
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


async def _lease_seconds(storage, iid):
    async with storage.engine.begin() as conn:
        locked_at, expires = (
            await conn.execute(
                text(
                    "SELECT locked_at, lock_expires_at FROM workflow_instances "
                    "WHERE instance_id = :i"
                ),
                {"i": iid},
            )
        ).one()
    return (
        datetime.fromisoformat(str(expires)) - datetime.fromisoformat(str(locked_at))
    ).total_seconds()


@pytest.mark.asyncio
async def test_recur_continuation_can_suspend_on_receive(sqlite_storage, replay_engine):
    """The continuation's lock is held under its epoch identity; registering
    its first wait must use that identity."""

    @workflow
    async def recur_receiver(ctx: WorkflowContext, n: int = 0) -> dict:
        await subscribe(ctx, "inbox", mode="broadcast")
        if n == 0:
            await ctx.recur(n=1)
        msg = await receive(ctx, "inbox")
        return {"got": msg.data}

    continuation = await recur_receiver.start()
    inst = await sqlite_storage.get_instance(continuation)
    assert inst["status"] == "waiting_for_message"
    assert inst["locked_by"] is None


@pytest.mark.asyncio
async def test_recur_continuation_can_suspend_on_sleep(sqlite_storage, replay_engine):
    @workflow
    async def recur_sleeper(ctx: WorkflowContext, n: int = 0) -> dict:
        if n == 0:
            await ctx.recur(n=1)
        await sleep(ctx, 30)
        return {}

    continuation = await recur_sleeper.start()
    inst = await sqlite_storage.get_instance(continuation)
    assert inst["status"] == "waiting_for_timer"
    assert inst["locked_by"] is None


@pytest.mark.asyncio
async def test_lock_lost_activity_is_not_retried(sqlite_storage, replay_engine):
    """A lost lease aborts the run: the body is not re-executed by retries and
    the stale holder writes nothing into the new owner's history."""
    calls = {"n": 0}

    @activity(retry_policy=RetryPolicy(max_attempts=3, initial_interval=0.01))
    async def charge_card(ctx: WorkflowContext) -> dict:
        calls["n"] += 1
        # The lease expired and another worker took the instance over.
        async with sqlite_storage.engine.begin() as conn:
            await conn.execute(
                text("UPDATE workflow_instances SET locked_by = 'other#1' WHERE instance_id = :i"),
                {"i": ctx.instance_id},
            )
        return {"charged": True}

    @workflow
    async def pay_wf(ctx: WorkflowContext) -> dict:
        return await charge_card(ctx)

    # The run stops; start() reports the instance, which its new owner continues.
    await pay_wf.start()
    assert calls["n"] == 1
    async with sqlite_storage.engine.begin() as conn:
        status, holder = (
            await conn.execute(
                text(
                    "SELECT status, locked_by FROM workflow_instances WHERE workflow_name = 'pay_wf'"
                )
            )
        ).one()
        events = (await conn.execute(text("SELECT event_type FROM workflow_history"))).all()
    assert (status, holder) == ("running", "other#1")
    assert events == []


@pytest.mark.asyncio
async def test_claim_resumable_does_not_overwrite_a_concurrent_lock(
    sqlite_storage, create_test_instance, monkeypatch
):
    """A lock taken between the claim's candidate SELECT and its UPDATE (no
    row locks on SQLite) must not be overwritten: exactly one side wins."""
    storage = sqlite_storage
    iid = await create_test_instance("wf-claim-race", "wf", {})
    await storage.update_instance_status(iid, "running")
    claimer = new_lock_identity("worker-A")
    other = new_lock_identity("worker-B")
    other_task = {}
    orig_execute = AsyncSession.execute

    async def execute(self, statement, *args, **kwargs):
        result = await orig_execute(self, statement, *args, **kwargs)
        if getattr(statement, "_for_update_arg", None) is not None and not other_task:
            other_task["t"] = asyncio.create_task(storage.try_acquire_lock(iid, other))
            # PostgreSQL/MySQL: blocked by FOR UPDATE until the claim commits
            await asyncio.wait({other_task["t"]}, timeout=0.5)
        return result

    monkeypatch.setattr(AsyncSession, "execute", execute)
    claimed = await storage.claim_resumable_workflows(worker_id=claimer, limit=10)
    other_got = await other_task["t"]
    monkeypatch.undo()

    claimed_it = iid in [r["instance_id"] for r in claimed]
    assert claimed_it != other_got
    assert (await storage.get_instance(iid))["locked_by"] == (claimer if claimed_it else other)


@pytest.mark.asyncio
async def test_claim_resumable_honours_lock_timeout(sqlite_storage):
    await sqlite_storage.create_instance(
        instance_id="wf-lease",
        workflow_name="test_workflow",
        source_hash="abc123def456",
        owner_service="t",
        input_data={},
        lock_timeout_seconds=3600,
    )
    await sqlite_storage.update_instance_status("wf-lease", "running")
    claimed = await sqlite_storage.claim_resumable_workflows(new_lock_identity("w"), limit=10)
    assert [r["instance_id"] for r in claimed] == ["wf-lease"]
    assert await _lease_seconds(sqlite_storage, "wf-lease") == 3600


@pytest.mark.asyncio
async def test_resume_of_lost_claim_does_not_run(sqlite_storage, replay_engine):
    """A batch-claimed instance whose claim lapsed before its turn is not
    replayed by the stale claimant (no activity side effects)."""
    runs = {"n": 0}

    @activity
    async def side_effect(ctx: WorkflowContext) -> dict:
        runs["n"] += 1
        return {}

    @workflow
    async def claimed_wf(ctx: WorkflowContext) -> dict:
        await sleep(ctx, 1)
        await side_effect(ctx)
        return {}

    iid = await claimed_wf.start()
    await _expire_timer(sqlite_storage, iid)
    stale = new_lock_identity("worker-A")
    assert [r["instance_id"] for r in await sqlite_storage.claim_resumable_workflows(stale)] == [
        iid
    ]
    # The claim lapsed and worker B took the instance over.
    await sqlite_storage.release_lock(iid, stale)
    assert await sqlite_storage.try_acquire_lock(iid, new_lock_identity("worker-B"))

    await replay_engine.resume_workflow(iid, claimed_wf.func, already_locked=True, lock_id=stale)
    assert runs["n"] == 0


@pytest.mark.asyncio
async def test_lock_heartbeat_cancels_only_the_run(sqlite_storage, create_test_instance):
    from edda.locking import run_with_lock_heartbeat

    iid = await create_test_instance("wf-heartbeat", "wf", {})
    epoch = new_lock_identity("worker-A")
    assert await sqlite_storage.try_acquire_lock(iid, epoch)
    started = asyncio.Event()

    async def long_run():
        started.set()
        await asyncio.sleep(30)

    async def take_over():
        await started.wait()
        await sqlite_storage.release_lock(iid, epoch)
        assert await sqlite_storage.try_acquire_lock(iid, new_lock_identity("worker-B"))

    thief = asyncio.create_task(take_over())
    with pytest.raises(LockLostError):
        await asyncio.wait_for(
            run_with_lock_heartbeat(sqlite_storage, iid, epoch, long_run, refresh_interval=0.05),
            timeout=5,
        )
    await thief
    assert asyncio.current_task().cancelling() == 0


@pytest.mark.asyncio
async def test_stale_holder_cannot_consume_competing_message(sqlite_storage, create_test_instance):
    iid = await create_test_instance("wf-stale-recv", "wf", {})
    await sqlite_storage.update_instance_status(iid, "running")
    await sqlite_storage.subscribe_to_channel(iid, "jobs", "competing")
    msg_id = await sqlite_storage.publish_to_channel("jobs", {"n": 1})

    stale = new_lock_identity("worker-A")
    assert await sqlite_storage.try_acquire_lock(iid, stale)
    await sqlite_storage.release_lock(iid, stale)
    assert await sqlite_storage.try_acquire_lock(iid, new_lock_identity("worker-B"))

    ctx = WorkflowContext(
        instance_id=iid,
        workflow_name="wf",
        storage=sqlite_storage,
        worker_id="worker-A",
        lock_id=stale,
    )
    with pytest.raises(LockLostError):
        await receive(ctx, "jobs")
    # Neither recorded nor consumed: the new owner still receives it.
    assert await sqlite_storage.get_history(iid) == []
    pending = await sqlite_storage.get_pending_channel_messages(iid, "jobs")
    assert [m["message_id"] for m in pending] == [msg_id]


@pytest.mark.asyncio
async def test_wait_registration_does_not_release_a_taken_over_lock(
    sqlite_storage, create_test_instance, monkeypatch
):
    iid = await create_test_instance("wf-reg-fence", "wf", {})
    await sqlite_storage.update_instance_status(iid, "running")
    await sqlite_storage.subscribe_to_channel(iid, "inbox", "broadcast")
    stale = new_lock_identity("worker-A")
    new_owner = new_lock_identity("worker-B")
    assert await sqlite_storage.try_acquire_lock(iid, stale)
    orig_check = SQLAlchemyStorage._has_pending_channel_message

    async def takeover_then_check(self, session, instance_id, channel, subscription):
        # The lease expired and worker B claimed the instance in between.
        async with self.engine.begin() as conn:
            await conn.execute(
                text("UPDATE workflow_instances SET locked_by = :w WHERE instance_id = :i"),
                {"w": new_owner, "i": instance_id},
            )
        return await orig_check(self, session, instance_id, channel, subscription)

    monkeypatch.setattr(SQLAlchemyStorage, "_has_pending_channel_message", takeover_then_check)
    with pytest.raises(RuntimeError):
        await sqlite_storage.register_channel_receive_and_release_lock(
            iid, stale, "inbox", activity_id="receive_inbox:1"
        )
    monkeypatch.undo()
    inst = await sqlite_storage.get_instance(iid)
    assert inst["locked_by"] == new_owner
    assert inst["status"] == "running"


@pytest.mark.asyncio
async def test_requeue_clears_the_wait_registration(sqlite_storage, create_test_instance):
    """Requeue ends the wait: no stale registration may keep reporting a
    timeout (or a pending channel) for the running instance."""
    iid = await create_test_instance("wf-requeue", "wf", {})
    await sqlite_storage.update_instance_status(iid, "running")
    await sqlite_storage.subscribe_to_channel(iid, "inbox", "broadcast")
    epoch = new_lock_identity("worker-A")
    assert await sqlite_storage.try_acquire_lock(iid, epoch)
    assert await sqlite_storage.register_channel_receive_and_release_lock(
        iid, epoch, "inbox", activity_id="receive_inbox:1", timeout_seconds=1
    )
    await sqlite_storage.publish_to_channel("inbox", {"n": 1})  # missed wake-up

    assert await sqlite_storage.requeue_channel_waits_with_pending() == 1
    assert (await sqlite_storage.get_instance(iid))["status"] == "running"
    assert (await sqlite_storage.get_channel_subscription(iid, "inbox"))["activity_id"] is None
    await asyncio.sleep(2)
    expired = await sqlite_storage.find_expired_message_subscriptions()
    assert iid not in [e["instance_id"] for e in expired]


def _db_ts(storage, dt):
    """Bind a UTC datetime for a TIMESTAMP column in raw SQL."""
    if storage.engine.dialect.name == "sqlite":
        return dt.astimezone(UTC).strftime("%Y-%m-%d %H:%M:%S")
    return dt.astimezone(UTC).replace(tzinfo=None)


async def _complete_under_lock(storage, iid):
    """Hold the lock under a fresh epoch and complete the instance with it
    (the engine writes the output first, then the status)."""
    epoch = new_lock_identity("worker-A")
    assert await storage.try_acquire_lock(iid, epoch)
    await storage.update_instance_status(iid, "completed", {"result": 1}, lock_id=epoch)
    return epoch


@pytest.mark.asyncio
async def test_fenced_status_write_cannot_revive_a_terminated_instance(
    sqlite_storage, create_test_instance
):
    iid = await create_test_instance("wf-absorbing", "wf", {})
    epoch = await _complete_under_lock(sqlite_storage, iid)

    # Rewriting the same terminal status stays a no-op success.
    await sqlite_storage.update_instance_status(iid, "completed", lock_id=epoch)
    with pytest.raises(LockLostError) as refused:
        await sqlite_storage.update_instance_status(iid, "running", lock_id=epoch)
    from edda.locking import InstanceTerminatedError

    assert isinstance(refused.value, InstanceTerminatedError)
    inst = await sqlite_storage.get_instance(iid)
    assert (inst["status"], inst["locked_by"]) == ("completed", epoch)


@pytest.mark.asyncio
async def test_timer_suspend_of_a_terminated_instance_drops_the_timer(
    sqlite_storage, create_test_instance
):
    iid = await create_test_instance("wf-absorbing-timer", "wf", {})
    epoch = await _complete_under_lock(sqlite_storage, iid)

    with pytest.raises(LockLostError) as refused:
        await sqlite_storage.register_timer_subscription_and_release_lock(
            iid, epoch, "t1", datetime.now(UTC) + timedelta(seconds=30), activity_id="sleep:1"
        )
    from edda.locking import InstanceTerminatedError

    assert isinstance(refused.value, InstanceTerminatedError)
    assert (await sqlite_storage.get_instance(iid))["status"] == "completed"
    async with sqlite_storage.engine.begin() as conn:
        timers = (
            await conn.execute(
                text("SELECT timer_id FROM workflow_timer_subscriptions WHERE instance_id = :i"),
                {"i": iid},
            )
        ).all()
    assert timers == []


@pytest.mark.asyncio
async def test_wait_registration_of_a_terminated_instance_is_refused(
    sqlite_storage, create_test_instance
):
    iid = await create_test_instance("wf-absorbing-wait", "wf", {})
    await sqlite_storage.subscribe_to_channel(iid, "inbox", "broadcast")
    epoch = await _complete_under_lock(sqlite_storage, iid)

    with pytest.raises(LockLostError) as refused:
        await sqlite_storage.register_channel_receive_and_release_lock(
            iid, epoch, "inbox", activity_id="receive_inbox:1", timeout_seconds=30
        )
    from edda.locking import InstanceTerminatedError

    assert isinstance(refused.value, InstanceTerminatedError)
    assert (await sqlite_storage.get_instance(iid))["status"] == "completed"
    sub = await sqlite_storage.get_channel_subscription(iid, "inbox")
    assert (sub["activity_id"], sub["timeout_at"]) == (None, None)


@pytest.mark.asyncio
async def test_cancel_drops_timers_and_channel_waits(sqlite_storage, create_test_instance):
    """Whatever the instance was doing, a cancel removes its timers and ends
    its channel waits, so nothing fires into the cancelled instance later."""
    iid = await create_test_instance("wf-cancel-cleanup", "wf", {})
    await sqlite_storage.subscribe_to_channel(iid, "inbox", "broadcast")
    epoch = new_lock_identity("worker-A")
    assert await sqlite_storage.try_acquire_lock(iid, epoch)
    await sqlite_storage.register_timer_subscription_and_release_lock(
        iid, epoch, "t1", datetime.now(UTC) + timedelta(seconds=30), activity_id="sleep:1"
    )
    # Left behind by a run that moved on: the instance runs again while the
    # timer row and a wait registration still exist.
    async with sqlite_storage.engine.begin() as conn:
        await conn.execute(
            text("UPDATE workflow_instances SET status = 'running' WHERE instance_id = :i"),
            {"i": iid},
        )
        await conn.execute(
            text(
                "UPDATE channel_subscriptions SET activity_id = 'receive_inbox:1', "
                "timeout_at = :t WHERE instance_id = :i"
            ),
            {"i": iid, "t": _db_ts(sqlite_storage, datetime.now(UTC) + timedelta(seconds=30))},
        )

    assert await sqlite_storage.cancel_instance(iid, "test")

    async with sqlite_storage.engine.begin() as conn:
        timers = (
            await conn.execute(
                text("SELECT timer_id FROM workflow_timer_subscriptions WHERE instance_id = :i"),
                {"i": iid},
            )
        ).all()
    assert timers == []
    sub = await sqlite_storage.get_channel_subscription(iid, "inbox")
    assert (sub["activity_id"], sub["timeout_at"]) == (None, None)


@pytest.mark.asyncio
async def test_resume_of_a_cancelled_instance_is_a_noop(sqlite_storage, replay_engine):
    runs = {"n": 0}

    @workflow
    async def cancel_then_resume_wf(ctx: WorkflowContext) -> dict:
        runs["n"] += 1
        await sleep(ctx, 30)
        return {}

    iid = await cancel_then_resume_wf.start()
    assert await sqlite_storage.cancel_instance(iid, "test")
    runs["n"] = 0

    await replay_engine.resume_by_name(iid, "cancel_then_resume_wf")

    assert runs["n"] == 0
    inst = await sqlite_storage.get_instance(iid)
    assert (inst["status"], inst["locked_by"]) == ("cancelled", None)


# --- Timer and receive-timeout handlers (they consume a wake-up under the lock)


def _handler_app(storage, engine):
    """An EddaApp whose wake-up handlers run against the test storage."""
    from edda.app import EddaApp

    app = EddaApp(service_name="fence-test", db_url="sqlite+aiosqlite:///:memory:")
    app.storage = storage
    app.replay_engine = engine
    return app


async def _make_due(storage, table, column, iid):
    async with storage.engine.begin() as conn:
        await conn.execute(
            text(f"UPDATE {table} SET {column} = :t WHERE instance_id = :i"),
            {"i": iid, "t": _db_ts(storage, datetime.now(UTC) - timedelta(seconds=5))},
        )


async def _timer_ids(storage, iid):
    async with storage.engine.begin() as conn:
        rows = await conn.execute(
            text("SELECT timer_id FROM workflow_timer_subscriptions WHERE instance_id = :i"),
            {"i": iid},
        )
        return [r[0] for r in rows]


@pytest.mark.asyncio
async def test_timer_is_consumed_together_with_the_status_flip(
    sqlite_storage, replay_engine, monkeypatch
):
    """Removing the timer, recording its expiry and making the instance
    runnable happen together: a resume that then fails leaves the instance
    to the resumable sweep instead of waiting for a timer that is gone."""

    @workflow
    async def timer_consume_wf(ctx: WorkflowContext) -> dict:
        await sleep(ctx, 30)
        return {}

    iid = await timer_consume_wf.start()
    await _make_due(sqlite_storage, "workflow_timer_subscriptions", "expires_at", iid)

    async def refused_resume(*args, **kwargs):
        raise RuntimeError("resume refused")

    monkeypatch.setattr(replay_engine, "resume_by_name", refused_resume)
    await _handler_app(sqlite_storage, replay_engine)._check_expired_timers()

    inst = await sqlite_storage.get_instance(iid)
    assert (inst["status"], inst["locked_by"]) == ("running", None)
    assert [h["event_type"] for h in await sqlite_storage.get_history(iid)] == ["TimerExpired"]
    assert await _timer_ids(sqlite_storage, iid) == []


@pytest.mark.asyncio
async def test_timer_of_a_terminated_instance_is_dropped(
    sqlite_storage, replay_engine, monkeypatch
):
    runs = {"n": 0}

    @workflow
    async def timer_after_cancel_wf(ctx: WorkflowContext) -> dict:
        runs["n"] += 1
        await sleep(ctx, 30)
        return {}

    iid = await timer_after_cancel_wf.start()
    await _make_due(sqlite_storage, "workflow_timer_subscriptions", "expires_at", iid)
    due = await sqlite_storage.find_expired_timers()
    assert [t["instance_id"] for t in due] == [iid]
    # Cancelled after the scan, by a path that left the timer behind.
    async with sqlite_storage.engine.begin() as conn:
        await conn.execute(
            text("UPDATE workflow_instances SET status = 'cancelled' WHERE instance_id = :i"),
            {"i": iid},
        )

    async def scan():
        return due

    monkeypatch.setattr(sqlite_storage, "find_expired_timers", scan)
    runs["n"] = 0
    await _handler_app(sqlite_storage, replay_engine)._check_expired_timers()
    monkeypatch.undo()

    assert runs["n"] == 0
    inst = await sqlite_storage.get_instance(iid)
    assert (inst["status"], inst["locked_by"]) == ("cancelled", None)
    assert await sqlite_storage.get_history(iid) == []
    assert await _timer_ids(sqlite_storage, iid) == []


@workflow
async def timeout_recheck_wf(ctx: WorkflowContext) -> dict:
    await subscribe(ctx, "inbox", mode="broadcast")
    msg = await receive(ctx, "inbox", timeout_seconds=30)
    return {"got": msg.data}


async def _due_receive(storage):
    iid = await timeout_recheck_wf.start()
    await _make_due(storage, "channel_subscriptions", "timeout_at", iid)
    due = await storage.find_expired_message_subscriptions()
    assert [s["instance_id"] for s in due] == [iid]
    return iid, due


@pytest.mark.asyncio
async def test_receive_timeout_fires_only_into_the_scanned_wait(
    sqlite_storage, replay_engine, monkeypatch
):
    iid, due = await _due_receive(sqlite_storage)
    # After the scan the wait was registered again with a later deadline.
    async with sqlite_storage.engine.begin() as conn:
        await conn.execute(
            text("UPDATE channel_subscriptions SET timeout_at = :t WHERE instance_id = :i"),
            {"i": iid, "t": _db_ts(sqlite_storage, datetime.now(UTC) + timedelta(seconds=60))},
        )

    async def scan():
        return due

    monkeypatch.setattr(sqlite_storage, "find_expired_message_subscriptions", scan)
    await _handler_app(sqlite_storage, replay_engine)._check_expired_message_subscriptions()
    monkeypatch.undo()

    assert await sqlite_storage.get_history(iid) == []
    inst = await sqlite_storage.get_instance(iid)
    assert (inst["status"], inst["locked_by"]) == ("waiting_for_message", None)
    sub = await sqlite_storage.get_channel_subscription(iid, "inbox")
    assert sub["activity_id"] == due[0]["activity_id"]


@pytest.mark.asyncio
@pytest.mark.parametrize("moved_on_to", ["running", "cancelled"])
async def test_receive_timeout_of_an_instance_that_moved_on_only_clears_the_wait(
    sqlite_storage, replay_engine, monkeypatch, moved_on_to
):
    """A stale deadline must not record a timeout into (or resume) an instance
    that no longer waits on it -- requeued and running, or cancelled."""
    iid, due = await _due_receive(sqlite_storage)
    async with sqlite_storage.engine.begin() as conn:
        await conn.execute(
            text("UPDATE workflow_instances SET status = :s WHERE instance_id = :i"),
            {"i": iid, "s": moved_on_to},
        )

    async def scan():
        return due

    monkeypatch.setattr(sqlite_storage, "find_expired_message_subscriptions", scan)
    await _handler_app(sqlite_storage, replay_engine)._check_expired_message_subscriptions()
    monkeypatch.undo()

    assert await sqlite_storage.get_history(iid) == []
    inst = await sqlite_storage.get_instance(iid)
    assert (inst["status"], inst["locked_by"]) == (moved_on_to, None)
    sub = await sqlite_storage.get_channel_subscription(iid, "inbox")
    assert (sub["activity_id"], sub["timeout_at"]) == (None, None)


@workflow
async def sleep_then_done_wf(ctx: WorkflowContext) -> dict:
    await sleep(ctx, 30)
    return {"slept": True}


@workflow
async def timeout_then_done_wf(ctx: WorkflowContext) -> dict:
    await subscribe(ctx, "inbox", mode="broadcast")
    try:
        await receive(ctx, "inbox", timeout_seconds=30)
    except TimeoutError:
        return {"timed_out": True}
    return {"timed_out": False}


@pytest.mark.asyncio
async def test_due_timer_resumes_the_sleeping_workflow(sqlite_storage, replay_engine):
    """The handler's re-check under the lock still fires a due timer."""
    iid = await sleep_then_done_wf.start()
    await _make_due(sqlite_storage, "workflow_timer_subscriptions", "expires_at", iid)

    await _handler_app(sqlite_storage, replay_engine)._check_expired_timers()

    inst = await sqlite_storage.get_instance(iid)
    assert (inst["status"], inst["locked_by"]) == ("completed", None)
    assert inst["output_data"]["result"] == {"slept": True}
    assert await _timer_ids(sqlite_storage, iid) == []


@pytest.mark.asyncio
async def test_receive_timeout_fires_into_the_waiting_receive(sqlite_storage, replay_engine):
    """The handler's re-check under the lock (same wait, same deadline, still
    waiting) holds for an untouched wait, so a due receive times out."""
    iid = await timeout_then_done_wf.start()
    await _make_due(sqlite_storage, "channel_subscriptions", "timeout_at", iid)

    await _handler_app(sqlite_storage, replay_engine)._check_expired_message_subscriptions()

    inst = await sqlite_storage.get_instance(iid)
    assert (inst["status"], inst["locked_by"]) == ("completed", None)
    assert inst["output_data"]["result"] == {"timed_out": True}
    events = [h["event_type"] for h in await sqlite_storage.get_history(iid)]
    assert "MessageTimeout" in events


# --- A run that lost its lease leaves nothing behind


@pytest.mark.asyncio
@pytest.mark.parametrize("mode", ["competing", "broadcast"])
async def test_receive_under_a_lost_lease_consumes_nothing(
    sqlite_storage, create_test_instance, mode
):
    """The claim (or cursor advance), the record and the delete commit
    together after a lock proof: a stale run neither records the message nor
    leaves a claim that hides it, or a cursor that skips it."""
    iid = await create_test_instance("wf-lost-recv", "wf", {})
    other = await create_test_instance("wf-lost-recv-other", "wf", {})
    for instance in (iid, other):
        await sqlite_storage.update_instance_status(instance, "running")
        await sqlite_storage.subscribe_to_channel(instance, "jobs", mode)
    msg_id = await sqlite_storage.publish_to_channel("jobs", {"n": 1})
    stale = new_lock_identity("worker-A")
    assert await sqlite_storage.try_acquire_lock(iid, stale)
    await sqlite_storage.release_lock(iid, stale)
    assert await sqlite_storage.try_acquire_lock(iid, new_lock_identity("worker-B"))

    ctx = WorkflowContext(
        instance_id=iid,
        workflow_name="wf",
        storage=sqlite_storage,
        worker_id="worker-A",
        lock_id=stale,
    )
    with pytest.raises(LockLostError):
        await receive(ctx, "jobs")

    assert await sqlite_storage.get_history(iid) == []
    pending = await sqlite_storage.get_pending_channel_messages(iid, "jobs")
    assert [m["message_id"] for m in pending] == [msg_id]
    if mode == "competing":
        assert await sqlite_storage.claim_channel_message(msg_id, other)


@pytest.mark.asyncio
async def test_fenced_append_orders_a_lease_steal(sqlite_storage, create_test_instance):
    """A steal of an expired lease waits for an open fenced append: the stale
    record lands before the new holder takes over (and loads history)."""
    if sqlite_storage.engine.dialect.name == "sqlite":
        pytest.skip("one shared in-memory connection; SQLite serializes writers anyway")
    iid = await create_test_instance("wf-steal-order", "wf", {})
    holder = new_lock_identity("worker-A")
    assert await sqlite_storage.try_acquire_lock(iid, holder)
    async with sqlite_storage.engine.begin() as conn:
        await conn.execute(
            text("UPDATE workflow_instances SET lock_expires_at = :t WHERE instance_id = :i"),
            {"i": iid, "t": _db_ts(sqlite_storage, datetime.now(UTC) - timedelta(seconds=5))},
        )

    await sqlite_storage.begin_transaction()
    try:
        await sqlite_storage.append_history(
            iid, "step:1", "ActivityCompleted", {"result": 1}, lock_id=holder
        )
        steal = asyncio.ensure_future(
            sqlite_storage.try_acquire_lock(iid, new_lock_identity("worker-B"))
        )
        await asyncio.sleep(0.5)
        assert not steal.done(), "the steal must wait for the fenced append"
        await sqlite_storage.commit_transaction()
    except BaseException:
        if sqlite_storage.in_transaction():
            await sqlite_storage.rollback_transaction()
        raise
    assert await asyncio.wait_for(steal, timeout=10)
    assert [h["activity_id"] for h in await sqlite_storage.get_history(iid)] == ["step:1"]


@pytest.mark.asyncio
async def test_lost_lease_leaves_no_effects(sqlite_storage, create_test_instance):
    """An effect and its step record commit together after a lock proof: a
    run that lost its lease applies none of its effects (the next run would
    apply them again)."""
    from edda import actors, tasks
    from edda.channels import publish, send_to

    iid = await create_test_instance("wf-lost-effects", "wf", {})
    target = await create_test_instance("wf-lost-effects-target", "wf", {})
    await sqlite_storage.post_task("task-held", "lost-jobs", "{}")
    assert (await sqlite_storage.claim_task("lost-jobs", iid))["id"] == "task-held"
    await sqlite_storage.post_task("task-open", "lost-open", "{}")
    stale = new_lock_identity("worker-A")
    assert await sqlite_storage.try_acquire_lock(iid, stale)
    await sqlite_storage.release_lock(iid, stale)
    assert await sqlite_storage.try_acquire_lock(iid, new_lock_identity("worker-B"))

    ctx = WorkflowContext(
        instance_id=iid,
        workflow_name="wf",
        storage=sqlite_storage,
        worker_id="worker-A",
        lock_id=stale,
    )
    effects = [
        actors.register_name(ctx, "lost-name"),
        actors.monitor(ctx, target, "lost-mon"),
        actors.issue_capability(ctx, "channel_pub", "lost-cap"),
        tasks.post_task(ctx, "lost-posts", {"n": 1}),
        tasks.claim_task(ctx, "lost-open"),
        tasks.complete_task(ctx, "task-held", {"ok": True}),
        publish(ctx, "lost-pub", {"n": 1}),
        send_to(ctx, target, {"n": 1}, channel="lost-direct"),
    ]
    for effect in effects:
        with pytest.raises(LockLostError):
            await effect

    assert await sqlite_storage.whereis_actor("lost-name") is None
    assert not await sqlite_storage.has_capability_grants("channel_pub", "lost-cap")
    assert await sqlite_storage.list_tasks("lost-posts", None) == []
    assert (await sqlite_storage.get_task("task-open"))["state"] == "open"
    assert (await sqlite_storage.get_task("task-held"))["state"] == "claimed"
    assert await sqlite_storage.channel_messages_since("lost-pub", None) == []
    assert await sqlite_storage.channel_messages_since(f"lost-direct:{target}", None) == []
    await sqlite_storage.update_instance_status(target, "completed")
    assert await sqlite_storage.emit_actor_down_for_terminated() == 0  # no monitor row
    assert await sqlite_storage.get_history(iid) == []


@pytest.mark.asyncio
@pytest.mark.parametrize("write", ["status", "activity", "history"])
async def test_refused_fenced_write_keeps_the_callers_transaction(
    sqlite_storage, create_test_instance, write
):
    """A fenced write refused inside a caller's transaction raises and
    leaves that transaction alone (it changed nothing): the caller decides,
    here by rolling back its savepoint only and committing the rest."""
    iid = await create_test_instance(f"wf-keep-{write}", "wf", {})
    other = await create_test_instance(f"wf-keep-other-{write}", "wf", {})
    stale = "nobody#1"
    refused = {
        "status": lambda: sqlite_storage.update_instance_status(iid, "failed", lock_id=stale),
        "activity": lambda: sqlite_storage.update_instance_activity(iid, "a:1", lock_id=stale),
        "history": lambda: sqlite_storage.append_history(
            iid,
            activity_id="a:1",
            event_type="ActivityCompleted",
            event_data={"result": 1},
            lock_id=stale,
        ),
    }[write]
    await sqlite_storage.begin_transaction()
    try:
        await sqlite_storage.append_history(
            other, activity_id="outer:1", event_type="ActivityCompleted", event_data={"result": 1}
        )
        await sqlite_storage.begin_transaction()  # savepoint
        with pytest.raises(LockLostError):
            await refused()
        await sqlite_storage.rollback_transaction()  # back to the savepoint only
        await sqlite_storage.commit_transaction()
    except BaseException:
        if sqlite_storage.in_transaction():
            await sqlite_storage.rollback_transaction()
        raise
    assert [h["activity_id"] for h in await sqlite_storage.get_history(other)] == ["outer:1"]
    assert await sqlite_storage.get_history(iid) == []


@pytest.mark.asyncio
async def test_workflow_lock_ends_only_its_block_on_lease_loss(
    sqlite_storage, create_test_instance
):
    """On lease loss the heartbeat interrupts the block it guards. The block
    ends with LockLostError, and the task that ran it -- possibly a caller's,
    such as a parent's activity starting a child inline -- is not left with a
    cancellation request."""
    from edda.locking import workflow_lock

    iid = await create_test_instance("wf-lock-block", "wf", {})
    epoch = new_lock_identity("worker-A")
    started = asyncio.Event()

    async def take_over():
        await started.wait()
        await sqlite_storage.release_lock(iid, epoch)
        assert await sqlite_storage.try_acquire_lock(iid, new_lock_identity("worker-B"))

    thief = asyncio.create_task(take_over())
    with pytest.raises(LockLostError):
        async with workflow_lock(sqlite_storage, iid, epoch, refresh_interval=0.05):
            started.set()
            await asyncio.sleep(30)
    await thief
    assert asyncio.current_task().cancelling() == 0
    await asyncio.sleep(0)  # and it is not cancelled at its next await


@pytest.mark.asyncio
async def test_workflow_lock_passes_a_cancellation_of_its_caller(
    sqlite_storage, create_test_instance
):
    from edda.locking import workflow_lock

    iid = await create_test_instance("wf-lock-cancel", "wf", {})
    epoch = new_lock_identity("worker-A")
    started = asyncio.Event()

    async def hold():
        async with workflow_lock(sqlite_storage, iid, epoch, refresh_interval=0.05):
            started.set()
            await asyncio.sleep(30)

    task = asyncio.create_task(hold())
    await started.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert (await sqlite_storage.get_instance(iid))["locked_by"] is None


@pytest.mark.asyncio
async def test_start_returns_when_the_first_run_loses_its_lease(
    sqlite_storage, replay_engine, monkeypatch
):
    """A lost lease ends the first run, not the start(): the instance exists
    and its new owner continues it, so start() returns its id -- inside a
    parent's activity, for example, the activity goes on instead of failing
    and being retried into a second start."""
    import edda.replay as replay_module

    real_lock = replay_module.workflow_lock

    def fast_heartbeat(storage, instance_id, lock_id, timeout_seconds=300, refresh_interval=None):
        return real_lock(storage, instance_id, lock_id, timeout_seconds, refresh_interval=0.05)

    monkeypatch.setattr(replay_module, "workflow_lock", fast_heartbeat)
    seen: dict[str, str] = {}
    started = asyncio.Event()

    @workflow
    async def lease_lost_first_run(ctx: WorkflowContext) -> dict:
        seen["id"] = ctx.instance_id
        started.set()
        await asyncio.sleep(30)
        return {}

    async def take_over():
        await started.wait()
        holder = (await sqlite_storage.get_instance(seen["id"]))["locked_by"]
        await sqlite_storage.release_lock(seen["id"], holder)
        assert await sqlite_storage.try_acquire_lock(seen["id"], new_lock_identity("worker-B"))

    thief = asyncio.create_task(take_over())
    iid = await lease_lost_first_run.start()
    await thief
    assert iid == seen["id"]
    assert asyncio.current_task().cancelling() == 0
    inst = await sqlite_storage.get_instance(iid)
    assert inst["status"] == "running"
    assert inst["locked_by"].startswith("worker-B#")
