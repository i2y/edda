"""Tests for capability primitives (grant/check/revoke/issue)."""

import hashlib
import os
import time
from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy import text

from edda import actors
from edda.channels import sleep
from edda.context import WorkflowContext
from edda.replay import ReplayEngine
from edda.workflow import set_replay_engine, workflow

pytestmark = pytest.mark.multidb


def _hash(s: str) -> str:
    return hashlib.sha256(s.encode()).hexdigest()


@pytest.mark.asyncio
async def test_capability_roundtrip(sqlite_storage):
    await sqlite_storage.grant_capability("cap-1", "channel_pub", "orders", _hash("sekret"))
    assert await sqlite_storage.check_capability("channel_pub", "orders", _hash("sekret"))
    assert not await sqlite_storage.check_capability("channel_pub", "orders", _hash("wrong"))
    assert not await sqlite_storage.check_capability("channel_sub", "orders", _hash("sekret"))
    assert not await sqlite_storage.check_capability("channel_pub", "other", _hash("sekret"))

    await sqlite_storage.revoke_capability("cap-1")
    assert not await sqlite_storage.check_capability("channel_pub", "orders", _hash("sekret"))
    await sqlite_storage.revoke_capability("cap-1")  # idempotent


@pytest.mark.asyncio
async def test_capability_expiry(sqlite_storage):
    past = (datetime.now(UTC) - timedelta(hours=1)).isoformat()
    await sqlite_storage.grant_capability(
        "cap-x", "channel_pub", "orders", _hash("s2"), expires_at=past
    )
    assert not await sqlite_storage.check_capability("channel_pub", "orders", _hash("s2"))


@pytest.mark.asyncio
async def test_issue_and_check_token(sqlite_storage):
    from edda.context import WorkflowContext

    await sqlite_storage.upsert_workflow_definition(
        workflow_name="wf",
        source_hash="wf-hash",
        source_code="async def wf(ctx): pass",
    )
    await sqlite_storage.create_instance(
        instance_id="cap-wf-1",
        workflow_name="wf",
        source_hash="wf-hash",
        owner_service="test",
        input_data={},
    )
    ctx = WorkflowContext(
        instance_id="cap-wf-1",
        workflow_name="wf",
        storage=sqlite_storage,
        worker_id="w1",
    )
    workflow_context = ctx
    token, cap_id = await actors.issue_capability(workflow_context, "channel_pub", "orders")
    assert cap_id.startswith("cap-")
    kind, target, secret = token.split(":")[1], token.split(":")[2], token.split(":")[3]
    assert kind == "channel_pub" and target == "orders"
    assert await actors.check_capability(workflow_context, "channel_pub", "orders", token)
    # raw secret form also accepted
    assert await actors.check_capability(workflow_context, "channel_pub", "orders", secret)
    assert not await actors.check_capability(workflow_context, "channel_pub", "orders", "bogus")


@pytest.mark.asyncio
async def test_issue_rejects_unknown_kind(sqlite_storage, create_test_instance):
    from edda.context import WorkflowContext

    # The refusal is a recorded step outcome, so the instance must exist
    # (PostgreSQL and MySQL enforce the history foreign key).
    await create_test_instance("cap-wf-2", "wf", {})
    workflow_context = WorkflowContext(
        instance_id="cap-wf-2",
        workflow_name="wf",
        storage=sqlite_storage,
        worker_id="w1",
    )
    with pytest.raises(ValueError, match="unknown capability kind"):
        await actors.issue_capability(workflow_context, "root", "everything")


@pytest.fixture
def replay_engine(sqlite_storage):
    engine = ReplayEngine(storage=sqlite_storage, service_name="cap-test", worker_id="cap-worker")
    set_replay_engine(engine)
    yield engine
    set_replay_engine(None)


@pytest.fixture
def non_utc_host():
    """Run with a non-UTC local timezone, so naive UTC values that get
    mistaken for local time shift by nine hours."""
    old = os.environ.get("TZ")
    os.environ["TZ"] = "Asia/Tokyo"
    time.tzset()
    yield
    if old is None:
        os.environ.pop("TZ", None)
    else:
        os.environ["TZ"] = old
    time.tzset()


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
async def test_capability_expiry_is_utc_on_non_utc_host(sqlite_storage, non_utc_host):
    soon = (datetime.now(UTC) + timedelta(minutes=30)).isoformat()
    await sqlite_storage.grant_capability(
        "cap-soon", "channel_pub", "orders", _hash("s3"), expires_at=soon
    )
    assert await sqlite_storage.check_capability("channel_pub", "orders", _hash("s3"))
    assert await sqlite_storage.delete_expired_capabilities() == 0
    assert await sqlite_storage.check_capability("channel_pub", "orders", _hash("s3"))


@pytest.mark.asyncio
async def test_delete_expired_capabilities(sqlite_storage):
    past = (datetime.now(UTC) - timedelta(hours=1)).isoformat()
    future = (datetime.now(UTC) + timedelta(hours=1)).isoformat()
    await sqlite_storage.grant_capability(
        "cap-old", "fs_write", "/shared/", _hash("a"), expires_at=past
    )
    await sqlite_storage.grant_capability(
        "cap-new", "fs_write", "/shared/", _hash("b"), expires_at=future
    )
    await sqlite_storage.grant_capability("cap-forever", "fs_write", "/shared/", _hash("c"))

    assert await sqlite_storage.delete_expired_capabilities() == 1
    assert not await sqlite_storage.check_capability("fs_write", "/shared/", _hash("a"))
    assert await sqlite_storage.check_capability("fs_write", "/shared/", _hash("b"))
    assert await sqlite_storage.check_capability("fs_write", "/shared/", _hash("c"))


@pytest.mark.asyncio
async def test_issue_capability_is_a_recorded_step(sqlite_storage, replay_engine):
    """A replay returns the recorded (token, capability_id) instead of minting
    a second grant, so revoking by that id really revokes the issued token."""
    issued = []

    @workflow
    async def minter_wf(ctx: WorkflowContext) -> dict:
        result = await actors.issue_capability(ctx, "channel_pub", "orders")
        issued.append(result)
        await sleep(ctx, 1)
        await actors.revoke_capability(ctx, result[1])
        return {"cap_id": result[1]}

    iid = await minter_wf.start()
    await _fire_timer_and_resume(sqlite_storage, replay_engine, iid, minter_wf)

    assert (await sqlite_storage.get_instance(iid))["status"] == "completed"
    assert len(issued) == 2 and issued[0] == issued[1]
    assert isinstance(issued[1], tuple)
    assert not await sqlite_storage.has_capability_grants("channel_pub", "orders")
