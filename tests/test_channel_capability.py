"""Sealed-channel capability enforcement tests."""

import pytest
from sqlalchemy import text

from edda.activity import activity
from edda.channels import _enforce_channel_capability, publish, send_to, sleep
from edda.context import WorkflowContext
from edda.exceptions import ChannelPermissionError
from edda.replay import ReplayEngine
from edda.workflow import set_replay_engine, workflow

pytestmark = [pytest.mark.asyncio, pytest.mark.multidb]


async def _grant(storage, kind, target, secret="s3cret"):
    from edda.actors import _hash_secret

    await storage.grant_capability(
        capability_id=f"cap-test-{kind}-{target}",
        kind=kind,
        target=target,
        secret_hash=_hash_secret(secret),
        granted_by="test",
    )
    return f"cap:{kind}:{target}:{secret}"


async def test_open_channel_allows_anonymous_publish(sqlite_storage):
    # no grants — publish without any token
    msg_id = await publish(sqlite_storage, "open-ch", {"n": 1})
    assert msg_id


async def test_sealed_publish_requires_token(sqlite_storage):
    token = await _grant(sqlite_storage, "channel_pub", "sealed-pub")

    with pytest.raises(ChannelPermissionError):
        await publish(sqlite_storage, "sealed-pub", {"n": 1})

    with pytest.raises(ChannelPermissionError):
        await publish(
            sqlite_storage,
            "sealed-pub",
            {"n": 1},
            capability_token="cap:channel_pub:sealed-pub:wrong",
        )

    msg_id = await publish(sqlite_storage, "sealed-pub", {"n": 1}, capability_token=token)
    assert msg_id


async def test_sealed_receive_requires_token(sqlite_storage):
    await _grant(sqlite_storage, "channel_sub", "sealed-sub")

    with pytest.raises(ChannelPermissionError):
        await _enforce_channel_capability(sqlite_storage, "channel_sub", "sealed-sub", None)


async def test_sealing_one_channel_does_not_affect_others(sqlite_storage):
    await _grant(sqlite_storage, "channel_pub", "other-ch")

    # plain-ch has no grants — enforcement is a no-op
    await _enforce_channel_capability(sqlite_storage, "channel_pub", "plain-ch", None)
    msg_id = await publish(sqlite_storage, "plain-ch", {"n": 1})
    assert msg_id


async def test_revoked_capability_unseals_channel(sqlite_storage):
    token = await _grant(sqlite_storage, "channel_pub", "temp-ch")
    cap_id = "cap-test-channel_pub-temp-ch"

    with pytest.raises(ChannelPermissionError):
        await publish(sqlite_storage, "temp-ch", {"n": 1})

    await sqlite_storage.revoke_capability(cap_id)
    # no grants left — open again
    msg_id = await publish(sqlite_storage, "temp-ch", {"n": 1})
    assert msg_id
    _ = token


async def test_seal_covers_the_direct_names_under_a_channel(sqlite_storage):
    """A seal on L also covers every physical name "L:..." -- otherwise
    publishing to "L:{victim}" bypasses the seal send_to(victim, L) enforces,
    and receiving on "L:{victim}" bypasses L's channel_sub seal."""
    pub_token = await _grant(sqlite_storage, "channel_pub", "orders")
    with pytest.raises(ChannelPermissionError):
        await publish(sqlite_storage, "orders:victim-1", {"n": 1})
    assert await publish(sqlite_storage, "orders:victim-1", {"n": 1}, capability_token=pub_token)

    sub_token = await _grant(sqlite_storage, "channel_sub", "inbox")
    with pytest.raises(ChannelPermissionError):
        await _enforce_channel_capability(sqlite_storage, "channel_sub", "inbox:victim-1", None)
    await _enforce_channel_capability(sqlite_storage, "channel_sub", "inbox:victim-1", sub_token)

    # Only prefixes that end before a ':' are scopes.
    await _enforce_channel_capability(sqlite_storage, "channel_pub", "orders2", None)


# --- publish / send_to from workflow code are recorded steps


@pytest.fixture
def replay_engine(sqlite_storage):
    engine = ReplayEngine(storage=sqlite_storage, service_name="cap-test", worker_id="cap-w")
    set_replay_engine(engine)
    yield engine
    set_replay_engine(None)


async def _fire_timer_and_resume(storage, engine, iid, workflow_name):
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
    await engine.resume_by_name(iid, workflow_name)
    return await storage.get_instance(iid)


@workflow
async def publish_then_sleep_wf(ctx: WorkflowContext, token: str | None = None) -> dict:
    message_id = await publish(ctx, "replayed-pub", {"n": 1}, capability_token=token)
    await sleep(ctx, 30)
    return {"message_id": message_id}


async def test_publish_is_not_repeated_on_replay(sqlite_storage, replay_engine):
    iid = await publish_then_sleep_wf.start()
    inst = await _fire_timer_and_resume(sqlite_storage, replay_engine, iid, "publish_then_sleep_wf")

    assert inst["status"] == "completed"
    messages = await sqlite_storage.channel_messages_since("replayed-pub", None)
    assert [m["message_id"] for m in messages] == [inst["output_data"]["result"]["message_id"]]


async def test_replayed_publish_does_not_recheck_the_capability(sqlite_storage, replay_engine):
    """The capability check runs after the replay check: a recorded publish
    stays valid after its grant is revoked (the channel stays sealed by
    another grant)."""
    from edda.actors import _hash_secret

    token = await _grant(sqlite_storage, "channel_pub", "replayed-pub")
    await sqlite_storage.grant_capability(
        capability_id="cap-other",
        kind="channel_pub",
        target="replayed-pub",
        secret_hash=_hash_secret("other-secret"),
        granted_by="test",
    )
    iid = await publish_then_sleep_wf.start(token=token)
    await sqlite_storage.revoke_capability("cap-test-channel_pub-replayed-pub")

    inst = await _fire_timer_and_resume(sqlite_storage, replay_engine, iid, "publish_then_sleep_wf")
    assert inst["status"] == "completed"
    assert len(await sqlite_storage.channel_messages_since("replayed-pub", None)) == 1


@workflow
async def send_then_sleep_wf(ctx: WorkflowContext, target: str) -> dict:
    await send_to(ctx, target, {"n": 1}, channel="replayed-direct")
    await sleep(ctx, 30)
    return {}


async def test_send_to_is_not_repeated_on_replay(
    sqlite_storage, replay_engine, create_test_instance
):
    target = await create_test_instance("wf-send-target", "wf", {})
    iid = await send_then_sleep_wf.start(target=target)
    inst = await _fire_timer_and_resume(sqlite_storage, replay_engine, iid, "send_then_sleep_wf")

    assert inst["status"] == "completed"
    assert len(await sqlite_storage.channel_messages_since(f"replayed-direct:{target}", None)) == 1


@activity
async def publish_twice_activity(ctx: WorkflowContext) -> dict:
    await publish(ctx, "activity-pub", {"n": 1})
    await publish(ctx, "activity-pub", {"n": 2})
    return {}


async def test_publish_inside_an_activity_is_not_a_step(sqlite_storage, replay_engine):
    """The enclosing activity is the replay unit: publishing inside it
    records nothing and leaves the workflow's step ids alone."""

    @workflow
    async def publish_in_activity_wf(ctx: WorkflowContext) -> dict:
        await publish_twice_activity(ctx)
        await publish(ctx, "activity-pub", {"n": 3})
        return {}

    iid = await publish_in_activity_wf.start()

    history = await sqlite_storage.get_history(iid)
    assert sorted(h["activity_id"] for h in history) == [
        "edda.publish:1",
        "publish_twice_activity:1",
    ]
    assert len(await sqlite_storage.channel_messages_since("activity-pub", None)) == 3
