"""receive() returns the published payload unchanged, live and on replay."""

import pytest
from sqlalchemy import text

from edda.channels import _message_data_from_history, publish, receive, sleep, subscribe
from edda.context import WorkflowContext
from edda.replay import ReplayEngine

pytestmark = pytest.mark.multidb

PAYLOADS = [{"n": 1}, [1, 2, 3], "text", 7, 2.5, True, None, b"\x00\x01binary"]


def _plain(data):
    """JSON form of a received payload (workflow output must be JSON)."""
    if isinstance(data, bytes):
        return {"bytes": data.hex()}
    return {"value": data}


async def _fire_timer_and_resume(storage, engine, instance_id, workflow_func):
    async with storage.engine.begin() as conn:
        timer_id, activity_id = (
            await conn.execute(
                text(
                    "SELECT timer_id, activity_id FROM workflow_timer_subscriptions "
                    "WHERE instance_id = :i"
                ),
                {"i": instance_id},
            )
        ).one()
    await storage.append_history(
        instance_id,
        activity_id=activity_id,
        event_type="TimerExpired",
        event_data={"result": None, "timer_id": timer_id},
    )
    await storage.remove_timer_subscription(instance_id, timer_id)
    await storage.update_instance_status(instance_id, "running")
    await engine.resume_workflow(instance_id, workflow_func)


async def test_receive_returns_every_json_value_and_bytes_unchanged(sqlite_storage):
    engine = ReplayEngine(
        storage=sqlite_storage, service_name="payloads", worker_id="payloads-worker"
    )

    async def receiving(ctx: WorkflowContext) -> dict:
        await subscribe(ctx, "payloads", mode="broadcast")
        await sleep(ctx, 1)  # the payloads are published while it sleeps
        got = []
        for _ in PAYLOADS:
            message = await receive(ctx, "payloads")
            got.append(_plain(message.data))
        await sleep(ctx, 1)  # the last run replays every receive from history
        return {"got": got}

    instance_id = await engine.start_workflow("payloads_receiving", receiving, {})
    for payload in PAYLOADS:
        await publish(sqlite_storage, "payloads", payload)

    await _fire_timer_and_resume(sqlite_storage, engine, instance_id, receiving)
    await _fire_timer_and_resume(sqlite_storage, engine, instance_id, receiving)

    instance = await sqlite_storage.get_instance(instance_id)
    assert instance is not None
    assert instance["status"] == "completed", instance
    assert instance["output_data"]["result"] == {"got": [_plain(p) for p in PAYLOADS]}


def test_history_entries_written_before_0_17_replay_as_before():
    assert _message_data_from_history(b"\x01raw") == b"\x01raw"
    assert _message_data_from_history({"id": "m", "data": {"n": 1}}) == {"n": 1}
    assert _message_data_from_history({"id": "m", "data": None}) == {}
    assert _message_data_from_history({"id": "m", "payload": {"p": 1}}) == {"p": 1}


async def test_a_raw_binary_entry_written_before_0_17_replays(sqlite_storage, create_test_instance):
    await create_test_instance("legacy-binary", "test_workflow", {})
    await sqlite_storage.append_history(
        "legacy-binary",
        activity_id="receive_legacy:1",
        event_type="ChannelMessageReceived",
        event_data=b"\x00legacy",
    )
    ctx = WorkflowContext(
        instance_id="legacy-binary",
        workflow_name="test_workflow",
        storage=sqlite_storage,
        worker_id="worker-1",
        is_replaying=True,
    )
    await ctx._load_history()
    message = await receive(ctx, "legacy")
    assert message.data == b"\x00legacy"
