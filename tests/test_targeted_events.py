"""
Targeted events and direct-channel mailboxes (SEMANTICS §3, §3c, §5).

A CloudEvent with the eddainstanceid extension, and a publish with
target_instance_id, are for one instance only: they are stored on its direct
channel "{type}:{instance}", which no other instance can read, and wait_event
in the target receives them like the events of its type. A direct channel is
a mailbox: a subscription to it also gets the messages sent before it. Cursors
live in channel_delivery_cursors, which outlives the subscriptions, so a
channel subscribed again after a recur continues where it stopped.
"""

import asyncio
import json
from typing import Any

import pytest
from sqlalchemy import text

from edda import EddaApp, WorkflowContext, workflow
from edda.channels import publish, receive, subscribe, wait_event

pytestmark = pytest.mark.multidb


# =============================================================================
# Storage
# =============================================================================


async def pending_data(storage: Any, instance_id: str, channel: str) -> list[Any]:
    return [m["data"] for m in await storage.get_pending_channel_messages(instance_id, channel)]


async def test_direct_channel_is_a_mailbox(sqlite_storage, create_test_instance):
    instance_id = await create_test_instance("mailbox-1", "wf", {})
    mailbox = f"orders:{instance_id}"
    await sqlite_storage.publish_to_channel(mailbox, {"n": 1}, {})
    await sqlite_storage.publish_to_channel("orders", {"n": 2}, {})

    await sqlite_storage.subscribe_to_channel(instance_id, mailbox, "broadcast")
    await sqlite_storage.subscribe_to_channel(instance_id, "orders", "broadcast")

    # The mailbox keeps what was sent before the subscription; a broadcast
    # subscription starts after the channel's latest message.
    assert await pending_data(sqlite_storage, instance_id, mailbox) == [{"n": 1}]
    assert await pending_data(sqlite_storage, instance_id, "orders") == []


async def test_recur_keeps_the_mailbox_position(sqlite_storage, create_test_instance):
    instance_id = await create_test_instance("mailbox-recur", "wf", {})
    mailbox = f"orders:{instance_id}"
    await sqlite_storage.subscribe_to_channel(instance_id, mailbox, "broadcast")
    await sqlite_storage.publish_to_channel(mailbox, {"n": 1}, {})
    [first] = await sqlite_storage.get_pending_channel_messages(instance_id, mailbox)
    await sqlite_storage.update_delivery_cursor(mailbox, instance_id, first["id"])

    lock_id = "mailbox-recur#1"
    assert await sqlite_storage.try_acquire_lock(instance_id, lock_id)
    await sqlite_storage.reset_instance_for_recur(instance_id, {}, lock_id)
    await sqlite_storage.release_lock(instance_id, lock_id)
    await sqlite_storage.publish_to_channel(mailbox, {"n": 2}, {})

    # The next iteration subscribes again and gets only what it has not received.
    await sqlite_storage.subscribe_to_channel(instance_id, mailbox, "broadcast")
    assert await pending_data(sqlite_storage, instance_id, mailbox) == [{"n": 2}]


async def test_a_cursor_moved_by_another_port_is_respected(sqlite_storage, create_test_instance):
    instance_id = await create_test_instance("cursor-port", "wf", {})
    await sqlite_storage.subscribe_to_channel(instance_id, "ticks", "broadcast")
    await sqlite_storage.publish_to_channel("ticks", {"n": 1}, {})
    await sqlite_storage.publish_to_channel("ticks", {"n": 2}, {})
    first, _ = await sqlite_storage.get_pending_channel_messages(instance_id, "ticks")

    # Romancy delivers the first message and moves only the durable cursor.
    async with sqlite_storage.engine.begin() as conn:
        await conn.execute(
            text(
                "UPDATE channel_delivery_cursors SET last_delivered_id = :m "
                "WHERE instance_id = :i AND channel = 'ticks'"
            ),
            {"m": first["id"], "i": instance_id},
        )

    assert await pending_data(sqlite_storage, instance_id, "ticks") == [{"n": 2}]


async def test_wait_on_two_channels(sqlite_storage, create_test_instance):
    instance_id = await create_test_instance("two-channels", "wf", {})
    mailbox = f"payment.done:{instance_id}"
    for channel in ("payment.done", mailbox):
        await sqlite_storage.subscribe_to_channel(instance_id, channel, "broadcast")

    async def register(lock_id: str, activity_id: str) -> bool:
        assert await sqlite_storage.try_acquire_lock(instance_id, lock_id)
        registered: bool = await sqlite_storage.register_channel_receive_and_release_lock(
            instance_id, lock_id, "payment.done", activity_id, 60, also_channels=[mailbox]
        )
        return registered

    async def waits() -> list[Any]:
        rows = [
            await sqlite_storage.get_channel_subscription(instance_id, channel)
            for channel in ("payment.done", mailbox)
        ]
        return [(row["activity_id"], row["timeout_at"] is not None) for row in rows]

    # A message pending on the second channel keeps the instance running.
    await sqlite_storage.publish_to_channel(mailbox, {"n": 1}, {})
    assert await register("w#1", "receive_payment.done:1") is False
    instance = await sqlite_storage.get_instance(instance_id)
    assert instance["status"] == "running"
    assert instance["locked_by"] is None
    [pending] = await sqlite_storage.get_pending_channel_messages(instance_id, mailbox)
    await sqlite_storage.update_delivery_cursor(mailbox, instance_id, pending["id"])

    # Registered on both; the timeout only on the first.
    assert await register("w#2", "receive_payment.done:2") is True
    assert await waits() == [("receive_payment.done:2", True), ("receive_payment.done:2", False)]

    # A delivery on the second ends the wait on both.
    message_id = await sqlite_storage.publish_to_channel(mailbox, {"n": 2}, {})
    delivered = await sqlite_storage.deliver_channel_message(
        instance_id, mailbox, message_id, {"n": 2}, {}, "w#3"
    )
    assert delivered is not None
    assert delivered["activity_id"] == "receive_payment.done:2"
    assert await waits() == [(None, False), (None, False)]

    # Ending the wait on the first (a timeout) ends it on both.
    assert await register("w#4", "receive_payment.done:3") is True
    await sqlite_storage.remove_message_subscription(instance_id, "payment.done")
    assert await waits() == [(None, False), (None, False)]


async def test_targeted_publish_is_not_seen_by_other_subscribers(
    sqlite_storage, create_test_instance
):
    target = await create_test_instance("publish-target", "wf", {})
    other = await create_test_instance("publish-other", "wf", {})
    for instance_id in (target, other):
        await sqlite_storage.subscribe_to_channel(instance_id, "approvals", "broadcast")

    await publish(
        sqlite_storage, "approvals", {"ok": True}, target_instance_id=target, worker_id="w"
    )

    assert await pending_data(sqlite_storage, other, "approvals") == []
    assert await pending_data(sqlite_storage, target, "approvals") == []
    await sqlite_storage.subscribe_to_channel(target, f"approvals:{target}", "broadcast")
    assert await pending_data(sqlite_storage, target, f"approvals:{target}") == [{"ok": True}]


# =============================================================================
# CloudEvents and wait_event
# =============================================================================


@pytest.fixture
async def app(tmp_path):
    app = EddaApp(service_name="targeted", db_url=f"sqlite:///{tmp_path / 'targeted.db'}")
    await app.initialize()
    yield app
    if app._initialized:
        await app.shutdown()


async def post_event(app: EddaApp, event_type: str, n: int, target: str | None = None) -> None:
    """POST a structured-mode CloudEvent, for one instance when target is set,
    and wait for the work it starts."""
    event: dict[str, Any] = {
        "specversion": "1.0",
        "type": event_type,
        "source": "test",
        "id": f"{event_type}-{n}",
        "data": {"n": n},
    }
    if target is not None:
        event["eddainstanceid"] = target
    body = json.dumps(event).encode()
    response: dict[str, Any] = {}

    async def receive_body() -> dict[str, Any]:
        return {"type": "http.request", "body": body, "more_body": False}

    async def send(message: dict[str, Any]) -> None:
        if message["type"] == "http.response.start":
            response["status"] = message["status"]

    scope = {
        "type": "http",
        "method": "POST",
        "path": "/",
        "query_string": b"",
        "headers": [(b"content-type", b"application/cloudevents+json")],
    }
    await app(scope, receive_body, send)
    assert response["status"] == 202
    await asyncio.gather(*list(app._event_tasks))


async def status_of(app: EddaApp, instance_id: str) -> str:
    instance = await app.storage.get_instance(instance_id)
    assert instance is not None
    return str(instance["status"])


async def result_of(app: EddaApp, instance_id: str) -> Any:
    """Resume what is runnable until instance_id completes; its result."""
    for _ in range(50):
        if await status_of(app, instance_id) == "completed":
            instance = await app.storage.get_instance(instance_id)
            assert instance is not None
            return instance["output_data"]["result"]
        await app._resume_running_workflows()
        await asyncio.sleep(0.02)
    raise AssertionError(f"{instance_id} did not complete: {await status_of(app, instance_id)}")


@workflow
async def targeted_wait_once(ctx: WorkflowContext, event_type: str) -> list[int]:
    event = await wait_event(ctx, event_type)
    return [event.data["n"]]


async def test_targeted_event_reaches_only_its_target(app):
    a = await targeted_wait_once.start(event_type="targeted.only")
    b = await targeted_wait_once.start(event_type="targeted.only")

    await post_event(app, "targeted.only", 1, target=a)
    assert await result_of(app, a) == [1]
    # Nothing is pending for b: the sweep that requeues waits with pending
    # messages leaves it waiting.
    await app.storage.requeue_channel_waits_with_pending()
    await app._resume_running_workflows()
    assert await status_of(app, b) == "waiting_for_message"

    await post_event(app, "targeted.only", 2)
    assert await result_of(app, b) == [2]


@workflow
async def targeted_after_gate(ctx: WorkflowContext, event_type: str) -> list[int]:
    await wait_event(ctx, f"{event_type}.gate")
    event = await wait_event(ctx, event_type)
    return [event.data["n"]]


async def test_targeted_event_that_arrived_before_the_wait(app):
    instance_id = await targeted_after_gate.start(event_type="targeted.early")

    await post_event(app, "targeted.early", 7, target=instance_id)
    await post_event(app, "targeted.early.gate", 0)
    assert await result_of(app, instance_id) == [7]


@workflow
async def targeted_wait_twice(ctx: WorkflowContext, event_type: str) -> list[int]:
    # Subscribed before the events, so the untargeted one counts.
    await subscribe(ctx, event_type, mode="broadcast")
    await wait_event(ctx, f"{event_type}.gate")
    got = []
    for _ in range(2):
        event = await wait_event(ctx, event_type)
        got.append(event.data["n"])
    return got


@pytest.mark.parametrize("targeted_first", [False, True])
async def test_wait_event_takes_the_oldest_event_first(app, targeted_first):
    event_type = f"targeted.order.{targeted_first}"
    instance_id = await targeted_wait_twice.start(event_type=event_type)

    for n, targeted in ((1, targeted_first), (2, not targeted_first)):
        await post_event(app, event_type, n, target=instance_id if targeted else None)
    await post_event(app, f"{event_type}.gate", 0)
    assert await result_of(app, instance_id) == [1, 2]


@workflow
async def targeted_timeout(ctx: WorkflowContext, event_type: str) -> list[int]:
    try:
        await wait_event(ctx, event_type, timeout_seconds=1)
        return []
    except TimeoutError:
        pass
    event = await wait_event(ctx, f"{event_type}.next")
    return [-1, event.data["n"]]


async def test_a_timeout_ends_the_wait_on_both_channels(app):
    instance_id = await targeted_timeout.start(event_type="targeted.slow")
    # Deadlines are kept to the second: the timeout fires within two seconds.
    for _ in range(40):
        await asyncio.sleep(0.1)
        await app._check_expired_message_subscriptions()
        if await app.storage.get_channel_subscription(instance_id, "targeted.slow.next"):
            break

    for channel in ("targeted.slow", f"targeted.slow:{instance_id}"):
        subscription = await app.storage.get_channel_subscription(instance_id, channel)
        assert subscription is not None
        assert subscription["activity_id"] is None, channel
    # A late event for the wait that timed out is not recorded into it.
    await post_event(app, "targeted.slow", 1, target=instance_id)
    await post_event(app, "targeted.slow.next", 2, target=instance_id)
    assert await result_of(app, instance_id) == [-1, 2]


# Switches targeted_upgraded from the wait of earlier versions (the event
# type's channel alone) to wait_event.
waits_on_the_type_alone = {"on": True}


@workflow
async def targeted_upgraded(ctx: WorkflowContext, event_type: str) -> list[int]:
    if waits_on_the_type_alone["on"]:
        await subscribe(ctx, event_type, mode="broadcast")
        message = await receive(ctx, event_type)
        return [message.data["n"]]
    event = await wait_event(ctx, event_type)
    return [event.data["n"]]


async def test_a_wait_on_the_type_alone_is_resumed(app):
    waits_on_the_type_alone["on"] = True
    instance_id = await targeted_upgraded.start(event_type="targeted.upgraded")
    assert await status_of(app, instance_id) == "waiting_for_message"

    waits_on_the_type_alone["on"] = False  # the upgrade
    await post_event(app, "targeted.upgraded", 5, target=instance_id)
    assert await result_of(app, instance_id) == [5]


@workflow
async def targeted_direct(ctx: WorkflowContext, event_type: str) -> list[int]:
    await wait_event(ctx, f"{event_type}.gate")
    await subscribe(ctx, event_type, mode="direct")
    message = await receive(ctx, event_type)
    return [message.data["n"]]


async def test_a_direct_subscription_gets_earlier_targeted_events(app):
    instance_id = await targeted_direct.start(event_type="targeted.direct")

    await post_event(app, "targeted.direct", 3, target=instance_id)
    await post_event(app, "targeted.direct.gate", 0)
    assert await result_of(app, instance_id) == [3]
