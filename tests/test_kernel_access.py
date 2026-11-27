"""Kernel semantics: capabilities, cursor reads, direct-channel privacy,
the claim filter, and "inside an activity body" tracking."""

import asyncio
import hashlib
import hmac
import json
from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy import text

from edda import actors, tasks
from edda.activity import activity
from edda.channels import publish, receive, send_to, sleep, subscribe
from edda.context import WorkflowContext
from edda.exceptions import (
    ChannelPermissionError,
    CursorNotFoundError,
    NotOwnerError,
    TerminalError,
)
from edda.replay import ReplayEngine

pytestmark = pytest.mark.multidb


def _engine(storage, **kwargs):
    return ReplayEngine(
        storage=storage, service_name="phase-b", worker_id="phase-b-worker", **kwargs
    )


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


def _hash(secret: str) -> str:
    return hashlib.sha256(secret.encode()).hexdigest()


async def _result(storage, instance_id):
    instance = await storage.get_instance(instance_id)
    assert instance is not None
    assert instance["status"] == "completed", instance
    return instance["output_data"]["result"]


async def _grant_ids(storage):
    async with storage.engine.begin() as conn:
        rows = (await conn.execute(text("SELECT id FROM actor_capabilities ORDER BY id"))).all()
    return [r[0] for r in rows]


# ---------------------------------------------------------------------------
# S7a: expiry does not unseal
# ---------------------------------------------------------------------------


async def _set_created_at(storage, capability_id, when):
    async with storage.engine.begin() as conn:
        await conn.execute(
            text("UPDATE actor_capabilities SET created_at = :c WHERE id = :i"),
            {"c": storage._ts_param(when), "i": capability_id},
        )


def _past():
    return (datetime.now(UTC) - timedelta(hours=1)).isoformat()


async def test_an_expired_only_grant_survives_the_sweep_and_keeps_the_channel_sealed(
    sqlite_storage,
):
    await sqlite_storage.grant_capability(
        "cap-only", "channel_pub", "sealed-expiry", _hash("s"), expires_at=_past()
    )

    assert await sqlite_storage.delete_expired_capabilities() == 0

    assert await _grant_ids(sqlite_storage) == ["cap-only"]
    assert await sqlite_storage.has_capability_grants("channel_pub", "sealed-expiry")
    with pytest.raises(ChannelPermissionError):
        await publish(sqlite_storage, "sealed-expiry", {"n": 1})
    with pytest.raises(ChannelPermissionError):
        await publish(
            sqlite_storage,
            "sealed-expiry",
            {"n": 1},
            capability_token="cap:channel_pub:sealed-expiry:s",
        )


async def test_an_expired_grant_with_a_live_sibling_is_deleted(sqlite_storage):
    future = (datetime.now(UTC) + timedelta(hours=1)).isoformat()
    await sqlite_storage.grant_capability(
        "cap-old", "channel_pub", "sibling", _hash("a"), expires_at=_past()
    )
    await sqlite_storage.grant_capability(
        "cap-live", "channel_pub", "sibling", _hash("b"), expires_at=future
    )
    # a grant of another kind or target never keeps this one alive
    await sqlite_storage.grant_capability(
        "cap-other", "channel_sub", "sibling", _hash("c"), expires_at=_past()
    )

    assert await sqlite_storage.delete_expired_capabilities() == 1

    assert await _grant_ids(sqlite_storage) == ["cap-live", "cap-other"]


async def test_the_newest_expired_grant_stays_and_equal_created_at_keeps_the_larger_id(
    sqlite_storage,
):
    now = datetime.now(UTC)
    for cap_id in ("cap-1", "cap-2", "cap-3", "cap-eq-a", "cap-eq-b"):
        target = "eq" if cap_id.startswith("cap-eq") else "newest"
        await sqlite_storage.grant_capability(
            cap_id, "channel_pub", target, _hash(cap_id), expires_at=_past()
        )
    await _set_created_at(sqlite_storage, "cap-1", now - timedelta(minutes=30))
    await _set_created_at(sqlite_storage, "cap-2", now - timedelta(minutes=20))
    await _set_created_at(sqlite_storage, "cap-3", now - timedelta(minutes=10))
    await _set_created_at(sqlite_storage, "cap-eq-a", now - timedelta(minutes=5))
    await _set_created_at(sqlite_storage, "cap-eq-b", now - timedelta(minutes=5))

    assert await sqlite_storage.delete_expired_capabilities() == 3

    assert await _grant_ids(sqlite_storage) == ["cap-3", "cap-eq-b"]
    assert not await sqlite_storage.check_capability("channel_pub", "newest", _hash("cap-3"))


# ---------------------------------------------------------------------------
# S7b: ownership of revoke and unregister
# ---------------------------------------------------------------------------


async def test_revoke_and_unregister_are_owner_checked_and_recorded(sqlite_storage):
    engine = _engine(sqlite_storage)
    issued: dict[str, str] = {}

    async def owner_workflow(ctx: WorkflowContext) -> dict:
        await actors.register_name(ctx, "phase-b-name")
        _, issued["cap"] = await actors.issue_capability(ctx, "actor", "phase-b-target")
        return {}

    async def intruder_workflow(ctx: WorkflowContext) -> dict:
        outcome = {}
        for label, call in (
            ("revoke", lambda: actors.revoke_capability(ctx, issued["cap"])),
            ("unregister", lambda: actors.unregister_name(ctx, "phase-b-name")),
        ):
            try:
                await call()
                outcome[label] = "ok"
            except NotOwnerError:
                outcome[label] = "not_owner"
        # unknown id / unbound name: successful no-ops
        await actors.revoke_capability(ctx, "cap-does-not-exist")
        await actors.unregister_name(ctx, "phase-b-unbound")
        return outcome

    owner_id = await engine.start_workflow("phase_b_owner", owner_workflow, {})
    intruder_id = await engine.start_workflow("phase_b_intruder", intruder_workflow, {})

    assert await _result(sqlite_storage, intruder_id) == {
        "revoke": "not_owner",
        "unregister": "not_owner",
    }
    assert await _grant_ids(sqlite_storage) == [issued["cap"]]
    assert await sqlite_storage.whereis_actor("phase-b-name") == owner_id
    failures = [
        h["event_data"]["code"]
        for h in await sqlite_storage.get_history(intruder_id)
        if h["event_type"] == "ActivityFailed"
    ]
    assert failures == ["not_owner", "not_owner"]

    # the owner itself may revoke and unregister
    ctx = WorkflowContext(
        instance_id=owner_id, workflow_name="x", storage=sqlite_storage, worker_id="w"
    )
    await actors.revoke_capability(ctx, issued["cap"])
    await actors.unregister_name(ctx, "phase-b-name")
    assert await _grant_ids(sqlite_storage) == []
    assert await sqlite_storage.whereis_actor("phase-b-name") is None


async def test_a_not_owner_refusal_is_replayed_from_history(sqlite_storage, monkeypatch):
    engine = _engine(sqlite_storage)
    await sqlite_storage.grant_capability(
        "cap-foreign", "actor", "phase-b-replay", _hash("x"), granted_by="somebody-else"
    )
    calls = 0
    original = sqlite_storage.revoke_own_capability

    async def counting(capability_id, owner):
        nonlocal calls
        calls += 1
        return await original(capability_id, owner)

    monkeypatch.setattr(sqlite_storage, "revoke_own_capability", counting)

    async def workflow(ctx: WorkflowContext) -> dict:
        try:
            await actors.revoke_capability(ctx, "cap-foreign")
            outcome = "revoked"
        except NotOwnerError:
            outcome = "not_owner"
        await sleep(ctx, 1)
        return {"outcome": outcome}

    instance_id = await engine.start_workflow("phase_b_not_owner_replay", workflow, {})
    assert calls == 1
    await _fire_timer_and_resume(sqlite_storage, engine, instance_id, workflow)

    assert calls == 1
    assert await _result(sqlite_storage, instance_id) == {"outcome": "not_owner"}


# ---------------------------------------------------------------------------
# S7c: capability key
# ---------------------------------------------------------------------------


def _derive(key: bytes, capability_id: str) -> str:
    return hmac.new(key, capability_id.encode(), hashlib.sha256).hexdigest()


async def test_a_keyed_capability_derives_its_secret_and_keeps_tokens_out_of_history(
    sqlite_storage,
):
    key = b"phase-b-key"
    engine = _engine(sqlite_storage, capability_key=key)

    async def issuing(ctx: WorkflowContext) -> dict:
        token, cap_id = await actors.issue_capability(ctx, "channel_pub", "keyed-orders")
        await sleep(ctx, 1)
        return {"token": token, "cap_id": cap_id}

    instance_id = await engine.start_workflow("phase_b_keyed", issuing, {})
    history = await sqlite_storage.get_history(instance_id)
    record = next(h for h in history if h["activity_id"].startswith("edda.issue_capability"))
    cap_id = record["event_data"]["result"]["id"]
    assert record["event_data"]["result"] == {
        "id": cap_id,
        "kind": "channel_pub",
        "target": "keyed-orders",
        "derived": True,
    }
    secret = _derive(key, cap_id)
    assert secret not in json.dumps(history, default=str)

    # replay (after the suspension) derives the same token, and it validates
    await _fire_timer_and_resume(sqlite_storage, engine, instance_id, issuing)
    result = await _result(sqlite_storage, instance_id)
    assert result["cap_id"] == cap_id
    assert result["token"] == f"cap:channel_pub:keyed-orders:{secret}"
    assert await publish(sqlite_storage, "keyed-orders", {"n": 1}, capability_token=result["token"])
    with pytest.raises(ChannelPermissionError):
        await publish(sqlite_storage, "keyed-orders", {"n": 1})


async def test_a_capability_without_a_key_keeps_the_token_in_the_recorded_step(sqlite_storage):
    engine = _engine(sqlite_storage)

    async def issuing(ctx: WorkflowContext) -> dict:
        token, cap_id = await actors.issue_capability(ctx, "channel_pub", "unkeyed-orders")
        await sleep(ctx, 1)
        return {"token": token, "cap_id": cap_id}

    instance_id = await engine.start_workflow("phase_b_unkeyed", issuing, {})
    history = await sqlite_storage.get_history(instance_id)
    record = next(h for h in history if h["activity_id"].startswith("edda.issue_capability"))
    token, cap_id = record["event_data"]["result"]
    assert token.startswith("cap:channel_pub:unkeyed-orders:")

    await _fire_timer_and_resume(sqlite_storage, engine, instance_id, issuing)
    assert await _result(sqlite_storage, instance_id) == {"token": token, "cap_id": cap_id}


def test_the_capability_key_comes_from_the_option_then_the_environment(monkeypatch):
    from edda import EddaApp

    def app(**kwargs):
        return EddaApp(service_name="s", db_url="sqlite+aiosqlite:///:memory:", **kwargs)

    monkeypatch.delenv("DURAX_CAPABILITY_KEY", raising=False)
    assert app()._capability_key is None
    assert app(capability_key="ключ")._capability_key == "ключ".encode()
    assert app(capability_key=b"raw")._capability_key == b"raw"

    monkeypatch.setenv("DURAX_CAPABILITY_KEY", "from-env")
    assert app()._capability_key == b"from-env"
    assert app(capability_key=b"option")._capability_key == b"option"

    monkeypatch.setenv("DURAX_CAPABILITY_KEY", "")
    assert app()._capability_key is None


# ---------------------------------------------------------------------------
# S7d: CloudEvents ingress respects seals (SQLite only: the check is dialect-free)
# ---------------------------------------------------------------------------


async def _post(app, headers, body):
    status = 0
    response = b""

    async def receive_():
        return {"type": "http.request", "body": body, "more_body": False}

    async def send(message):
        nonlocal status, response
        if message["type"] == "http.response.start":
            status = message["status"]
        elif message["type"] == "http.response.body":
            response += message.get("body", b"")

    scope = {
        "type": "http",
        "method": "POST",
        "path": "/",
        "query_string": b"",
        "headers": [(k.encode(), v.encode()) for k, v in headers.items()],
    }
    await app(scope, receive_, send)
    return status, json.loads(response)


def _structured(event_type, **extra):
    event = {
        "specversion": "1.0",
        "type": event_type,
        "source": "phase-b",
        "id": f"evt-{event_type}-{len(extra)}",
        "datacontenttype": "application/json",
        "data": {"n": 1},
        **extra,
    }
    return {"content-type": "application/cloudevents+json"}, json.dumps(event).encode()


async def _stored(app, channel):
    for _ in range(50):
        rows = await app.storage.channel_messages_since(channel, None)
        if rows:
            return rows
        await asyncio.sleep(0.02)
    return []


@pytest.fixture
async def ingress_app():
    from edda import EddaApp

    app = EddaApp(service_name="phase-b-ingress", db_url="sqlite+aiosqlite:///:memory:")
    await app.initialize()
    yield app
    await app.shutdown()


async def test_a_sealed_channel_refuses_events_without_a_valid_token(ingress_app):
    app = ingress_app
    await app.storage.grant_capability(
        "cap-ingress", "channel_pub", "sealed.event", _hash("tok"), granted_by="x"
    )

    headers, body = _structured("sealed.event")
    status, response = await _post(app, headers, body)
    assert status == 403
    assert response["error_type"] == "Forbidden"
    assert response["retryable"] is False

    headers, body = _structured("sealed.event", capabilitytoken="cap:channel_pub:sealed.event:bad")
    status, _ = await _post(app, headers, body)
    assert status == 403

    # a targeted event is checked against "type:instance" and its seal scopes
    headers, body = _structured("sealed.event", eddainstanceid="some-instance")
    status, _ = await _post(app, headers, body)
    assert status == 403

    await asyncio.sleep(0.2)
    assert await app.storage.channel_messages_since("sealed.event", None) == []


async def test_a_valid_token_is_accepted_and_not_stored_with_the_message(ingress_app):
    app = ingress_app
    await app.storage.grant_capability(
        "cap-ingress", "channel_pub", "sealed.event", _hash("tok"), granted_by="x"
    )
    token = "cap:channel_pub:sealed.event:tok"

    headers, body = _structured("sealed.event", capabilitytoken=token, extra="kept")
    status, _ = await _post(app, headers, body)
    assert status == 202
    (message,) = await _stored(app, "sealed.event")
    assert message["metadata"]["ce_extensions"] == {"extra": "kept"}

    # binary mode: header ce-capabilitytoken
    headers = {
        "content-type": "application/json",
        "ce-specversion": "1.0",
        "ce-type": "sealed.event",
        "ce-source": "phase-b",
        "ce-id": "binary-1",
        "ce-capabilitytoken": token,
    }
    status, _ = await _post(app, headers, json.dumps({"n": 2}).encode())
    assert status == 202
    for _ in range(50):
        messages = await app.storage.channel_messages_since("sealed.event", None)
        if len(messages) == 2:
            break
        await asyncio.sleep(0.02)
    assert len(messages) == 2
    assert "capabilitytoken" not in json.dumps(messages, default=str)


async def test_an_open_channel_needs_no_token(ingress_app):
    headers, body = _structured("open.event")
    status, response = await _post(ingress_app, headers, body)
    assert status == 202
    assert response == {"status": "accepted"}
    assert len(await _stored(ingress_app, "open.event")) == 1


# ---------------------------------------------------------------------------
# S8: cursor reads
# ---------------------------------------------------------------------------


def _data(message):
    data = message["data"]
    return json.loads(data) if isinstance(data, str) else data


async def _seed(storage, channel, count=3):
    for n in range(count):
        await publish(storage, channel, {"n": n})
    return await storage.channel_messages_since(channel, None)


async def test_since_with_an_unknown_cursor_fails_and_after_works_from_ids(sqlite_storage):
    rows = await _seed(sqlite_storage, "cursor-ch")
    assert [_data(r)["n"] for r in rows] == [0, 1, 2]
    assert all(isinstance(r["id"], int) and r["message_id"] for r in rows)
    assert rows[0]["id"] < rows[1]["id"] < rows[2]["id"]

    assert [
        _data(r)["n"] for r in await sqlite_storage.channel_messages_since("cursor-ch", "")
    ] == [
        0,
        1,
        2,
    ]
    since = await sqlite_storage.channel_messages_since("cursor-ch", rows[0]["message_id"])
    assert [_data(r)["n"] for r in since] == [1, 2]
    with pytest.raises(CursorNotFoundError):
        await sqlite_storage.channel_messages_since("cursor-ch", "no-such-message")

    # a message id from another channel is not a cursor here
    other = await _seed(sqlite_storage, "cursor-other", 1)
    with pytest.raises(CursorNotFoundError):
        await sqlite_storage.channel_messages_since("cursor-ch", other[0]["message_id"])

    assert len(await sqlite_storage.channel_messages_after("cursor-ch", 0)) == 3
    after = await sqlite_storage.channel_messages_after("cursor-ch", rows[1]["id"])
    assert [_data(r)["n"] for r in after] == [2]
    assert all(r["id"] and r["message_id"] for r in after)
    assert await sqlite_storage.channel_messages_after("cursor-ch", rows[2]["id"]) == []


async def test_after_survives_a_deleted_cursor_message_since_does_not(sqlite_storage):
    rows = await _seed(sqlite_storage, "cursor-deleted")
    await sqlite_storage.delete_channel_message(rows[1]["message_id"])

    with pytest.raises(CursorNotFoundError):
        await sqlite_storage.channel_messages_since("cursor-deleted", rows[1]["message_id"])
    after = await sqlite_storage.channel_messages_after("cursor-deleted", rows[1]["id"])
    assert [_data(r)["n"] for r in after] == [2]


async def test_read_channel_after_is_a_sealed_recorded_step(sqlite_storage):
    engine = _engine(sqlite_storage)
    rows = await _seed(sqlite_storage, "cursor-step")
    await sqlite_storage.grant_capability(
        "cap-read", "channel_sub", "cursor-sealed", _hash("rd"), granted_by="x"
    )
    calls = 0
    original = sqlite_storage.channel_messages_after

    async def counting(channel, after_id):
        nonlocal calls
        calls += 1
        return await original(channel, after_id)

    sqlite_storage.channel_messages_after = counting  # type: ignore[method-assign]

    async def reading(ctx: WorkflowContext) -> dict:
        got = await tasks.read_channel_after(ctx, "cursor-step", rows[0]["id"])
        try:
            await tasks.read_channel_after(ctx, "cursor-sealed", 0)
            sealed = "read"
        except ChannelPermissionError:
            sealed = "forbidden"
        try:
            await tasks.read_channel_since(ctx, "cursor-step", "missing")
            missing = "read"
        except CursorNotFoundError:
            missing = "cursor_not_found"
        await sleep(ctx, 1)
        return {"ns": [_data(m)["n"] for m in got], "sealed": sealed, "missing": missing}

    instance_id = await engine.start_workflow("phase_b_read_after", reading, {})
    history = await sqlite_storage.get_history(instance_id)
    codes = [h["event_data"].get("code") for h in history if h["event_type"] == "ActivityFailed"]
    assert codes == ["channel_forbidden", "cursor_not_found"]
    assert calls == 1

    await _fire_timer_and_resume(sqlite_storage, engine, instance_id, reading)
    assert calls == 1
    assert await _result(sqlite_storage, instance_id) == {
        "ns": [1, 2],
        "sealed": "forbidden",
        "missing": "cursor_not_found",
    }


# ---------------------------------------------------------------------------
# K4: direct channels are private
# ---------------------------------------------------------------------------


async def test_another_instances_direct_channel_is_private(sqlite_storage, create_test_instance):
    engine = _engine(sqlite_storage)
    await create_test_instance("phase-b-victim", "wf", {})
    await create_test_instance("pb:nested", "wf", {})

    async def snooping(ctx: WorkflowContext) -> dict:
        outcome = {}

        async def attempt(label, call):
            try:
                await call()
                outcome[label] = "ok"
            except ChannelPermissionError:
                outcome[label] = "forbidden"

        victim = "orders:phase-b-victim"
        await attempt("subscribe", lambda: subscribe(ctx, victim))
        await attempt("receive", lambda: receive(ctx, victim, timeout_seconds=1))
        await attempt("since", lambda: tasks.read_channel_since(ctx, victim))
        await attempt("after", lambda: tasks.read_channel_after(ctx, victim, 0))
        # instance ids may contain ':': every split point is checked
        await attempt("nested", lambda: subscribe(ctx, "orders:pb:nested"))
        await attempt("nested_suffix", lambda: subscribe(ctx, "orders:x:pb:nested"))
        # own channel, a suffix that is no instance id
        await attempt("own", lambda: subscribe(ctx, f"orders:{ctx.instance_id}"))
        await attempt("own_read", lambda: tasks.read_channel_after(ctx, f"o:{ctx.instance_id}", 0))
        await attempt("plain", lambda: subscribe(ctx, "orders:nobody-here"))
        await attempt("direct_mode", lambda: subscribe(ctx, "orders", mode="direct"))
        # sending stays allowed
        await send_to(ctx, "phase-b-victim", {"hi": 1}, channel="orders")
        await publish(ctx, victim, {"hi": 2})
        return outcome

    instance_id = await engine.start_workflow("phase_b_snoop", snooping, {})

    assert await _result(sqlite_storage, instance_id) == {
        "subscribe": "forbidden",
        "receive": "forbidden",
        "since": "forbidden",
        "after": "forbidden",
        "nested": "forbidden",
        "nested_suffix": "forbidden",
        "own": "ok",
        "own_read": "ok",
        "plain": "ok",
        "direct_mode": "ok",
    }
    sent = await sqlite_storage.channel_messages_since("orders:phase-b-victim", None)
    assert [_data(m) for m in sent] == [{"hi": 1}, {"hi": 2}]
    failures = [
        h["event_data"]["code"]
        for h in await sqlite_storage.get_history(instance_id)
        if h["event_type"] == "ActivityFailed"
    ]
    # subscribe (three channels), receive, since, after: all recorded refusals
    assert failures == ["channel_forbidden"] * 6


async def test_a_subscribe_refusal_replays_as_recorded(sqlite_storage, create_test_instance):
    engine = _engine(sqlite_storage)
    await create_test_instance("pb-gone", "wf", {})

    async def subscribing(ctx: WorkflowContext) -> dict:
        try:
            await subscribe(ctx, "orders:pb-gone")
            outcome = "ok"
        except ChannelPermissionError:
            outcome = "forbidden"
        await sleep(ctx, 1)
        return {"outcome": outcome}

    instance_id = await engine.start_workflow("phase_b_subscribe_replay", subscribing, {})
    # The owner is gone: a live re-check would now allow the subscription.
    async with sqlite_storage.engine.begin() as conn:
        await conn.execute(text("DELETE FROM workflow_instances WHERE instance_id = 'pb-gone'"))

    await _fire_timer_and_resume(sqlite_storage, engine, instance_id, subscribing)
    assert await _result(sqlite_storage, instance_id) == {"outcome": "forbidden"}


# ---------------------------------------------------------------------------
# K3: the resumable claim filters on registered workflow names
# ---------------------------------------------------------------------------


async def test_the_claim_takes_only_registered_workflows(sqlite_storage, create_test_instance):
    mine_a = await create_test_instance("phase-b-a", "wf_registered_a", {})
    mine_b = await create_test_instance("phase-b-b", "wf_registered_b", {})
    foreign = await create_test_instance("phase-b-foreign", "wf_unregistered", {})

    claimed = await sqlite_storage.claim_resumable_workflows(
        "claimer", limit=10, workflow_names=["wf_registered_a", "wf_registered_b"]
    )

    assert {r["instance_id"] for r in claimed} == {mine_a, mine_b}
    async with sqlite_storage.engine.begin() as conn:
        locks = dict(
            (
                await conn.execute(text("SELECT instance_id, locked_by FROM workflow_instances"))
            ).all()
        )
    assert locks[foreign] is None
    assert locks[mine_a] == locks[mine_b] == "claimer"

    assert await sqlite_storage.claim_resumable_workflows("c2", workflow_names=[]) == []


# ---------------------------------------------------------------------------
# K5 / K6: "inside an activity body"
# ---------------------------------------------------------------------------


async def test_a_primitive_gathered_next_to_a_running_activity_is_still_recorded(
    sqlite_storage, monkeypatch
):
    engine = _engine(sqlite_storage)
    in_activity = asyncio.Event()
    release = asyncio.Event()
    calls = 0
    original = sqlite_storage.whereis_actor

    async def counting(name):
        nonlocal calls
        calls += 1
        return await original(name)

    monkeypatch.setattr(sqlite_storage, "whereis_actor", counting)

    @activity
    async def slow_activity(ctx: WorkflowContext) -> int:
        in_activity.set()
        await release.wait()
        return 1

    async def gathering(ctx: WorkflowContext) -> dict:
        async def lookup():
            if not ctx.is_replaying:
                await in_activity.wait()  # the activity body is running now
            found = await actors.whereis(ctx, "nobody")
            release.set()
            return found

        await asyncio.gather(slow_activity(ctx), lookup())
        await sleep(ctx, 1)
        return {}

    instance_id = await engine.start_workflow("phase_b_gather", gathering, {})
    history = await sqlite_storage.get_history(instance_id)
    assert "edda.whereis:1" in {h["activity_id"] for h in history}
    assert calls == 1

    await _fire_timer_and_resume(sqlite_storage, engine, instance_id, gathering)
    assert calls == 1


async def test_receive_inside_an_activity_body_is_refused_and_takes_no_step_ids(sqlite_storage):
    engine = _engine(sqlite_storage)

    @activity
    async def receiving_activity(ctx: WorkflowContext) -> dict:
        await receive(ctx, "phase-b-in-activity")
        return {}

    async def workflow(ctx: WorkflowContext) -> dict:
        try:
            await receiving_activity(ctx)
            outcome = "received"
        except TerminalError:
            outcome = "refused"
        found = await actors.whereis(ctx, "nobody")
        await sleep(ctx, 1)
        return {"outcome": outcome, "found": found}

    instance_id = await engine.start_workflow("phase_b_receive_in_activity", workflow, {})
    first = sorted(h["activity_id"] for h in await sqlite_storage.get_history(instance_id))
    assert not any(a.startswith("receive_") for a in first)
    assert "edda.whereis:1" in first

    await _fire_timer_and_resume(sqlite_storage, engine, instance_id, workflow)
    assert await _result(sqlite_storage, instance_id) == {"outcome": "refused", "found": None}
    replayed = sorted(h["activity_id"] for h in await sqlite_storage.get_history(instance_id))
    assert [a for a in replayed if not a.startswith("sleep")] == [
        a for a in first if not a.startswith("sleep")
    ]
