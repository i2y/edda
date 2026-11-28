"""
Accepting incoming CloudEvents (SEMANTICS §3c).

The endpoint answers 202 only once the event is stored: its channel message,
and an instance of each workflow that handles its type. The rest runs in
background tasks that shutdown() waits for. An event that cannot be stored
gets a retryable 500, and an app that shuts down answers a retryable 503.
"""

import asyncio
import json
from typing import Any

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

from edda import EddaApp, WorkflowContext, workflow
from edda.locking import LockNotAcquiredError
from edda.replay import ReplayEngine
from edda.storage.sqlalchemy_storage import SQLAlchemyStorage


@workflow(event_handler=True)
async def accepted_order(ctx: WorkflowContext, order_id: str) -> dict[str, Any]:
    return {"order_id": order_id}


async def post_event(app: EddaApp, event_type: str, data: Any) -> tuple[int, dict[str, Any]]:
    """POST a structured-mode CloudEvent to the app; returns status and body."""
    body = json.dumps(
        {"specversion": "1.0", "type": event_type, "source": "test", "id": "e1", "data": data}
    ).encode()
    response: dict[str, Any] = {}

    async def receive() -> dict[str, Any]:
        return {"type": "http.request", "body": body, "more_body": False}

    async def send(message: dict[str, Any]) -> None:
        if message["type"] == "http.response.start":
            response["status"] = message["status"]
        elif message["type"] == "http.response.body":
            response["body"] = json.loads(message["body"])

    scope = {
        "type": "http",
        "method": "POST",
        "path": "/",
        "query_string": b"",
        "headers": [(b"content-type", b"application/cloudevents+json")],
    }
    await app(scope, receive, send)
    return response["status"], response["body"]


async def count(app: EddaApp, sql: str, value: str) -> int:
    async with app.storage.engine.connect() as conn:
        return int((await conn.execute(text(sql), {"v": value})).scalar_one())


async def channel_messages(app: EddaApp, channel: str) -> int:
    return await count(app, "SELECT COUNT(*) FROM channel_messages WHERE channel = :v", channel)


async def instances(app: EddaApp, workflow_name: str) -> int:
    return await count(
        app, "SELECT COUNT(*) FROM workflow_instances WHERE workflow_name = :v", workflow_name
    )


async def wait_status(app: EddaApp, instance_id: str, status: str) -> None:
    for _ in range(100):
        instance = await app.storage.get_instance(instance_id)
        if instance is not None and instance["status"] == status:
            return
        await asyncio.sleep(0.05)
    raise AssertionError(f"{instance_id} did not reach {status}")


@pytest.fixture
async def app(tmp_path):
    app = EddaApp(service_name="acceptance", db_url=f"sqlite:///{tmp_path / 'events.db'}")
    await app.initialize()
    yield app
    if app._initialized:
        await app.shutdown()


async def test_event_is_stored_before_the_answer(app):
    status, body = await post_event(app, "accepted_order", {"order_id": "o-1"})
    assert (status, body) == (202, {"status": "accepted"})
    # Checked before the background work runs: both are stored already.
    assert await channel_messages(app, "accepted_order") == 1
    assert await instances(app, "accepted_order") == 1


@pytest.mark.parametrize("failing", ["publish_to_channel", "create_instance"])
async def test_event_that_cannot_be_stored_gets_retryable_500(app, monkeypatch, failing):
    async def unreachable(*_args: Any, **_kwargs: Any) -> Any:
        raise RuntimeError("database unreachable")

    monkeypatch.setattr(app.storage, failing, unreachable)
    status, body = await post_event(app, "accepted_order", {"order_id": "o-1"})
    assert status == 500
    assert body["retryable"] is True
    assert "database unreachable" in body["error"]


async def test_instance_whose_first_run_is_cut_short_is_resumed(app):
    status, _ = await post_event(app, "accepted_order", {"order_id": "o-2"})
    assert status == 202
    # Cut the background work short, as a crash or a shutdown would.
    tasks = list(app._event_tasks)
    for task in tasks:
        task.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)

    rows = await app.storage.list_instances(workflow_name_filter="accepted_order")
    [instance] = rows["instances"]
    await app._resume_running_workflows()
    await wait_status(app, instance["instance_id"], "completed")


async def test_shutdown_waits_for_the_work_of_accepted_events(app):
    started, release = asyncio.Event(), asyncio.Event()
    finished: list[str] = []

    @app.on_event("slow.event")
    async def slow(event: Any) -> None:
        started.set()
        await release.wait()
        await app.storage.get_instance("none")  # storage is still open
        finished.append(event["id"])

    status, _ = await post_event(app, "slow.event", {})
    assert status == 202
    await asyncio.wait_for(started.wait(), 5)

    shutdown = asyncio.create_task(app.shutdown())
    await asyncio.sleep(0.2)
    assert not shutdown.done(), "shutdown did not wait for the accepted event"

    status, body = await post_event(app, "slow.event", {})
    assert status == 503
    assert body["error_type"] == "ServiceUnavailable"
    assert body["retryable"] is True

    release.set()
    await asyncio.wait_for(shutdown, 5)
    assert finished == ["e1"]


async def test_shutdown_cancels_event_work_after_the_grace_period(app):
    app._event_shutdown_grace = 0.1
    started, cancelled = asyncio.Event(), asyncio.Event()

    @app.on_event("stuck.event")
    async def stuck(event: Any) -> None:
        started.set()
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            cancelled.set()
            raise

    status, _ = await post_event(app, "stuck.event", {})
    assert status == 202
    await asyncio.wait_for(started.wait(), 5)

    await asyncio.wait_for(app.shutdown(), 5)
    assert cancelled.is_set()


async def test_first_run_leaves_a_claimed_instance_to_its_claimant(tmp_path):
    storage = SQLAlchemyStorage(create_async_engine(f"sqlite+aiosqlite:///{tmp_path / 'r.db'}"))
    await storage.initialize()
    try:
        engine = ReplayEngine(storage=storage, service_name="acceptance", worker_id="w")
        inputs = {"order_id": "o-3"}
        instance_id = await engine.create_workflow_instance(
            "accepted_order", accepted_order.func, inputs
        )
        # A resumption poll claimed the new instance first.
        assert await storage.try_acquire_lock(instance_id, "poll#1", 300)

        ran = await engine.run_new_workflow(
            instance_id, "accepted_order", accepted_order.func, inputs, claimed_ok=True
        )
        assert ran == instance_id
        with pytest.raises(LockNotAcquiredError):
            await engine.run_new_workflow(
                instance_id, "accepted_order", accepted_order.func, inputs
            )
        instance = await storage.get_instance(instance_id)
        assert instance is not None and instance["status"] == "running"
        assert instance["locked_by"] == "poll#1"
    finally:
        await storage.close()


async def post_request(
    app: EddaApp, headers: list[tuple[bytes, bytes]], body: bytes
) -> tuple[int, dict[str, Any]]:
    """POST a request to the app; returns status and body."""
    response: dict[str, Any] = {}

    async def receive() -> dict[str, Any]:
        return {"type": "http.request", "body": body, "more_body": False}

    async def send(message: dict[str, Any]) -> None:
        if message["type"] == "http.response.start":
            response["status"] = message["status"]
        elif message["type"] == "http.response.body":
            response["body"] = json.loads(message["body"])

    scope = {"type": "http", "method": "POST", "path": "/", "query_string": b"", "headers": headers}
    await app(scope, receive, send)
    return response["status"], response["body"]


REQUIRED = {"specversion": "1.0", "id": "e1", "source": "test", "type": "accepted_order"}


@pytest.mark.parametrize("missing", list(REQUIRED))
async def test_a_structured_event_without_a_required_attribute_is_refused(app, missing):
    # The SDK would fill in a missing id, and a retried event would look new.
    event = {name: value for name, value in REQUIRED.items() if name != missing}
    event["data"] = {"order_id": "o-1"}
    status, body = await post_request(
        app, [(b"content-type", b"application/cloudevents+json")], json.dumps(event).encode()
    )
    assert status == 400
    assert body["retryable"] is False
    assert missing in body["error"]
    assert await channel_messages(app, "accepted_order") == 0
    assert await instances(app, "accepted_order") == 0


@pytest.mark.parametrize("missing", list(REQUIRED))
async def test_a_binary_event_without_a_required_attribute_is_refused(app, missing):
    headers = [(b"content-type", b"application/json")] + [
        (f"ce-{name}".encode(), value.encode())
        for name, value in REQUIRED.items()
        if name != missing
    ]
    status, body = await post_request(app, headers, b'{"order_id": "o-1"}')
    assert status == 400
    assert body["retryable"] is False
    assert await channel_messages(app, "accepted_order") == 0


async def test_a_binary_event_is_accepted(app):
    headers = [(b"content-type", b"application/json")] + [
        (f"ce-{name}".encode(), value.encode()) for name, value in REQUIRED.items()
    ]
    status, _ = await post_request(app, headers, b'{"order_id": "o-1"}')
    assert status == 202
    assert await channel_messages(app, "accepted_order") == 1
    assert await instances(app, "accepted_order") == 1
