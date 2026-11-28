"""
The stored form of a CloudEvent (SEMANTICS §3c).

The endpoints of Edda, Romancy and Shikibu store an event as one channel
message: its data is the event's data, and its metadata holds the event's
attributes under ce_ keys. wait_event in every runtime reads them, so an event
that one runtime stored reaches a workflow of another.
"""

import asyncio
import json
from typing import Any

import pytest
from sqlalchemy import text

from edda import EddaApp, WorkflowContext, workflow
from edda.channels import _message_data_from_history, _wake_waiting_subscribers, wait_event


@pytest.fixture
async def app(tmp_path):
    app = EddaApp(service_name="format", db_url=f"sqlite:///{tmp_path / 'format.db'}")
    await app.initialize()
    yield app
    if app._initialized:
        await app.shutdown()


async def post_structured(app: EddaApp, event: dict[str, Any]) -> None:
    """POST a structured-mode CloudEvent and wait for the work it starts."""
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


async def stored_messages(app: EddaApp, channel: str) -> list[tuple[Any, dict[str, Any]]]:
    async with app.storage.engine.begin() as conn:
        rows = await conn.execute(
            text("SELECT data, metadata FROM channel_messages WHERE channel = :c ORDER BY id"),
            {"c": channel},
        )
        return [(json.loads(data), json.loads(metadata)) for data, metadata in rows]


async def status_of(app: EddaApp, instance_id: str) -> str:
    instance = await app.storage.get_instance(instance_id)
    assert instance is not None
    return str(instance["status"])


async def wait_status(app: EddaApp, instance_id: str, status: str) -> None:
    for _ in range(100):
        if await status_of(app, instance_id) == status:
            return
        await asyncio.sleep(0.02)
    raise AssertionError(f"{instance_id} is {await status_of(app, instance_id)}, not {status}")


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
async def format_wait_event(ctx: WorkflowContext, event_type: str) -> dict[str, Any]:
    event = await wait_event(ctx, event_type)
    return {
        "data": event.data,
        "type": event.type,
        "source": event.source,
        "id": event.id,
        "specversion": event.specversion,
        "time": event.time,
        "subject": event.subject,
        "datacontenttype": event.datacontenttype,
        "dataschema": event.dataschema,
        "extensions": event.extensions,
    }


async def test_an_event_is_stored_as_its_data_and_attributes(app):
    await post_structured(
        app,
        {
            "specversion": "1.0",
            "id": "evt-1",
            "source": "shop",
            "type": "order.stored",
            "time": "2026-11-28T10:00:00.5+09:00",
            "subject": "order-1",
            "dataschema": "https://example.com/order.json",
            "datacontenttype": "application/json",
            "region": "eu",
            "capabilitytoken": "not-kept",
            "data": {"n": 7},
        },
    )
    ((data, metadata),) = await stored_messages(app, "order.stored")
    assert data == {"n": 7}
    metadata.pop("published_at")  # Edda's own key
    assert metadata == {
        "ce_specversion": "1.0",
        "ce_id": "evt-1",
        "ce_source": "shop",
        "ce_type": "order.stored",
        "ce_time": "2026-11-28T10:00:00.5+09:00",
        "ce_subject": "order-1",
        "ce_dataschema": "https://example.com/order.json",
        "ce_datacontenttype": "application/json",
        "ce_extensions": {"region": "eu"},
    }

    # Only the required attributes, and no data.
    await post_structured(
        app, {"specversion": "1.0", "id": "evt-2", "source": "shop", "type": "order.bare"}
    )
    ((data, metadata),) = await stored_messages(app, "order.bare")
    assert data is None
    metadata.pop("published_at")
    assert metadata == {
        "ce_specversion": "1.0",
        "ce_id": "evt-2",
        "ce_source": "shop",
        "ce_type": "order.bare",
    }


async def test_wait_event_reads_an_event_another_runtime_stored(app):
    instance_id = await format_wait_event.start(event_type="order.shipped")
    await wait_status(app, instance_id, "waiting_for_message")

    # As Romancy stores it: data of any JSON type, no published_at.
    metadata = {
        "ce_specversion": "1.0",
        "ce_id": "go-1",
        "ce_source": "warehouse",
        "ce_type": "order.shipped",
        "ce_time": "2026-11-28T01:02:03.456789Z",
        "ce_subject": "order-9",
        "ce_datacontenttype": "application/json",
        "ce_dataschema": "https://example.com/s.json",
        "ce_extensions": {"region": "eu"},
    }
    message_id = await app.storage.publish_to_channel("order.shipped", [1, 2, 3], metadata)
    await _wake_waiting_subscribers(
        app.storage, "order.shipped", message_id, [1, 2, 3], metadata, worker_id=app.worker_id
    )

    assert await result_of(app, instance_id) == {
        "data": [1, 2, 3],
        "type": "order.shipped",
        "source": "warehouse",
        "id": "go-1",
        "specversion": "1.0",
        "time": "2026-11-28T01:02:03.456789Z",
        "subject": "order-9",
        "datacontenttype": "application/json",
        "dataschema": "https://example.com/s.json",
        "extensions": {"region": "eu"},
    }


def test_a_receive_that_romancy_recorded_replays_its_data():
    # Romancy's waiter scan delivers to the instances of every runtime and
    # records the receive in its own form, which says that the data is JSON.
    recorded = {
        "id": 7,
        "channel_name": "order.shipped",
        "created_at": "2026-11-28T01:02:03Z",
        "data_type": "json",
        "data": [1, 2, 3],
        "metadata": {"ce_id": "go-1"},
    }
    assert _message_data_from_history(recorded) == [1, 2, 3]
    # Entries without data_type, which Edda before 0.17 wrote, kept only
    # objects and replay as they did.
    del recorded["data_type"]
    assert _message_data_from_history(recorded) == {}
