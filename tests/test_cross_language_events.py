"""
CloudEvents across Edda (Python), Romancy (Go) and Shikibu (Ruby).

The endpoint of each runtime stores an event in one form (SEMANTICS §3c): a
channel message whose data is the event's data, with the event's attributes
under the ce_ keys of its metadata. A workflow of another runtime waits for the
event with wait_event. The runtime that stored the event delivers it, since the
delivery covers the waiters of every runtime, and records the receive in its
own form. The waiter's runtime then replays that record (SEMANTICS §3). The
three runtimes share one SQLite file. Romancy and Shikibu take part through the
crosstest CLIs of their repositories: wait-event mode waits for one event and
prints it, and post-event mode sends one through that runtime's endpoint in its
own process.

Requirements: as tests/test_cross_language_supervisor.py.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import shutil
import subprocess
from collections.abc import AsyncIterator
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pytest
import pytest_asyncio
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

from edda import EddaApp, WorkflowContext, workflow
from edda.channels import wait_event
from edda.storage.migrations import apply_dbmate_migrations

SCHEMA_DIR = Path(__file__).parent.parent / "schema" / "db" / "migrations"
ROMANCY_DIR = Path(__file__).parent.parent.parent / "romancy"
SHIKIBU_DIR = Path(__file__).parent.parent.parent / "shikibu"

requires_go = pytest.mark.skipif(
    shutil.which("go") is None or not (ROMANCY_DIR / "cmd" / "crosstest" / "main.go").exists(),
    reason="Go or the Romancy repository (../romancy) is not available",
)
requires_ruby = pytest.mark.skipif(
    shutil.which("bundle") is None or not (SHIKIBU_DIR / "bin" / "crosstest").exists(),
    reason="Bundler or the Shikibu repository (../shikibu) is not available",
)

ATTRIBUTES = (
    "specversion",
    "id",
    "source",
    "type",
    "time",
    "subject",
    "datacontenttype",
    "dataschema",
)


@workflow
async def crosstest_wait_event(ctx: WorkflowContext, event_type: str) -> dict[str, Any]:
    """Wait for one event of a type, as the wait-event mode of the CLIs does."""
    event = await wait_event(ctx, event_type)
    return {
        "data": event.data,
        **{name: getattr(event, name) for name in ATTRIBUTES},
        "extensions": event.extensions,
    }


def an_event(event_type: str, sent_by: str) -> dict[str, Any]:
    """A structured CloudEvent with every attribute. Its data is not an
    object: replaying another runtime's record of it needs the record to
    say that it is JSON."""
    return {
        "specversion": "1.0",
        "id": f"{sent_by}-1",
        "source": f"crosstest/{sent_by}",
        "type": event_type,
        "time": "2026-11-28T01:02:03.456789Z",
        "subject": "order-9",
        "datacontenttype": "application/json",
        "dataschema": "https://example.com/order.json",
        "region": "eu",
        "data": [1, 2, {"n": 3}],
    }


def received(event: dict[str, Any]) -> dict[str, Any]:
    """What wait_event returns for the event in every runtime."""
    return {
        "data": event["data"],
        **{name: event[name] for name in ATTRIBUTES},
        "extensions": {"region": "eu"},
    }


@pytest.fixture(scope="module")
def go_binary(tmp_path_factory) -> Path:
    """Build the Romancy crosstest binary once per module."""
    binary = tmp_path_factory.mktemp("go_build") / "crosstest"
    result = subprocess.run(
        ["go", "build", "-o", str(binary), "./cmd/crosstest"],
        cwd=ROMANCY_DIR,
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        pytest.fail(f"Failed to build the Romancy crosstest binary: {result.stderr}")
    return binary


@pytest_asyncio.fixture
async def cross_db(tmp_path) -> Path:
    """A migrated SQLite file that the runtimes share."""
    db_path = tmp_path / "cross_events.db"
    engine = create_async_engine(f"sqlite+aiosqlite:///{db_path}")
    await apply_dbmate_migrations(engine, SCHEMA_DIR)
    await engine.dispose()
    return db_path


@pytest_asyncio.fixture
async def edda_app(cross_db) -> AsyncIterator[EddaApp]:
    """An Edda app on the shared file. Only the tests that Edda takes part in
    start one: its own deliveries would stand in for the other runtimes'."""
    app = EddaApp(service_name="crosstest-events", db_url=f"sqlite:///{cross_db}")
    await app.initialize()
    yield app
    await app.shutdown()


def go_cmd(binary: Path, db: Path, mode: str, *args: str) -> list[str]:
    return [str(binary), f"--db={db}", f"--mode={mode}", *args]


def ruby_cmd(db: Path, mode: str, *args: str) -> list[str]:
    crosstest = str(SHIKIBU_DIR / "bin" / "crosstest")
    return ["bundle", "exec", "ruby", crosstest, f"--db={db}", f"--mode={mode}", *args]


def printed_json(stdout: str) -> dict[str, Any]:
    out: dict[str, Any] = json.loads(stdout[stdout.index("{") :])
    return out


async def run_cli(cmd: list[str], cwd: Path | None = None) -> dict[str, Any]:
    """Run a crosstest CLI to its end without blocking the event loop and
    return what it printed."""
    result = await asyncio.to_thread(
        subprocess.run, cmd, cwd=cwd, capture_output=True, text=True, timeout=180
    )
    if result.returncode != 0:
        raise RuntimeError(f"crosstest failed ({result.returncode}): {result.stderr[-3000:]}")
    return printed_json(result.stdout)


@dataclass
class Waiter:
    """A crosstest CLI in wait-event mode."""

    proc: subprocess.Popen[str]

    def check_running(self) -> None:
        if self.proc.poll() is not None:
            _, stderr = self.proc.communicate()
            raise RuntimeError(f"waiter ended ({self.proc.returncode}): {stderr[-3000:]}")

    async def printed(self) -> dict[str, Any]:
        stdout, stderr = await asyncio.to_thread(self.proc.communicate, timeout=120)
        if self.proc.returncode != 0:
            raise RuntimeError(f"waiter failed ({self.proc.returncode}): {stderr[-3000:]}")
        return printed_json(stdout)


@contextlib.asynccontextmanager
async def waiter_cli(cmd: list[str], cwd: Path | None = None) -> AsyncIterator[Waiter]:
    """Run a crosstest CLI in wait-event mode while the block runs."""
    proc = subprocess.Popen(
        cmd,
        cwd=cwd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        start_new_session=True,
    )
    try:
        yield Waiter(proc)
    finally:
        if proc.poll() is None:
            proc.kill()
            await asyncio.to_thread(proc.communicate)


async def wait_for_waiter(
    db: Path, framework: str, event_type: str, waiter: Waiter | None = None
) -> None:
    """Wait until a crosstest_wait_event instance that `framework` runs waits
    for event_type."""
    engine = create_async_engine(f"sqlite+aiosqlite:///{db}")
    query = text(
        """
        SELECT wi.instance_id
        FROM workflow_instances wi
        JOIN channel_subscriptions cs ON cs.instance_id = wi.instance_id
        WHERE wi.workflow_name = 'crosstest_wait_event'
          AND wi.framework = :framework
          AND wi.status = 'waiting_for_message'
          AND cs.channel = :channel
          AND cs.activity_id IS NOT NULL
        """
    )
    loop = asyncio.get_running_loop()
    deadline = loop.time() + 90
    try:
        while loop.time() < deadline:
            async with engine.connect() as conn:
                found = (
                    await conn.execute(query, {"framework": framework, "channel": event_type})
                ).first()
            if found:
                return
            if waiter:
                waiter.check_running()
            await asyncio.sleep(0.1)
    finally:
        await engine.dispose()
    raise AssertionError(f"no {framework} instance waits for {event_type}")


async def post_to_edda(app: EddaApp, event: dict[str, Any]) -> None:
    """Send a structured CloudEvent through Edda's endpoint and wait for the
    deliveries it starts."""
    body = json.dumps(event).encode()
    answer: dict[str, Any] = {}

    async def receive() -> dict[str, Any]:
        return {"type": "http.request", "body": body, "more_body": False}

    async def send(message: dict[str, Any]) -> None:
        if message["type"] == "http.response.start":
            answer["status"] = message["status"]

    scope = {
        "type": "http",
        "method": "POST",
        "path": "/",
        "query_string": b"",
        "headers": [(b"content-type", b"application/cloudevents+json")],
    }
    await app(scope, receive, send)
    assert answer["status"] == 202
    await asyncio.gather(*list(app._event_tasks))


async def edda_waits(cross_db: Path, event_type: str) -> str:
    instance_id = await crosstest_wait_event.start(event_type=event_type)
    await wait_for_waiter(cross_db, "python", event_type)
    return instance_id


async def edda_received(app: EddaApp, instance_id: str) -> Any:
    """Resume what is runnable until instance_id completes; its result."""
    loop = asyncio.get_running_loop()
    deadline = loop.time() + 90
    while loop.time() < deadline:
        instance = await app.storage.get_instance(instance_id)
        assert instance is not None
        if instance["status"] == "completed":
            return instance["output_data"]["result"]
        await app._resume_running_workflows()
        await asyncio.sleep(0.1)
    raise AssertionError(f"{instance_id} did not complete")


def go_received(out: dict[str, Any]) -> Any:
    assert out["received"] is True, out
    return out["event"]


def ruby_received(out: dict[str, Any]) -> Any:
    assert out["received"] is True, out
    return {"data": out["data"], **out["cloudevent"]}


# Edda's endpoint


@requires_go
async def test_go_receives_an_event_that_edda_stored(go_binary, cross_db, edda_app):
    event = an_event("crosstest.edda_to_go", "edda")
    async with waiter_cli(
        go_cmd(go_binary, cross_db, "wait-event", f"--event-type={event['type']}", "--timeout=90")
    ) as waiter:
        await wait_for_waiter(cross_db, "go", event["type"], waiter)
        await post_to_edda(edda_app, event)
        out = await waiter.printed()
    assert go_received(out) == received(event)


@requires_ruby
async def test_ruby_receives_an_event_that_edda_stored(cross_db, edda_app):
    event = an_event("crosstest.edda_to_ruby", "edda")
    async with waiter_cli(
        ruby_cmd(cross_db, "wait-event", f"--event-type={event['type']}", "--timeout=90"),
        cwd=SHIKIBU_DIR,
    ) as waiter:
        await wait_for_waiter(cross_db, "ruby", event["type"], waiter)
        await post_to_edda(edda_app, event)
        out = await waiter.printed()
    assert ruby_received(out) == received(event)


# Romancy's endpoint


@requires_go
async def test_edda_receives_an_event_that_romancy_stored(go_binary, cross_db, edda_app):
    event = an_event("crosstest.romancy_to_edda", "romancy")
    instance_id = await edda_waits(cross_db, event["type"])
    out = await run_cli(go_cmd(go_binary, cross_db, "post-event", f"--event={json.dumps(event)}"))
    assert out == {"status": 202}
    assert await edda_received(edda_app, instance_id) == received(event)


@requires_go
@requires_ruby
async def test_ruby_receives_an_event_that_romancy_stored(go_binary, cross_db):
    event = an_event("crosstest.romancy_to_ruby", "romancy")
    async with waiter_cli(
        ruby_cmd(cross_db, "wait-event", f"--event-type={event['type']}", "--timeout=90"),
        cwd=SHIKIBU_DIR,
    ) as waiter:
        await wait_for_waiter(cross_db, "ruby", event["type"], waiter)
        posted = await run_cli(
            go_cmd(go_binary, cross_db, "post-event", f"--event={json.dumps(event)}")
        )
        assert posted == {"status": 202}
        out = await waiter.printed()
    assert ruby_received(out) == received(event)


# Shikibu's endpoint


@requires_ruby
async def test_edda_receives_an_event_that_shikibu_stored(cross_db, edda_app):
    event = an_event("crosstest.shikibu_to_edda", "shikibu")
    instance_id = await edda_waits(cross_db, event["type"])
    out = await run_cli(
        ruby_cmd(cross_db, "post-event", f"--event={json.dumps(event)}", "--timeout=60"),
        cwd=SHIKIBU_DIR,
    )
    assert out == {"status": 202}
    assert await edda_received(edda_app, instance_id) == received(event)


@requires_go
@requires_ruby
async def test_go_receives_an_event_that_shikibu_stored(go_binary, cross_db):
    event = an_event("crosstest.shikibu_to_go", "shikibu")
    async with waiter_cli(
        go_cmd(go_binary, cross_db, "wait-event", f"--event-type={event['type']}", "--timeout=90")
    ) as waiter:
        await wait_for_waiter(cross_db, "go", event["type"], waiter)
        posted = await run_cli(
            ruby_cmd(cross_db, "post-event", f"--event={json.dumps(event)}", "--timeout=60"),
            cwd=SHIKIBU_DIR,
        )
        assert posted == {"status": 202}
        out = await waiter.printed()
    assert go_received(out) == received(event)
