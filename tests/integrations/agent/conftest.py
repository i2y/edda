"""Fixtures for `edda.agent` tests.

Storage is file-backed rather than the shared `:memory:` fixture: a port writes
its raw-I/O rows from a fresh `contextvars.Context` so they survive a rolled-back
retry attempt, and that second session would collide with the single shared
connection a `StaticPool` in-memory engine hands out.
"""

import asyncio
import importlib.util
import os
import shutil
import subprocess
import sys

import pytest
import pytest_asyncio
from sqlalchemy.ext.asyncio import create_async_engine

from edda.context import WorkflowContext
from edda.storage.migrations import apply_dbmate_migrations
from edda.storage.sqlalchemy_storage import SQLAlchemyStorage
from tests.conftest import SCHEMA_DIR

WORKFLOW_NAME = "port_test_workflow"
INSTANCE_ID = "port-test-instance"


def srt_available() -> bool:
    """Whether the runner subprocess would be able to establish a sandbox.

    Probed exactly the way the runner probes it, but without importing
    `sandbox_runtime` into the test process - only the runner may do that.

    On Linux the check is deliberately stricter than the runner's: `bwrap` may be
    installed yet unusable when unprivileged user namespaces are disabled (Ubuntu
    24's AppArmor default). Tests should skip there, not fail.
    """
    if os.environ.get("EDDA_AGENT_FORCE_SANDBOX_UNAVAILABLE") == "1":
        return False
    if importlib.util.find_spec("sandbox_runtime") is None:
        return False
    if sys.platform == "darwin":
        return shutil.which("rg") is not None
    if sys.platform.startswith("linux"):
        if not all(shutil.which(binary) for binary in ("rg", "bwrap", "socat")):
            return False
        try:
            probe = subprocess.run(
                ["bwrap", "--ro-bind", "/", "/", "true"],
                capture_output=True,
                timeout=30,
            )
        except (OSError, subprocess.SubprocessError):
            return False
        return probe.returncode == 0
    return False


needs_srt = pytest.mark.skipif(
    not srt_available(),
    reason="sandbox-runtime or its system dependencies (rg / bwrap / socat) are unavailable",
)

needs_posix = pytest.mark.skipif(os.name != "posix", reason="port relies on POSIX process groups")


@pytest_asyncio.fixture
async def agent_storage(tmp_path):
    """A file-backed SQLite storage with the real migrations applied."""
    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path / 'agent.db'}", echo=False)
    await apply_dbmate_migrations(engine, SCHEMA_DIR)
    storage = SQLAlchemyStorage(engine)
    yield storage
    await storage.close()


@pytest_asyncio.fixture
async def agent_instance(agent_storage):
    """A workflow instance row that port activities can attach history to."""
    await agent_storage.upsert_workflow_definition(
        workflow_name=WORKFLOW_NAME,
        source_hash="port-test-hash",
        source_code=f"async def {WORKFLOW_NAME}(ctx): pass",
    )
    await agent_storage.create_instance(
        instance_id=INSTANCE_ID,
        workflow_name=WORKFLOW_NAME,
        source_hash="port-test-hash",
        owner_service="test-service",
        input_data={},
    )
    return INSTANCE_ID


@pytest.fixture
def ctx(agent_storage, agent_instance):
    """A live `WorkflowContext`, as an activity sees it during a first run."""
    return WorkflowContext(
        instance_id=agent_instance,
        workflow_name=WORKFLOW_NAME,
        storage=agent_storage,
        worker_id="port-test-worker",
    )


async def extern_records(storage, instance_id):
    """Return the raw-I/O rows a port recorded, oldest first."""
    from edda.agent import EXTERN_EVENT_TYPE

    history = await storage.get_history(instance_id)
    return [event for event in history if event["event_type"] == EXTERN_EVENT_TYPE]


async def extern_meta(storage, instance_id):
    """Return the single `meta` extern row for the most recent port attempt."""
    rows = await extern_records(storage, instance_id)
    metas = [row["event_data"] for row in rows if row["event_data"]["stream"] == "meta"]
    assert metas, "port did not record an extern meta row"
    return metas[-1]


async def wait_until_dead(pid, timeout=10.0):
    """Block until `pid` no longer exists, or fail."""
    deadline = asyncio.get_running_loop().time() + timeout
    while asyncio.get_running_loop().time() < deadline:
        try:
            os.kill(pid, 0)
        except ProcessLookupError:
            return
        except PermissionError:  # pragma: no cover - pid reused by another user
            return
        await asyncio.sleep(0.05)
    raise AssertionError(f"process {pid} was still alive after {timeout}s")
