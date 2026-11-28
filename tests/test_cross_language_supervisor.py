"""
Cross-language supervision between Edda (Python), Romancy (Go) and Shikibu (Ruby).

A supervisor of each runtime starts one child that another runtime runs
(SEMANTICS §7): the child row carries the child's framework, an empty source
hash and, without an input, that runtime's empty input (null for Go). The
three runtimes share one SQLite file. Romancy and Shikibu take part through
the crosstest CLIs of their repositories: supervisor mode starts the
supervisor and prints how it and its child ended, worker mode runs the child
workflows until it is stopped.

Requirements:
- Go, and the Romancy repository at ../romancy (cmd/crosstest)
- Ruby with Bundler, and the Shikibu repository at ../shikibu (bin/crosstest,
  with its gems installed and its schema submodule checked out)
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import shutil
import subprocess
from collections.abc import AsyncIterator
from pathlib import Path
from typing import Any

import pytest
import pytest_asyncio
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

from edda import EddaApp, WorkflowContext, workflow
from edda.storage.migrations import apply_dbmate_migrations
from edda.supervisor import ChildSpec, RestartMode, define_supervisor

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


@workflow
async def crosstest_py_echo(ctx: WorkflowContext, msg: str = "") -> dict[str, Any]:
    """The child that a Romancy or Shikibu supervisor starts."""
    return {"echo": msg, "by": "edda"}


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
    """A migrated SQLite file that the three runtimes share."""
    db_path = tmp_path / "cross_supervision.db"
    engine = create_async_engine(f"sqlite+aiosqlite:///{db_path}")
    await apply_dbmate_migrations(engine, SCHEMA_DIR)
    await engine.dispose()
    return db_path


@pytest_asyncio.fixture
async def edda_app(cross_db) -> AsyncIterator[EddaApp]:
    """An Edda worker on the shared file (it runs crosstest_py_echo)."""
    app = EddaApp(service_name="crosstest-supervision", db_url=f"sqlite:///{cross_db}")
    await app.initialize()
    yield app
    await app.shutdown()


def go_cmd(binary: Path, db: Path, mode: str, *args: str) -> list[str]:
    return [str(binary), f"--db={db}", f"--mode={mode}", *args]


def ruby_cmd(db: Path, mode: str, *args: str) -> list[str]:
    crosstest = str(SHIKIBU_DIR / "bin" / "crosstest")
    return ["bundle", "exec", "ruby", crosstest, f"--db={db}", f"--mode={mode}", *args]


@contextlib.asynccontextmanager
async def crosstest_worker(
    cmd: list[str], log_path: Path, cwd: Path | None = None
) -> AsyncIterator[None]:
    """Run a crosstest CLI in worker mode while the block runs."""
    with log_path.open("w") as log:
        proc = subprocess.Popen(
            cmd, cwd=cwd, stdout=subprocess.PIPE, stderr=log, text=True, start_new_session=True
        )
        try:
            assert proc.stdout is not None
            line = await asyncio.wait_for(asyncio.to_thread(proc.stdout.readline), timeout=120)
            if "ready" not in line:
                raise RuntimeError(f"worker did not start ({line!r}): {log_path.read_text()}")
            yield
        finally:
            if proc.poll() is None:
                proc.terminate()
            try:
                await asyncio.to_thread(proc.wait, 30)
            except subprocess.TimeoutExpired:
                proc.kill()
                await asyncio.to_thread(proc.wait)


async def run_supervisor_cli(cmd: list[str], cwd: Path | None = None) -> dict[str, Any]:
    """Run a crosstest CLI in supervisor mode without blocking the event loop
    (an Edda child runs on it meanwhile) and return what it printed."""
    result = await asyncio.to_thread(
        subprocess.run, cmd, cwd=cwd, capture_output=True, text=True, timeout=180
    )
    if result.returncode != 0:
        raise RuntimeError(f"supervisor failed ({result.returncode}): {result.stderr[-3000:]}")
    out: dict[str, Any] = json.loads(result.stdout[result.stdout.index("{") :])
    return out


async def supervise_from_edda(
    app: EddaApp, name: str, framework: str, workflow_name: str, input_data: Any
) -> dict[str, Any]:
    """Start an Edda supervisor with one child that `framework` runs and
    return how both ended, in the shape the crosstest CLIs print."""
    sup = define_supervisor(
        name,
        max_restarts=3,
        within_seconds=60,
        poll_seconds=1,
        children=[
            ChildSpec(
                name="child",
                workflow=workflow_name,
                framework=framework,
                input=input_data,
                restart=RestartMode.TRANSIENT,
            )
        ],
    )
    sup_id = await sup.start()
    loop = asyncio.get_running_loop()
    deadline = loop.time() + 150
    while loop.time() < deadline:
        inst = await app.storage.get_instance(sup_id)
        if inst and inst["status"] in ("completed", "failed", "cancelled"):
            child_id = f"{sup_id}:child:g1"
            child = await app.storage.get_instance(child_id)
            assert child is not None
            async with app.storage.engine.begin() as conn:
                row = await conn.execute(
                    text("SELECT framework FROM workflow_instances WHERE instance_id = :id"),
                    {"id": child_id},
                )
                child_framework = row.scalar_one()
            return {
                "supervisor_status": inst["status"],
                "child_instance_id": child_id,
                "child_framework": child_framework,
                "child_status": child["status"],
                "child_input": child["input_data"],
                "child_output": child["output_data"],
            }
        await asyncio.sleep(0.2)
    raise AssertionError(f"supervisor {sup_id} did not end")


def assert_child_ran(
    out: dict[str, Any], framework: str, child_input: Any, child_output: Any
) -> None:
    assert out["supervisor_status"] == "completed", out
    assert out["child_framework"] == framework, out
    assert out["child_status"] == "completed", out
    assert out["child_input"] == child_input, out
    assert out["child_output"] == child_output, out


# Edda supervisors


@requires_go
async def test_edda_supervises_a_go_child(go_binary, cross_db, edda_app, tmp_path):
    async with crosstest_worker(
        go_cmd(go_binary, cross_db, "worker", "--timeout=600"), tmp_path / "go-worker.log"
    ):
        out = await supervise_from_edda(
            edda_app, "crosstest-edda-sup-go", "go", "crosstest_go_echo", {"msg": "from edda"}
        )
    assert_child_ran(out, "go", {"msg": "from edda"}, {"echo": "from edda", "by": "romancy"})


@requires_go
async def test_edda_supervises_a_go_child_without_input(go_binary, cross_db, edda_app, tmp_path):
    """A Go child without an input gets null, the zero value of any Go input
    type ({} would not decode into a string)."""
    async with crosstest_worker(
        go_cmd(go_binary, cross_db, "worker", "--timeout=600"), tmp_path / "go-worker.log"
    ):
        out = await supervise_from_edda(
            edda_app, "crosstest-edda-sup-go-noinput", "go", "crosstest_go_string", None
        )
    assert_child_ran(out, "go", None, 'romancy got ""')


@requires_ruby
async def test_edda_supervises_a_ruby_child(cross_db, edda_app, tmp_path):
    async with crosstest_worker(
        ruby_cmd(cross_db, "worker", "--timeout=600"), tmp_path / "ruby-worker.log", cwd=SHIKIBU_DIR
    ):
        out = await supervise_from_edda(
            edda_app, "crosstest-edda-sup-ruby", "ruby", "crosstest_rb_echo", {"msg": "from edda"}
        )
    assert_child_ran(out, "ruby", {"msg": "from edda"}, {"echo": "from edda", "by": "shikibu"})


# Romancy supervisors


@requires_go
async def test_romancy_supervises_a_python_child(go_binary, cross_db, edda_app):
    out = await run_supervisor_cli(
        go_cmd(
            go_binary,
            cross_db,
            "supervisor",
            "--child-framework=python",
            "--child-workflow=crosstest_py_echo",
            '--child-input={"msg": "from romancy"}',
            "--timeout=150",
        )
    )
    assert_child_ran(
        out, "python", {"msg": "from romancy"}, {"result": {"echo": "from romancy", "by": "edda"}}
    )


@requires_go
@requires_ruby
async def test_romancy_supervises_a_ruby_child(go_binary, cross_db, tmp_path):
    async with crosstest_worker(
        ruby_cmd(cross_db, "worker", "--timeout=600"), tmp_path / "ruby-worker.log", cwd=SHIKIBU_DIR
    ):
        out = await run_supervisor_cli(
            go_cmd(
                go_binary,
                cross_db,
                "supervisor",
                "--child-framework=ruby",
                "--child-workflow=crosstest_rb_echo",
                '--child-input={"msg": "from romancy"}',
                "--timeout=150",
            )
        )
    assert_child_ran(
        out, "ruby", {"msg": "from romancy"}, {"echo": "from romancy", "by": "shikibu"}
    )


# Shikibu supervisors


@requires_ruby
async def test_shikibu_supervises_a_python_child(cross_db, edda_app):
    out = await run_supervisor_cli(
        ruby_cmd(
            cross_db,
            "supervisor",
            "--child-framework=python",
            "--child-workflow=crosstest_py_echo",
            '--child-input={"msg": "from shikibu"}',
            "--timeout=150",
        ),
        cwd=SHIKIBU_DIR,
    )
    assert_child_ran(
        out, "python", {"msg": "from shikibu"}, {"result": {"echo": "from shikibu", "by": "edda"}}
    )


@requires_go
@requires_ruby
async def test_shikibu_supervises_a_go_child(go_binary, cross_db, tmp_path):
    async with crosstest_worker(
        go_cmd(go_binary, cross_db, "worker", "--timeout=600"), tmp_path / "go-worker.log"
    ):
        out = await run_supervisor_cli(
            ruby_cmd(
                cross_db,
                "supervisor",
                "--child-framework=go",
                "--child-workflow=crosstest_go_echo",
                '--child-input={"msg": "from shikibu"}',
                "--timeout=150",
            ),
            cwd=SHIKIBU_DIR,
        )
    assert_child_ran(out, "go", {"msg": "from shikibu"}, {"echo": "from shikibu", "by": "romancy"})


@requires_go
@requires_ruby
async def test_shikibu_supervises_a_go_child_without_input(go_binary, cross_db, tmp_path):
    async with crosstest_worker(
        go_cmd(go_binary, cross_db, "worker", "--timeout=600"), tmp_path / "go-worker.log"
    ):
        out = await run_supervisor_cli(
            ruby_cmd(
                cross_db,
                "supervisor",
                "--child-framework=go",
                "--child-workflow=crosstest_go_string",
                "--timeout=150",
            ),
            cwd=SHIKIBU_DIR,
        )
    assert_child_ran(out, "go", None, 'romancy got ""')
