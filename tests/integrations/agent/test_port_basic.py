"""The first wall: a port is a real, killable, replay-safe subprocess.

Everything here runs with `sandbox=UNSANDBOXED`, so it exercises the process
boundary on any POSIX machine - no `sandbox-runtime`, no `ripgrep`, no
`bubblewrap`. The second wall is covered by `test_srt_enforcement.py`.
"""

import asyncio
import base64
import logging
import os
import sys
from unittest import mock

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from edda.agent import (
    DEFAULT_BROAD,
    UNSANDBOXED,
    PortConfigError,
    PortError,
    PortFailedError,
    PortResult,
    PortTimeoutError,
    SandboxUnavailableError,
    port,
)
from edda.exceptions import RetryExhaustedError, TerminalError
from edda.replay import ReplayEngine
from edda.retry import RetryPolicy
from edda.workflow import set_replay_engine, workflow

from .conftest import extern_meta, extern_records, needs_posix, wait_until_dead

# The submodule and the re-exported `port` function share a name, so reach the
# module via sys.modules rather than attribute access on the package.
port_module = sys.modules["edda.agent.port"]

pytestmark = [needs_posix]

ONCE = RetryPolicy(max_attempts=1)


def unsandboxed(command, **kwargs):
    """A port with the second wall explicitly waived, for first-wall tests."""
    return port(command, sandbox=UNSANDBOXED, **kwargs)


class TestBasicExecution:
    async def test_echo_returns_a_recorded_result(self, ctx):
        result = await unsandboxed(["echo", "hello"])(ctx)

        assert isinstance(result, PortResult)
        assert result.exit_code == 0
        assert result.stdout.strip() == "hello"
        assert result.sandboxed is False
        assert result.truncated is False
        assert result.duration_ms >= 0

    async def test_stdin_round_trips_as_text(self, ctx):
        result = await unsandboxed(["cat"])(ctx, stdin="ping\n")
        assert result.stdout == "ping\n"

    async def test_stdin_round_trips_as_bytes(self, ctx):
        """Bytes must reach the program even though history recording is JSON-only."""
        result = await unsandboxed(["cat"])(ctx, stdin=b"\xc3\xa9 raw\n")
        assert result.stdout == "é raw\n"

    async def test_args_are_quoted_safely(self, ctx):
        result = await unsandboxed(["echo"])(ctx, args=["a b", "c;d", "$HOME"])
        assert result.stdout.strip() == "a b c;d $HOME"

    async def test_env_overrides_reach_the_program(self, ctx):
        p = unsandboxed(["sh", "-c", 'printf %s "$EDDA_PORT_PROBE"'], env={"EDDA_PORT_PROBE": "42"})
        assert (await p(ctx)).stdout == "42"

    async def test_cwd_is_honoured(self, ctx, tmp_path):
        (tmp_path / "here.txt").write_text("found")
        result = await unsandboxed(["cat", "here.txt"], cwd=tmp_path)(ctx)
        assert result.stdout == "found"

    async def test_nonzero_exit_raises_when_checking(self, ctx):
        p = unsandboxed(["sh", "-c", "echo bad >&2; exit 3"], retry_policy=ONCE)
        with pytest.raises(RetryExhaustedError) as excinfo:
            await p(ctx)

        cause = excinfo.value.__cause__
        assert isinstance(cause, PortFailedError)
        assert cause.exit_code == 3
        assert "bad" in cause.stderr_tail

    async def test_nonzero_exit_is_returned_when_not_checking(self, ctx):
        p = unsandboxed(["sh", "-c", "exit 3"], check=False)
        result = await p(ctx)
        assert result.exit_code == 3

    async def test_output_over_the_cap_is_truncated(self, ctx):
        p = unsandboxed(["sh", "-c", "yes | head -c 200000"], max_output_bytes=1000)
        result = await p(ctx)
        assert result.truncated is True
        assert len(result.stdout) <= 1000


class TestProcessBoundary:
    async def test_the_program_runs_in_its_own_process(self, ctx, agent_storage):
        """DoD: a port is a real separate OS process, not a thread or a call."""
        result = await unsandboxed([sys.executable, "-c", "import os; print(os.getpid())"])(ctx)
        program_pid = int(result.stdout.strip())

        meta = await extern_meta(agent_storage, ctx.instance_id)
        assert program_pid != os.getpid()
        assert meta["runner_pid"] != os.getpid()
        assert meta["child_pid"] != os.getpid()
        assert meta["runner_pid"] != program_pid

    async def test_timeout_kills_the_whole_process_tree(self, ctx, agent_storage, tmp_path):
        marker = tmp_path / "pid"
        p = unsandboxed(
            ["sh", "-c", f"echo $$ > {marker}; sleep 60"], timeout=1.0, retry_policy=ONCE
        )
        with pytest.raises(RetryExhaustedError) as excinfo:
            await p(ctx)
        assert isinstance(excinfo.value.__cause__, PortTimeoutError)

        meta = await extern_meta(agent_storage, ctx.instance_id)
        assert meta["outcome"] == "timeout"
        await wait_until_dead(meta["runner_pid"])
        await wait_until_dead(meta["child_pid"])
        await wait_until_dead(int(marker.read_text()))

    async def test_cancellation_kills_the_whole_process_tree(self, ctx, tmp_path):
        marker = tmp_path / "pid"
        p = unsandboxed(["sh", "-c", f"echo $$ > {marker}; sleep 60"], timeout=60.0)

        task = asyncio.create_task(p(ctx))
        for _ in range(200):
            await asyncio.sleep(0.05)
            if marker.exists() and marker.read_text().strip():
                break
        else:  # pragma: no cover - the shell always gets there
            pytest.fail("the external program never started")

        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        await wait_until_dead(int(marker.read_text()))

    async def test_concurrent_ports_need_explicit_activity_ids(self, ctx):
        p = unsandboxed(["echo", "concurrent"])
        results = await asyncio.gather(
            p(ctx, activity_id="echo:a"),
            p(ctx, activity_id="echo:b"),
        )
        assert [r.stdout.strip() for r in results] == ["concurrent", "concurrent"]


class TestExternRecords:
    async def test_raw_io_is_recorded_with_the_extern_tag(self, ctx, agent_storage):
        await unsandboxed(["sh", "-c", "cat; echo err >&2"])(ctx, stdin=b"ping\n")

        rows = await extern_records(agent_storage, ctx.instance_id)
        by_stream = {row["event_data"]["stream"]: row["event_data"] for row in rows}

        assert set(by_stream) == {"meta", "stdin", "stdout", "stderr"}
        assert all(row["event_data"]["tag"] == "extern" for row in rows)
        assert base64.b64decode(by_stream["stdin"]["data_b64"]) == b"ping\n"
        assert base64.b64decode(by_stream["stdout"]["data_b64"]) == b"ping\n"
        assert b"err" in base64.b64decode(by_stream["stderr"]["data_b64"])

        # Every row carries a unique activity id, per the history's UNIQUE constraint.
        ids = [row["activity_id"] for row in rows]
        assert len(ids) == len(set(ids))
        assert all(":extern:" in activity_id for activity_id in ids)

    async def test_meta_records_the_environment_names_but_never_the_values(
        self, ctx, agent_storage
    ):
        await unsandboxed(["true"], env={"EDDA_SECRET": "hunter2"})(ctx)
        meta = await extern_meta(agent_storage, ctx.instance_id)

        assert meta["env_keys"] == ["EDDA_SECRET"]
        assert "hunter2" not in str(meta)

    async def test_a_failed_attempt_still_leaves_its_raw_log(self, ctx, agent_storage):
        """The attempt's transaction rolls back; the raw log must not go with it."""
        p = unsandboxed(["sh", "-c", "echo why >&2; exit 1"], retry_policy=ONCE)
        with pytest.raises(RetryExhaustedError):
            await p(ctx)

        rows = await extern_records(agent_storage, ctx.instance_id)
        streams = {row["event_data"]["stream"] for row in rows}
        assert "meta" in streams and "stderr" in streams

        history = await agent_storage.get_history(ctx.instance_id)
        assert any(event["event_type"] == "ActivityFailed" for event in history)

    async def test_a_runner_that_dies_without_a_result_is_reported_cleanly(self, ctx):
        """An abnormal runner death (no exit/fatal event) must be a PortError, not
        some opaque exception, and must not leave the process group running.
        """
        p = unsandboxed(["echo", "hi"], retry_policy=ONCE)
        # Force the runner to die the instant it starts, before emitting anything.
        with (
            mock.patch.object(port_module, "_RUNNER_MODULE", "edda.agent._nonexistent_runner"),
            pytest.raises(RetryExhaustedError) as excinfo,
        ):
            await p(ctx)
        assert isinstance(excinfo.value.__cause__, PortError)


class TestDegradedMode:
    def test_sandbox_unavailable_is_terminal(self):
        """No retry storm: a missing `bwrap` will still be missing next attempt."""
        assert issubclass(SandboxUnavailableError, TerminalError)

    async def test_the_default_policy_refuses_to_run_without_a_sandbox(
        self, ctx, agent_storage, monkeypatch
    ):
        monkeypatch.setenv("EDDA_AGENT_FORCE_SANDBOX_UNAVAILABLE", "1")

        with pytest.raises(SandboxUnavailableError, match=r"edda-framework\[agent\]"):
            await port(["echo", "hi"])(ctx)

        history = await agent_storage.get_history(ctx.instance_id)
        failures = [event for event in history if event["event_type"] == "ActivityFailed"]
        assert len(failures) == 1, "a terminal error must not be retried"
        assert failures[0]["event_data"]["error_type"] == "SandboxUnavailableError"

    async def test_unsandboxed_says_so_loudly(self, ctx, caplog):
        with caplog.at_level(logging.WARNING, logger="edda.agent.port"):
            result = await unsandboxed(["true"])(ctx)

        assert result.sandboxed is False
        assert any("WITHOUT OS sandbox" in record.message for record in caplog.records)


class TestDefinitionTimeValidation:
    def test_port_is_pure_at_definition_time(self, monkeypatch):
        """Defining a port must not touch the sandbox, spawn anything, or fail.

        That is what makes a module-level `claude = port(...)` safe on a machine
        where the sandbox is unavailable, and safe to re-enter during replay.
        (That `sandbox_runtime` never enters this process is proven hermetically
        in `tests/test_agent_import_boundary.py`.)
        """
        monkeypatch.setenv("EDDA_AGENT_FORCE_SANDBOX_UNAVAILABLE", "1")

        defined = port("claude -p --output-format json", sandbox=DEFAULT_BROAD)
        assert defined.name == "claude"
        assert defined.command == "claude -p --output-format json"

    def test_name_defaults_to_the_program_basename(self):
        assert port("/usr/bin/claude -p", sandbox=UNSANDBOXED).name == "claude"
        assert port("7zip x", sandbox=UNSANDBOXED).name == "_7zip"
        assert port("echo", sandbox=UNSANDBOXED, name="custom").name == "custom"

    @pytest.mark.parametrize("bad", ["", "   ", []])
    def test_empty_commands_are_rejected(self, bad):
        with pytest.raises(ValueError, match="must not be empty"):
            port(bad, sandbox=UNSANDBOXED)

    def test_bad_types_are_rejected(self):
        with pytest.raises(TypeError, match="command parts must be strings"):
            port(["echo", 1], sandbox=UNSANDBOXED)
        with pytest.raises(TypeError, match="sandbox must be"):
            port("echo", sandbox="tight")
        with pytest.raises(TypeError, match="env keys and values"):
            port("echo", sandbox=UNSANDBOXED, env={"A": 1})

    @pytest.mark.parametrize("kwargs", [{"timeout": 0}, {"max_output_bytes": 0}])
    def test_non_positive_limits_are_rejected(self, kwargs):
        with pytest.raises(ValueError, match="must be positive"):
            port("echo", sandbox=UNSANDBOXED, **kwargs)

    async def test_stdin_and_args_are_type_checked_before_anything_runs(self, ctx):
        p = unsandboxed(["cat"])
        with pytest.raises(TypeError, match="stdin must be"):
            await p(ctx, stdin=123)
        with pytest.raises(TypeError, match="args must be strings"):
            await p(ctx, args=[1])


class TestReplay:
    async def test_replay_does_not_rerun_the_program(self, agent_storage, tmp_path, caplog):
        """DoD: a port is an activity - its result is history, not a re-execution.

        The execution counter lives in the filesystem, because the thing that must
        not run again lives in another process.
        """
        marker = tmp_path / "runs"
        appender = unsandboxed(["sh", "-c", f"printf x >> {marker}; echo done"])
        passes: list[bool] = []

        @workflow
        async def port_replay_workflow(ctx) -> dict:
            passes.append(ctx.is_replaying)
            result = await appender(ctx)
            return {"stdout": result.stdout.strip()}

        engine = ReplayEngine(
            storage=agent_storage, service_name="test-service", worker_id="worker-1"
        )
        set_replay_engine(engine)

        with caplog.at_level(logging.WARNING, logger="edda.agent.port"):
            instance_id = await port_replay_workflow.start()
            await asyncio.sleep(0.2)

            instance = await agent_storage.get_instance(instance_id)
            assert instance["status"] == "completed"
            assert instance["output_data"]["result"]["stdout"] == "done"
            assert marker.read_bytes() == b"x"

            await _simulate_crash(agent_storage, instance_id)
            stale = await agent_storage.cleanup_stale_locks()
            assert [entry["instance_id"] for entry in stale] == [instance_id]

            await engine.resume_by_name(instance_id, "port_replay_workflow")
            await asyncio.sleep(0.2)

        instance = await agent_storage.get_instance(instance_id)
        assert instance["status"] == "completed"
        assert instance["output_data"]["result"]["stdout"] == "done"

        # The workflow body really did run a second time, as a replay...
        assert passes == [False, True]
        # ...yet the external program did not.
        assert marker.read_bytes() == b"x", "the external program must not run again"
        # And the port activity's body never entered: only the first pass warned.
        warnings = [r for r in caplog.records if "WITHOUT OS sandbox" in r.message]
        assert len(warnings) == 1

    async def test_the_replayed_result_is_a_port_result(self, ctx, agent_storage):
        """The Pydantic model must survive the history round-trip."""
        p = unsandboxed(["echo", "cached"])
        first = await p(ctx)

        replay_ctx = type(ctx)(
            instance_id=ctx.instance_id,
            workflow_name=ctx.workflow_name,
            storage=agent_storage,
            worker_id="worker-2",
            is_replaying=True,
        )
        await replay_ctx._load_history()
        second = await p(replay_ctx)

        assert isinstance(second, PortResult)
        assert second == first


async def _simulate_crash(storage, instance_id):
    """Leave the instance as a crashed worker would: running, with a stale lock."""
    await storage.try_acquire_lock(instance_id, "crashed-worker")
    async with AsyncSession(storage.engine, expire_on_commit=False) as session:
        await session.execute(
            text(
                "UPDATE workflow_instances SET status = 'running', "
                "lock_expires_at = '2000-01-01 00:00:00' WHERE instance_id = :iid"
            ),
            {"iid": instance_id},
        )
        await session.commit()


class TestPlatformGuard:
    async def test_non_posix_is_a_terminal_error(self, ctx, monkeypatch):
        monkeypatch.setattr(os, "name", "nt")
        with pytest.raises(PortConfigError, match="POSIX"):
            await unsandboxed(["echo", "hi"], retry_policy=ONCE)(ctx)
