"""Run an external program as a sandboxed, subprocess-backed Edda activity.

A *port* launches a semi-trusted external program - an AI agent CLI, an MCP
server - behind two walls:

1. **The OS process boundary.** Always present, never forgettable. The program
   gets its own address space and its own process group, so a single ``killpg``
   ends it and everything it spawned.
2. **OS-enforced isolation** via `sandbox-runtime`_ (macOS Seatbelt, Linux
   bubblewrap + seccomp), restricting the filesystem and the network. Requested
   through a :class:`~edda.agent.sandbox.SandboxPolicy`.

The second wall can be missing on a given machine. The first cannot. That is the
point: a forgotten sandbox caps the blast radius instead of removing it.

A port *is* an activity. Its result is recorded in the workflow history, so a
replay never re-runs the program, and the activity's retry policy applies
unchanged - which is what lets a plain ``RetryPolicy`` absorb rate limits.

Example:
    >>> from edda import workflow, WorkflowContext
    >>> from edda.agent import port, DEFAULT_BROAD
    >>>
    >>> claude = port("claude -p --output-format json", sandbox=DEFAULT_BROAD)
    >>>
    >>> @workflow
    ... async def issue_driven(ctx: WorkflowContext, spec: str) -> str:
    ...     result = await claude(ctx, stdin=spec)
    ...     return result.stdout

.. _sandbox-runtime: https://github.com/anthropic-experimental/sandbox-runtime
"""

from __future__ import annotations

import asyncio
import base64
import contextlib
import contextvars
import dataclasses
import json
import logging
import os
import re
import shlex
import signal
import sys
import time
import uuid
from collections.abc import Awaitable, Callable, Mapping, Sequence
from datetime import UTC, datetime
from typing import Any, Final, cast

from pydantic import BaseModel

from edda.activity import activity
from edda.agent.errors import (
    PortConfigError,
    PortError,
    PortFailedError,
    PortTimeoutError,
    SandboxUnavailableError,
)
from edda.agent.sandbox import (
    DEFAULT_BROAD,
    SandboxPolicy,
    UnsandboxedType,
    check_srt_shape,
)
from edda.context import WorkflowContext
from edda.retry import RetryPolicy

__all__ = ["EXTERN_EVENT_TYPE", "EXTERN_TAG", "PortActivity", "PortResult", "port"]

logger = logging.getLogger(__name__)

#: The sandbox backend this build speaks. Sent in the runner spec so that an old
#: runner rejects a spec from a newer edda instead of misinterpreting it. A future
#: backend (e.g. Apple's ``container``) adds a value here, a config renderer on
#: :class:`~edda.agent.sandbox.SandboxPolicy`, and a branch in the runner.
_BACKEND: Final[str] = "srt"

_RUNNER_MODULE: Final[str] = "edda.agent._runner"

#: ``event_type`` of the raw-I/O history rows a port writes.
EXTERN_EVENT_TYPE: Final[str] = "ExternRecord"

#: Marks a record as coming from beyond the process boundary: we saw the bytes,
#: we did not interpret them.
EXTERN_TAG: Final[str] = "extern"

_DEFAULT_MAX_OUTPUT_BYTES: Final[int] = 10 * 1024 * 1024
_DEFAULT_TIMEOUT_SECONDS: Final[float] = 300.0

#: Grace between ``SIGTERM`` and ``SIGKILL`` when tearing down the process group.
#: Not a parameter: nobody has needed to tune it, and the API stays small.
_TERM_GRACE_SECONDS: Final[float] = 5.0
_KILL_WAIT_SECONDS: Final[float] = 5.0

#: How long to wait for the runner to exit after it reported its result.
_RUNNER_EXIT_GRACE_SECONDS: Final[float] = 10.0

#: The runner's own deadline sits this far beyond ours; it only ever fires if we
#: died without killing it.
_BELT_MARGIN_SECONDS: Final[float] = 30.0

#: An ``o``/``e`` event is a 64 KiB chunk base64-encoded, so ~87 KiB plus the
#: envelope. ``readline`` must not choke on that.
_PARENT_STREAM_LIMIT: Final[int] = 1 << 20

_FEED_CHUNK_BYTES: Final[int] = 65536
_RUNNER_STDERR_TAIL_BYTES: Final[int] = 8192
_ERROR_TAIL_CHARS: Final[int] = 2048

#: Durable raw-I/O rows are an audit trail, not the payload: the payload is
#: ``PortResult.stdout``. Keep them bounded.
_EXTERN_ROW_BYTES: Final[int] = 32 * 1024
_EXTERN_STREAM_BYTES: Final[int] = 64 * 1024

#: The activity id of the port currently executing, published by
#: :class:`PortActivity` so the activity body can label its extern records.
#: ``Activity`` resolves the id but does not pass it to the function body.
_ACTIVITY_ID: contextvars.ContextVar[str] = contextvars.ContextVar(
    "edda_agent_port_activity_id", default=""
)

_ActivityCallable = Callable[..., Awaitable["PortResult"]]


class PortResult(BaseModel):
    """What an external program left behind.

    Recorded in the workflow history and returned verbatim on replay.

    Attributes:
        exit_code: Exit status. Negative values are ``-signal``.
        stdout: Standard output, decoded as UTF-8 with replacement.
        stderr: Standard error, decoded as UTF-8 with replacement.
        sandboxed: Whether OS-level isolation was actually in force. Reported by
            the runner - what happened, not what was asked for.
        duration_ms: Wall-clock time for the whole call, including runner start-up.
        truncated: Whether output exceeded ``max_output_bytes`` and was cut.
    """

    exit_code: int
    stdout: str
    stderr: str
    sandboxed: bool
    duration_ms: int
    truncated: bool


@dataclasses.dataclass(frozen=True, slots=True)
class _PortConfig:
    """Everything ``port()`` resolved at definition time."""

    name: str
    command: str
    sandbox: SandboxPolicy | UnsandboxedType | dict[str, Any]
    timeout: float
    env: dict[str, str]
    cwd: str | None
    check: bool
    max_output_bytes: int


# --------------------------------------------------------------------------- #
# Process-group teardown
# --------------------------------------------------------------------------- #


def _signal_pg(proc: asyncio.subprocess.Process, sig: int) -> None:
    """Signal the runner's whole process group, if it is still alive.

    The runner is a session leader (``start_new_session=True``) and the external
    program joins its group, so one signal reaches the sandbox wrapper, the
    shell, the program, and anything the program spawned.
    """
    if proc.returncode is not None:
        return
    with contextlib.suppress(ProcessLookupError, PermissionError):
        os.killpg(proc.pid, sig)


async def _kill_tree(proc: asyncio.subprocess.Process, *, term_first: bool) -> None:
    """Tear down the runner's process group.

    ``SIGKILL`` escalation is mandatory, not defensive: once ``sandbox-runtime``
    is initialised it installs a ``SIGTERM`` handler that schedules a cleanup and
    does *not* exit, so the runner survives ``SIGTERM`` on its own. The external
    program does die from it, which is why we still send ``SIGTERM`` first when we
    can afford to - it lets the program flush and clean up.
    """
    if proc.returncode is not None:
        return
    if term_first:
        _signal_pg(proc, signal.SIGTERM)
        with contextlib.suppress(TimeoutError):
            await asyncio.wait_for(asyncio.shield(proc.wait()), _TERM_GRACE_SECONDS)
            return
    _signal_pg(proc, signal.SIGKILL)
    with contextlib.suppress(TimeoutError):
        await asyncio.wait_for(asyncio.shield(proc.wait()), _KILL_WAIT_SECONDS)


# --------------------------------------------------------------------------- #
# Runner I/O
# --------------------------------------------------------------------------- #


async def _feed(proc: asyncio.subprocess.Process, spec_line: bytes, payload: bytes) -> None:
    """Send the spec line, then the program's stdin, then close the pipe.

    Broken-pipe errors are swallowed: the runner died, and the reader loop is in
    a far better position to say why.
    """
    stdin = proc.stdin
    if stdin is None:  # pragma: no cover - stdin is always a pipe
        return
    try:
        stdin.write(spec_line)
        await stdin.drain()
        for offset in range(0, len(payload), _FEED_CHUNK_BYTES):
            stdin.write(payload[offset : offset + _FEED_CHUNK_BYTES])
            await stdin.drain()
    except (BrokenPipeError, ConnectionResetError):
        pass
    finally:
        with contextlib.suppress(Exception):
            stdin.close()


async def _tail(stream: asyncio.StreamReader, sink: bytearray, cap: int) -> None:
    """Accumulate the tail of ``stream`` into ``sink``.

    ``sink`` is caller-owned so that a cancelled tail still yields what it read.
    """
    while True:
        chunk = await stream.read(4096)
        if not chunk:
            break
        sink += chunk
        if len(sink) > cap:
            del sink[:-cap]


def _tail_text(data: bytes | bytearray, chars: int = _ERROR_TAIL_CHARS) -> str:
    """Decode ``data`` and keep its last ``chars`` characters."""
    text = bytes(data).decode("utf-8", errors="replace")
    return text[-chars:]


# --------------------------------------------------------------------------- #
# Durable raw-I/O records
# --------------------------------------------------------------------------- #


def _supports_isolated_write(storage: Any) -> bool:
    """Whether extern records may be committed outside the attempt transaction.

    The activity body runs inside the retry attempt's transaction, so a failed
    attempt rolls its history writes back - and a failed attempt is exactly when
    the raw log matters most. Writing from a fresh :class:`contextvars.Context`
    gives ``append_history`` its own session, which commits immediately.

    That trick is unsafe on an in-memory SQLite engine, where a ``StaticPool``
    hands every session the *same* connection and the second session's commit
    would end the attempt's open transaction. There we stay in-context and accept
    that a failed attempt loses its extern rows.
    """
    engine = getattr(storage, "engine", None)
    url = getattr(engine, "url", None)
    if url is None:
        return False
    try:
        if url.get_backend_name() != "sqlite":
            return True
        database = url.database
    except Exception:  # pragma: no cover - defensive against exotic URLs
        return False
    return bool(database) and database != ":memory:"


def _extern_rows(
    token: str, meta: dict[str, Any], streams: Sequence[tuple[str, bytes]]
) -> list[tuple[int, dict[str, Any]]]:
    """Build the history rows for one attempt's raw I/O.

    The payload bytes are base64 in a self-describing JSON row: not parsed, not
    interpreted, just kept.
    """
    rows: list[tuple[int, dict[str, Any]]] = [(0, meta)]
    seq = 1
    for name, data in streams:
        if not data:
            continue
        capped = data[:_EXTERN_STREAM_BYTES]
        for offset in range(0, len(capped), _EXTERN_ROW_BYTES):
            block = capped[offset : offset + _EXTERN_ROW_BYTES]
            rows.append(
                (
                    seq,
                    {
                        "tag": EXTERN_TAG,
                        "stream": name,
                        "token": token,
                        "seq": seq,
                        "data_b64": base64.b64encode(block).decode("ascii"),
                        "truncated": len(data) > _EXTERN_STREAM_BYTES,
                    },
                )
            )
            seq += 1
    return rows


async def _write_extern(
    ctx: WorkflowContext,
    activity_id: str,
    token: str,
    rows: Sequence[tuple[int, dict[str, Any]]],
) -> None:
    """Persist raw-I/O rows, surviving a rolled-back retry attempt if possible.

    Best-effort: a failure here is logged and never masks the port's own result
    or exception.
    """

    async def _append() -> None:
        for seq, payload in rows:
            await ctx.storage.append_history(
                ctx.instance_id,
                f"{activity_id}:extern:{token}:{seq:03d}",
                EXTERN_EVENT_TYPE,
                payload,
            )

    try:
        if _supports_isolated_write(ctx.storage):
            await asyncio.create_task(_append(), context=contextvars.Context())
        else:
            await _append()
    except Exception as exc:
        logger.warning("could not record extern log for %s: %s", activity_id, exc)


# --------------------------------------------------------------------------- #
# Execution
# --------------------------------------------------------------------------- #


def _build_spec(cfg: _PortConfig, command: str, cwd: str) -> dict[str, Any]:
    """Render the JSON spec handed to the runner on its stdin."""
    sandbox_config: dict[str, Any] | None
    if isinstance(cfg.sandbox, UnsandboxedType):
        sandbox_config = None
    elif isinstance(cfg.sandbox, SandboxPolicy):
        sandbox_config = cfg.sandbox.to_srt_config(cwd)
    else:
        sandbox_config = cfg.sandbox
    unsandboxed = sandbox_config is None

    return {
        "v": 1,
        "backend": _BACKEND,
        "command": command,
        "cwd": cwd,
        "env": dict(cfg.env),
        "unsandboxed": unsandboxed,
        "sandbox_config": sandbox_config,
        "timeout_belt_seconds": cfg.timeout + _BELT_MARGIN_SECONDS,
        "max_output_bytes": cfg.max_output_bytes,
        "collect_violations": not unsandboxed and sys.platform == "darwin",
    }


def _raise_fatal(kind: str, message: str) -> None:
    """Translate a runner ``fatal`` event into the right exception.

    Raises:
        SandboxUnavailableError: The sandbox could not be established.
        PortConfigError: The spec or sandbox configuration is wrong.
        PortTimeoutError: The runner's own backstop deadline fired.
        PortError: Anything else.
    """
    if kind == "sandbox_unavailable":
        raise SandboxUnavailableError(message)
    if kind in ("config_invalid", "unknown_backend"):
        raise PortConfigError(message)
    if kind == "belt_timeout":
        raise PortTimeoutError(message)
    raise PortError(message)


async def _execute_port(
    ctx: WorkflowContext,
    activity_id: str,
    cfg: _PortConfig,
    stdin_bytes: bytes,
    extra_args: list[str] | None,
) -> PortResult:
    """Run one attempt: spawn the runner, relay I/O, record, and report."""
    if os.name != "posix":
        raise PortConfigError(
            f"edda.agent.port needs POSIX process groups; {sys.platform!r} is not supported"
        )
    if not sys.executable:
        raise PortConfigError("sys.executable is empty; cannot spawn the port runner")

    cwd = os.path.realpath(cfg.cwd) if cfg.cwd else os.getcwd()
    command = cfg.command
    if extra_args:
        command = f"{command} {shlex.join(extra_args)}"

    spec = _build_spec(cfg, command, cwd)
    if spec["unsandboxed"]:
        logger.warning(
            "port %r (%s/%s) is running WITHOUT OS sandbox enforcement " "(process isolation only)",
            cfg.name,
            ctx.instance_id,
            activity_id,
        )

    token = uuid.uuid4().hex[:8]
    started_at = datetime.now(UTC)
    started = time.monotonic()

    proc = await asyncio.create_subprocess_exec(
        sys.executable,
        "-m",
        _RUNNER_MODULE,
        stdin=asyncio.subprocess.PIPE,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
        start_new_session=True,
        limit=_PARENT_STREAM_LIMIT,
    )
    assert proc.stdout is not None
    assert proc.stderr is not None

    spec_line = json.dumps(spec, separators=(",", ":")).encode("utf-8") + b"\n"
    runner_stderr = bytearray()
    feed = asyncio.create_task(_feed(proc, spec_line, stdin_bytes))
    tail = asyncio.create_task(_tail(proc.stderr, runner_stderr, _RUNNER_STDERR_TAIL_BYTES))

    stdout_buf = bytearray()
    stderr_buf = bytearray()
    child_pid: int | None = None
    sandboxed = False
    srt_version: str | None = None
    violations: list[str] = []
    exit_event: dict[str, Any] | None = None
    fatal_event: dict[str, Any] | None = None
    timed_out = False

    try:
        try:
            async with asyncio.timeout(cfg.timeout):
                while True:
                    try:
                        line = await proc.stdout.readline()
                    except ValueError:
                        # A single NDJSON line exceeded the read limit. The runner
                        # bounds every event, so this is a protocol violation, not
                        # normal output; treat it as a fatal runner error.
                        fatal_event = {
                            "kind": "internal",
                            "error": "runner emitted a line over the read limit",
                        }
                        await _kill_tree(proc, term_first=True)
                        break
                    if not line:
                        break
                    try:
                        event = json.loads(line)
                    except json.JSONDecodeError:
                        continue  # Forward compatibility: ignore what we cannot read.
                    kind = event.get("t")
                    if kind == "start":
                        child_pid = event.get("pid")
                        sandboxed = bool(event.get("sandboxed"))
                        srt_version = event.get("srt")
                        logger.info(
                            "port %r started: instance=%s activity=%s token=%s "
                            "runner_pid=%s child_pid=%s sandboxed=%s",
                            cfg.name,
                            ctx.instance_id,
                            activity_id,
                            token,
                            proc.pid,
                            child_pid,
                            sandboxed,
                        )
                    elif kind in ("o", "e"):
                        payload = event.get("d")
                        if not isinstance(payload, str):
                            continue
                        try:
                            chunk = base64.b64decode(payload)
                        except ValueError:
                            continue
                        buf = stdout_buf if kind == "o" else stderr_buf
                        room = cfg.max_output_bytes - len(buf)
                        if room > 0:
                            buf += chunk[:room]
                        logger.debug(
                            "extern %s %s:%s %s | %s",
                            ctx.instance_id,
                            activity_id,
                            token,
                            "stdout" if kind == "o" else "stderr",
                            chunk.decode("utf-8", errors="replace"),
                        )
                    elif kind == "violations":
                        violations = [str(item) for item in event.get("lines") or []]
                    elif kind == "exit":
                        exit_event = event
                        break
                    elif kind == "fatal":
                        fatal_event = event
                        break

                if exit_event is not None or fatal_event is not None:
                    try:
                        await asyncio.wait_for(proc.wait(), _RUNNER_EXIT_GRACE_SECONDS)
                    except TimeoutError:
                        # The runner reported, then hung (srt teardown). Not our problem.
                        await _kill_tree(proc, term_first=True)
                else:
                    # EOF with no terminal event: the runner died abnormally
                    # (SIGKILL, OOM, a native crash in srt). Its own belt and
                    # orphan-watch died with it, so we must take down the group.
                    await _kill_tree(proc, term_first=False)
        except TimeoutError:
            timed_out = True
            await _kill_tree(proc, term_first=True)
        except asyncio.CancelledError:
            # Promptness wins over tidiness; durability lives in the history.
            _signal_pg(proc, signal.SIGKILL)
            with contextlib.suppress(BaseException):
                await asyncio.wait_for(asyncio.shield(proc.wait()), _KILL_WAIT_SECONDS)
            raise
    finally:
        feed.cancel()
        tail.cancel()
        with contextlib.suppress(BaseException):
            await asyncio.gather(feed, tail, return_exceptions=True)
        # Sweep any grandchild that outlived the child and still holds the group
        # open. The runner (the group leader) is gone on every path by now, so the
        # pgid is held only by such stragglers - nothing the port started outlives it.
        with contextlib.suppress(ProcessLookupError, PermissionError):
            os.killpg(proc.pid, signal.SIGKILL)

    duration_ms = int((time.monotonic() - started) * 1000)
    stdout_text = bytes(stdout_buf).decode("utf-8", errors="replace")
    stderr_text = bytes(stderr_buf).decode("utf-8", errors="replace")
    runner_tail = _tail_text(runner_stderr)

    # A real child result outranks a teardown that overran the deadline, and a
    # reported fatal reason outranks giving up while waiting for the runner.
    if exit_event is not None:
        outcome = "exit"
    elif fatal_event is not None:
        outcome = f"fatal:{fatal_event.get('kind')}"
    elif timed_out:
        outcome = "timeout"
    else:
        outcome = "no_result"

    dropped_out = int(exit_event.get("stdout_dropped", 0)) if exit_event else 0
    dropped_err = int(exit_event.get("stderr_dropped", 0)) if exit_event else 0

    meta: dict[str, Any] = {
        "tag": EXTERN_TAG,
        "stream": "meta",
        "token": token,
        "seq": 0,
        "name": cfg.name,
        "command": command,
        "cwd": cwd,
        "sandboxed": sandboxed,
        "unsandboxed_requested": bool(spec["unsandboxed"]),
        "srt_version": srt_version,
        "runner_pid": proc.pid,
        "child_pid": child_pid,
        "outcome": outcome,
        "exit_code": exit_event.get("code") if exit_event else None,
        "duration_ms": duration_ms,
        "child_duration_ms": exit_event.get("duration_ms") if exit_event else None,
        "stdout_bytes": int(exit_event.get("stdout_bytes", 0)) if exit_event else len(stdout_buf),
        "stderr_bytes": int(exit_event.get("stderr_bytes", 0)) if exit_event else len(stderr_buf),
        "stdout_dropped": dropped_out,
        "stderr_dropped": dropped_err,
        "violations": violations,
        # Names only. Never the values.
        "env_keys": sorted(cfg.env),
        "started_at": started_at.isoformat(),
        "ended_at": datetime.now(UTC).isoformat(),
    }
    await _write_extern(
        ctx,
        activity_id,
        token,
        _extern_rows(
            token,
            meta,
            [("stdin", stdin_bytes), ("stdout", bytes(stdout_buf)), ("stderr", bytes(stderr_buf))],
        ),
    )

    logger.info(
        "port %r finished: instance=%s activity=%s token=%s outcome=%s duration_ms=%d",
        cfg.name,
        ctx.instance_id,
        activity_id,
        token,
        outcome,
        duration_ms,
    )

    detail = _failure_detail(stderr_text, runner_tail, violations)

    if exit_event is not None:
        raw_code = exit_event.get("code")
        if not isinstance(raw_code, int):
            raise PortError(
                f"port {cfg.name!r} runner reported an invalid exit code {raw_code!r}{detail}"
            )
        result = PortResult(
            exit_code=raw_code,
            stdout=stdout_text,
            stderr=stderr_text,
            sandboxed=sandboxed,
            duration_ms=duration_ms,
            truncated=bool(dropped_out or dropped_err),
        )
        if cfg.check and raw_code != 0:
            raise PortFailedError(
                f"port {cfg.name!r} exited with {raw_code}{detail}",
                exit_code=raw_code,
                stdout_tail=stdout_text[-_ERROR_TAIL_CHARS:],
                stderr_tail=stderr_text[-_ERROR_TAIL_CHARS:],
            )
        return result

    if fatal_event is not None:
        _raise_fatal(str(fatal_event.get("kind")), f"{fatal_event.get('error')}{detail}")
    if timed_out:
        raise PortTimeoutError(f"port {cfg.name!r} timed out after {cfg.timeout}s{detail}")
    raise PortError(
        f"port {cfg.name!r} runner exited without a result "
        f"(returncode={proc.returncode}){detail}"
    )


def _failure_detail(stderr_text: str, runner_tail: str, violations: Sequence[str]) -> str:
    """Assemble the diagnostic suffix appended to every port exception message."""
    parts: list[str] = []
    if stderr_text.strip():
        parts.append(f"stderr: {stderr_text[-_ERROR_TAIL_CHARS:]}")
    if runner_tail.strip():
        parts.append(f"runner stderr: {runner_tail}")
    if violations:
        parts.append("sandbox violations:\n" + "\n".join(violations))
    return ("\n" + "\n".join(parts)) if parts else ""


# --------------------------------------------------------------------------- #
# Public surface
# --------------------------------------------------------------------------- #


class PortActivity:
    """The callable returned by :func:`port`.

    A thin shell around an Edda activity. It exists because ``bytes`` cannot pass
    through the activity machinery: the framework serialises an activity's kwargs
    into the history on *both* the success and the failure path, and ``json.dumps``
    rejects raw bytes. So stdin is normalised to text or base64 here, before the
    activity ever sees it.
    """

    __slots__ = ("_activity", "_command", "_name")

    def __init__(self, inner: _ActivityCallable, *, name: str, command: str) -> None:
        self._activity = inner
        self._name = name
        self._command = command

    @property
    def name(self) -> str:
        """The activity name, which also prefixes the generated activity ids."""
        return self._name

    @property
    def command(self) -> str:
        """The shell command this port runs."""
        return self._command

    def __repr__(self) -> str:
        """Return a debug representation."""
        return f"<PortActivity {self._name!r} command={self._command!r}>"

    async def __call__(
        self,
        ctx: WorkflowContext,
        stdin: str | bytes | None = None,
        args: Sequence[str] | None = None,
        activity_id: str | None = None,
    ) -> PortResult:
        """Run the external program once, as an activity of ``ctx``.

        Args:
            ctx: The workflow context.
            stdin: Data to pipe to the program. The pipe is closed afterwards, so
                a program that reads until EOF terminates normally. **This is
                recorded durably in the workflow history** (both in the activity's
                input and, capped, in an ``extern`` row) so the run can be
                audited and replayed. Do not pass a secret you would not want
                persisted; unlike ``env`` values, stdin is not redacted.
            args: Extra arguments appended to the command, quoted safely. Also
                recorded in the history as activity input.
            activity_id: Explicit activity id. Required when several ports run
                concurrently under ``asyncio.gather``, exactly as for any activity.

        Returns:
            The recorded :class:`PortResult`. On replay this comes from the
            history and the program is not run again.

        Raises:
            TypeError: If ``stdin`` or ``args`` have the wrong type.
            PortFailedError: Non-zero exit while ``check=True``.
            PortTimeoutError: The program overran its timeout.
            SandboxUnavailableError: Sandboxing was requested but is unavailable.
            PortConfigError: The configuration or platform is unusable.
            PortError: The runner failed to produce a result.
        """
        kwargs: dict[str, Any] = {}
        if stdin is not None:
            if isinstance(stdin, str):
                kwargs["stdin_text"] = stdin
            elif isinstance(stdin, bytes):
                kwargs["stdin_b64"] = base64.b64encode(stdin).decode("ascii")
            else:
                raise TypeError(f"stdin must be str, bytes or None, got {type(stdin).__name__}")
        if args is not None:
            arg_list = list(args)
            for item in arg_list:
                if not isinstance(item, str):
                    raise TypeError(f"args must be strings, got {type(item).__name__}")
            kwargs["args"] = arg_list

        # Resolve the id here rather than letting Activity do it, so the body can
        # label its extern records with it. The counter advances in workflow order,
        # so a replay resolves the same ids.
        resolved = activity_id if activity_id is not None else ctx._generate_activity_id(self._name)

        reset = _ACTIVITY_ID.set(resolved)
        try:
            return await self._activity(ctx, activity_id=resolved, **kwargs)
        finally:
            _ACTIVITY_ID.reset(reset)


def _default_name(command: str) -> str:
    """Derive an activity name from the command's program, e.g. ``claude``."""
    try:
        parts = shlex.split(command)
    except ValueError:
        parts = command.split()
    base = os.path.basename(parts[0]) if parts else "port"
    cleaned = re.sub(r"\W", "_", base) or "port"
    return f"_{cleaned}" if cleaned[0].isdigit() else cleaned


def _normalize_command(command: str | Sequence[str]) -> str:
    """Return a shell command string.

    A ``str`` is used verbatim, so shell features (pipes, redirection) work and
    quoting is the caller's problem. A sequence is joined with :func:`shlex.join`,
    which is the injection-safe path.

    Raises:
        ValueError: If the command is empty.
        TypeError: If a sequence element is not a string.
    """
    if isinstance(command, str):
        if not command.strip():
            raise ValueError("port command must not be empty")
        return command
    parts = list(command)
    if not parts:
        raise ValueError("port command must not be empty")
    for part in parts:
        if not isinstance(part, str):
            raise TypeError(f"command parts must be strings, got {type(part).__name__}")
    return shlex.join(parts)


def _normalize_sandbox(
    sandbox: SandboxPolicy | UnsandboxedType | Mapping[str, Any],
) -> SandboxPolicy | UnsandboxedType | dict[str, Any]:
    """Validate and freeze the sandbox argument at definition time."""
    if isinstance(sandbox, (UnsandboxedType, SandboxPolicy)):
        return sandbox
    if isinstance(sandbox, Mapping):
        return check_srt_shape(sandbox)
    raise TypeError(
        "sandbox must be a SandboxPolicy, UNSANDBOXED, or an srt-shaped mapping, "
        f"got {type(sandbox).__name__}"
    )


def port(
    command: str | Sequence[str],
    *,
    sandbox: SandboxPolicy | UnsandboxedType | Mapping[str, Any] = DEFAULT_BROAD,
    timeout: float = _DEFAULT_TIMEOUT_SECONDS,
    name: str | None = None,
    env: Mapping[str, str] | None = None,
    cwd: str | os.PathLike[str] | None = None,
    check: bool = True,
    max_output_bytes: int = _DEFAULT_MAX_OUTPUT_BYTES,
    retry_policy: RetryPolicy | None = None,
) -> PortActivity:
    """Define an activity that runs an external program in an isolated process.

    Calling ``port(...)`` is pure: nothing is spawned, no sandbox is touched. The
    returned object is an Edda activity, so it may be defined at module level and
    is safe to re-enter during replay.

    Args:
        command: The program to run. A string is a shell command (pipes work); a
            sequence of strings is joined safely with :func:`shlex.join`.
        sandbox: A :class:`~edda.agent.sandbox.SandboxPolicy`, a raw srt-shaped
            mapping, or :data:`~edda.agent.sandbox.UNSANDBOXED` to run with
            process isolation only. Defaults to
            :data:`~edda.agent.sandbox.DEFAULT_BROAD`.
        timeout: Wall-clock seconds for one attempt, runner start-up included.
            On expiry the whole process group is killed and
            :class:`~edda.agent.errors.PortTimeoutError` is raised.
        name: Activity name. Defaults to the program's basename, so
            ``claude -p`` yields activity ids like ``claude:1``.
        env: Environment overrides for the external program. It otherwise
            inherits the worker's environment - an agent CLI needs ``PATH``,
            ``HOME`` and its API key; confinement is the walls' job, not the
            environment's.
        cwd: Working directory of the external program. Defaults to the worker's.
        check: Raise :class:`~edda.agent.errors.PortFailedError` on a non-zero
            exit. Set ``False`` to inspect :class:`PortResult` yourself.
        max_output_bytes: Per-stream cap on captured output.
        retry_policy: Activity retry policy. ``None`` uses the app default.

    Returns:
        A :class:`PortActivity` to ``await`` inside a workflow.

    Raises:
        ValueError: If the command is empty or a numeric argument is out of range.
        TypeError: If an argument has the wrong type.

    Note:
        With the default ``timeout`` of 300s and the default retry policy's
        ``max_duration`` of 300s, one timed-out attempt already exhausts the retry
        budget. Raise ``max_duration`` if timeouts should be retried.

    Note:
        Retryable port errors surface as :class:`~edda.exceptions.RetryExhaustedError`
        once the retries run out, with the original
        :class:`~edda.agent.errors.PortFailedError` or
        :class:`~edda.agent.errors.PortTimeoutError` in ``__cause__``. That is Edda's
        behaviour for every activity. The terminal errors -
        :class:`~edda.agent.errors.SandboxUnavailableError` and
        :class:`~edda.agent.errors.PortConfigError` - are raised directly and never
        retried.

    Note:
        What is persisted to the workflow history: the ``command``, ``args``, the
        program's ``stdin``, and its captured ``stdout``/``stderr`` (capped). Also
        the *names* of any ``env`` keys - never their values, and never the
        program's environment. Treat stdin and output as recorded; keep secrets
        in ``env``.
    """
    resolved_sandbox = _normalize_sandbox(sandbox)
    command_str = _normalize_command(command)

    if timeout <= 0:
        raise ValueError(f"timeout must be positive, got {timeout}")
    if max_output_bytes <= 0:
        raise ValueError(f"max_output_bytes must be positive, got {max_output_bytes}")

    env_map: dict[str, str] = {}
    for key, value in (env or {}).items():
        if not isinstance(key, str) or not isinstance(value, str):
            raise TypeError("env keys and values must be strings")
        env_map[key] = value

    cfg = _PortConfig(
        name=name or _default_name(command_str),
        command=command_str,
        sandbox=resolved_sandbox,
        timeout=timeout,
        env=env_map,
        cwd=os.fspath(cwd) if cwd is not None else None,
        check=check,
        max_output_bytes=max_output_bytes,
    )

    async def _run(
        ctx: WorkflowContext,
        stdin_text: str | None = None,
        stdin_b64: str | None = None,
        args: list[str] | None = None,
    ) -> PortResult:
        if stdin_text is not None:
            stdin_bytes = stdin_text.encode("utf-8")
        elif stdin_b64 is not None:
            stdin_bytes = base64.b64decode(stdin_b64)
        else:
            stdin_bytes = b""
        return await _execute_port(ctx, _ACTIVITY_ID.get(), cfg, stdin_bytes, args)

    _run.__name__ = cfg.name
    _run.__qualname__ = cfg.name
    _run.__doc__ = f"Run the external program {cfg.command!r} in an isolated process."
    # Activity restores a Pydantic result on replay by inspecting the *object* in
    # the return annotation. This module uses postponed annotations, so the literal
    # one would be the string "PortResult". Bind the class explicitly.
    _run.__annotations__["return"] = PortResult

    decorate = cast("Callable[[Any], _ActivityCallable]", activity(retry_policy=retry_policy))
    return PortActivity(decorate(_run), name=cfg.name, command=cfg.command)
