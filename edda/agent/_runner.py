"""Runner subprocess for :mod:`edda.agent`.

Spawned as ``python -m edda.agent._runner`` by :func:`edda.agent.port.port`, one
throwaway process per port call. This is the **only** place ``sandbox_runtime``
is ever imported, and the import is lazy so an unsandboxed port works without it.

Running srt here rather than in the Edda worker is not a style choice:

- ``SandboxManager`` keeps its configuration and proxy servers in module globals,
  and the network proxy consults those globals per request. Two concurrent ports
  with different domain allowlists cannot both be enforced inside one process.
- ``SandboxManager.initialize`` replaces the process's ``SIGINT``/``SIGTERM``
  handlers and registers an ``atexit`` hook. Its ``SIGTERM`` handler schedules a
  cleanup but does not exit, so an srt-initialised process survives ``SIGTERM``.

A fresh process per call makes the first correct by construction and confines the
second. The parent escalates to ``SIGKILL`` to deal with the third.

Wire protocol
-------------

Parent -> runner, on stdin:
    Line 1 is a UTF-8 JSON spec terminated by ``\\n``. Everything after it is raw
    bytes to be piped to the external program's stdin; the parent closes the pipe
    to signal EOF, which the runner forwards by closing the child's stdin.

Runner -> parent, on stdout, newline-delimited JSON:
    ``{"t": "start", "pid": int, "sandboxed": bool, "srt": str | null}``
        Emitted exactly once, before any output event.
    ``{"t": "o" | "e", "d": str}``
        A base64 chunk of the child's stdout / stderr, in order.
    ``{"t": "violations", "lines": [str]}``
        Optional, at most once, after the child exits (macOS only).
    ``{"t": "exit", "code": int, "duration_ms": int, ...}``
        Terminal, success path. ``code`` is negative for ``-signal``.
    ``{"t": "fatal", "kind": str, "error": str}``
        Terminal, failure path.

    Unknown event types must be ignored by the parent. The runner's own stderr is
    not protocol - it carries free-form diagnostics.

``fatal.kind`` is a closed set: ``sandbox_unavailable``, ``config_invalid``,
``unknown_backend``, ``spawn_failed``, ``belt_timeout``, ``orphaned``, ``internal``.
"""

from __future__ import annotations

import asyncio
import base64
import contextlib
import json
import os
import shutil
import signal
import sys
import time
import traceback
from typing import Any

#: Read size for every pipe. Never use ``readline`` on child output: an agent CLI
#: invoked with ``--output-format json`` emits a single multi-megabyte line.
_CHUNK_BYTES = 65536

#: Upper bound on the spec line. Generous; a sandbox config is a few KiB.
_SPEC_LIMIT_BYTES = 1 << 20

#: srt forces ``TMPDIR`` here for the sandboxed child but never creates it.
_SANDBOX_TMPDIR = "/tmp/claude"

#: Test-support only. Not public API: the availability check runs in this
#: process, so a test cannot monkeypatch it - it injects an environment variable.
_FORCE_UNAVAILABLE_ENV = "EDDA_AGENT_FORCE_SANDBOX_UNAVAILABLE"

#: How long to let macOS's ``log stream`` catch up before reading violations.
#: Violation reporting is advisory and best-effort; a fast failure may outrun it.
_VIOLATION_DRAIN_SECONDS = 0.25

#: How long to keep draining the child's pipes after it exits (grandchildren may
#: still hold them open).
_CHILD_DRAIN_SECONDS = 5.0

#: How often to check whether the Edda worker that spawned us is still alive.
_PARENT_POLL_SECONDS = 1.0

#: Bounds on the ``violations`` event. A filesystem scan tripping thousands of
#: denials could otherwise produce a single NDJSON line larger than the parent's
#: read limit and turn a *successful* run into an opaque read error.
_MAX_VIOLATION_LINES = 100
_MAX_VIOLATION_CHARS = 2048


def _emit(event_type: str, **fields: Any) -> None:
    """Write one NDJSON event to stdout.

    Args:
        event_type: Value of the ``t`` field.
        **fields: Remaining event fields.

    Raises:
        BrokenPipeError: If the parent has gone away.
    """
    line = json.dumps({"t": event_type, **fields}, separators=(",", ":"))
    stream = sys.stdout.buffer
    stream.write(line.encode("utf-8") + b"\n")
    stream.flush()


def _install_hint() -> str:
    """Return a platform-specific hint for making the sandbox available."""
    base = "Install it with: pip install 'edda-framework[agent]'"
    if sys.platform == "darwin":
        return f"{base}, and install ripgrep (brew install ripgrep)."
    if sys.platform.startswith("linux"):
        return f"{base}, and install ripgrep, bubblewrap and socat."
    return f"OS-level sandboxing is not supported on platform {sys.platform!r}."


class _Stream:
    """Byte accounting for one of the child's output streams."""

    __slots__ = ("dropped", "emitted", "total")

    def __init__(self) -> None:
        self.total = 0
        self.emitted = 0
        self.dropped = 0


async def _stdin_reader() -> asyncio.StreamReader:
    """Attach an :class:`asyncio.StreamReader` to this process's stdin."""
    loop = asyncio.get_running_loop()
    reader = asyncio.StreamReader(limit=_SPEC_LIMIT_BYTES)
    protocol = asyncio.StreamReaderProtocol(reader)
    await loop.connect_read_pipe(lambda: protocol, sys.stdin.buffer)
    return reader


async def _pump_stdin(reader: asyncio.StreamReader, child: asyncio.subprocess.Process) -> None:
    """Forward the parent's raw stdin payload to the child, then close its stdin.

    Closing on EOF is what gives the child a clean end-of-input, which is exactly
    what a program reading stdin until EOF (``cat``, ``claude -p``) needs.
    """
    stdin = child.stdin
    if stdin is None:  # pragma: no cover - stdin is always a pipe
        return
    try:
        while True:
            chunk = await reader.read(_CHUNK_BYTES)
            if not chunk:
                break
            stdin.write(chunk)
            await stdin.drain()
    except (BrokenPipeError, ConnectionResetError):
        pass  # The child closed its stdin or died; the exit path reports why.
    finally:
        with contextlib.suppress(Exception):
            stdin.close()


async def _read_stream(
    stream: asyncio.StreamReader, event_type: str, cap: int, acc: _Stream
) -> None:
    """Relay a child output stream as base64 chunks, draining past the cap.

    Draining after the cap is reached is mandatory: if we stopped reading, the
    pipe would fill and the child would block forever on its next write.
    """
    while True:
        chunk = await stream.read(_CHUNK_BYTES)
        if not chunk:
            break
        acc.total += len(chunk)
        room = cap - acc.emitted
        if room > 0:
            part = chunk[:room]
            acc.emitted += len(part)
            acc.dropped += len(chunk) - len(part)
            _emit(event_type, d=base64.b64encode(part).decode("ascii"))
        else:
            acc.dropped += len(chunk)


def _self_destruct(kind: str, error: str) -> None:
    """Report why, then take down our whole process group - ourselves included.

    The runner and the sandboxed child share a process group, so a self-sparing
    kill is not expressible. That is exactly the semantics we want here: nothing
    the port started may outlive the port.
    """
    with contextlib.suppress(Exception):
        _emit("fatal", kind=kind, error=error)
    os.killpg(os.getpgrp(), signal.SIGKILL)


async def _watch_parent(original_ppid: int) -> None:
    """Self-destruct if the Edda worker that spawned us disappears.

    We run in our own session, so nothing else would ever signal us. Without
    this an orphaned runner would linger until the belt expires, holding srt's
    proxy servers and the external program alive.
    """
    if original_ppid <= 1:  # pragma: no cover - already orphaned or PID 1
        return
    while True:
        await asyncio.sleep(_PARENT_POLL_SECONDS)
        if os.getppid() != original_ppid:
            _self_destruct("orphaned", "the parent Edda worker exited")


async def _supervise(
    child: asyncio.subprocess.Process,
    reader: asyncio.StreamReader,
    cap: int,
    belt_seconds: float,
) -> tuple[int, _Stream, _Stream]:
    """Pump stdin, relay output, and wait for the child.

    Args:
        child: The running external program.
        reader: Reader attached to this process's stdin.
        cap: Per-stream limit on bytes relayed to the parent.
        belt_seconds: Self-destruct deadline, a backstop for a dead parent.

    Returns:
        The child's exit code and the two stream accounts.
    """
    assert child.stdout is not None
    assert child.stderr is not None

    out, err = _Stream(), _Stream()
    stdin_task = asyncio.create_task(_pump_stdin(reader, child))
    out_task = asyncio.create_task(_read_stream(child.stdout, "o", cap, out))
    err_task = asyncio.create_task(_read_stream(child.stderr, "e", cap, err))
    watch_task = asyncio.create_task(_watch_parent(os.getppid()))

    try:
        returncode = await asyncio.wait_for(child.wait(), belt_seconds)
    except TimeoutError:
        _self_destruct("belt_timeout", f"runner belt expired after {belt_seconds}s")
        raise  # pragma: no cover - unreachable, SIGKILL is not maskable
    finally:
        for task in (stdin_task, watch_task):
            task.cancel()
            with contextlib.suppress(BaseException):
                await task

    # The child is gone but grandchildren may still hold its pipes open.
    with contextlib.suppress(TimeoutError):
        await asyncio.wait_for(asyncio.gather(out_task, err_task), _CHILD_DRAIN_SECONDS)
    for task in (out_task, err_task):
        task.cancel()
        with contextlib.suppress(BaseException):
            await task

    return returncode, out, err


def _load_spec(raw: bytes) -> dict[str, Any]:
    """Parse and sanity-check the spec line.

    Raises:
        ValueError: If the spec is absent, malformed, or not an object.
    """
    if not raw.strip():
        raise ValueError("no spec received on stdin")
    spec = json.loads(raw)
    if not isinstance(spec, dict):
        raise ValueError(f"spec must be a JSON object, got {type(spec).__name__}")
    return spec


async def _prepare_sandbox(spec: dict[str, Any]) -> tuple[str, str | None, bool]:
    """Initialise srt and wrap the command.

    Args:
        spec: The parsed spec.

    Returns:
        The wrapped command, the srt version, and whether a violation monitor is
        running.

    Raises:
        _Fatal: If the sandbox cannot be established.
    """
    if os.environ.get(_FORCE_UNAVAILABLE_ENV) == "1":
        raise _Fatal(
            "sandbox_unavailable",
            f"sandbox forced unavailable by {_FORCE_UNAVAILABLE_ENV}=1. {_install_hint()}",
        )

    try:
        import sandbox_runtime
        from sandbox_runtime import SandboxManager, SandboxRuntimeConfig
    except ImportError as exc:
        raise _Fatal(
            "sandbox_unavailable", f"sandbox-runtime is not installed ({exc}). {_install_hint()}"
        ) from exc

    if not SandboxManager.check_dependencies():
        raise _Fatal(
            "sandbox_unavailable",
            f"sandbox-runtime's system dependencies are missing on {sys.platform!r}. "
            f"{_install_hint()}",
        )

    try:
        config = SandboxRuntimeConfig(**spec["sandbox_config"])
    except Exception as exc:
        raise _Fatal(
            "config_invalid", f"sandbox-runtime rejected the configuration: {exc}"
        ) from exc

    # srt forces TMPDIR=/tmp/claude on the sandboxed child but never creates it,
    # and the child cannot create it itself (writing /tmp is denied).
    with contextlib.suppress(OSError):
        os.makedirs(_SANDBOX_TMPDIR, exist_ok=True)

    monitor = (
        bool(spec.get("collect_violations"))
        and sys.platform == "darwin"
        and shutil.which("log") is not None
    )
    try:
        await SandboxManager.initialize(config, enable_log_monitor=monitor)
    except Exception as exc:
        if not monitor:
            raise _Fatal("sandbox_unavailable", str(exc)) from exc
        # The log monitor is advisory; never let it cost us the sandbox.
        with contextlib.suppress(Exception):
            await SandboxManager.reset()
        monitor = False
        try:
            await SandboxManager.initialize(config, enable_log_monitor=False)
        except Exception as retry_exc:
            raise _Fatal("sandbox_unavailable", str(retry_exc)) from retry_exc

    command: str = await SandboxManager.wrap_with_sandbox(spec["command"])
    version: str | None = getattr(sandbox_runtime, "__version__", None)
    return command, version, monitor


def _collect_violations() -> list[str]:
    """Return the sandbox violations recorded for this call (macOS, best-effort).

    Deliberately *not* ``get_violations_for_command()``. The kernel emits one log
    record whose message holds both ``Sandbox: cmd(pid) deny(1) ...`` and srt's
    ``CMD64_..._END`` tag, separated by a newline; srt's monitor reads ``log
    stream`` line by line, so the tag never shares a line with the denial and the
    violation is stored with ``encoded_command=None``. Filtering by command
    therefore always returns nothing.

    We can simply skip the filter: this process ran exactly one command, and the
    log predicate keys on a session tag unique to this process, so everything in
    the store is ours. One more thing the runner-per-call design buys.

    Violations only appear once ``log stream`` is live, which takes a second or
    two. A command that fails immediately can outrun it. They are advisory.

    The result is bounded (count and per-line length): the advisory feed must
    never grow a protocol line past what the parent will read.
    """
    try:
        from sandbox_runtime import SandboxManager

        store = SandboxManager.get_sandbox_violation_store()
        lines = [str(violation.line)[:_MAX_VIOLATION_CHARS] for violation in store.get_violations()]
        return lines[:_MAX_VIOLATION_LINES]
    except Exception:  # pragma: no cover - advisory only
        return []


class _Fatal(Exception):
    """Internal carrier for a ``fatal`` event."""

    def __init__(self, kind: str, error: str) -> None:
        super().__init__(error)
        self.kind = kind
        self.error = error


async def _run() -> int:
    """Execute one port call. Returns the runner's exit status."""
    started = time.monotonic()
    reader = await _stdin_reader()

    try:
        spec = _load_spec(await reader.readline())
    except Exception as exc:
        raise _Fatal("internal", f"invalid spec: {exc}") from exc

    if spec.get("backend") != "srt":
        raise _Fatal("unknown_backend", f"unsupported sandbox backend: {spec.get('backend')!r}")

    unsandboxed = bool(spec.get("unsandboxed"))
    monitor = False
    srt_version: str | None = None
    command: str = spec["command"]

    if not unsandboxed:
        command, srt_version, monitor = await _prepare_sandbox(spec)

    env = os.environ.copy()
    env.update(spec.get("env") or {})

    try:
        child = await asyncio.create_subprocess_shell(
            command,
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            cwd=spec["cwd"],
            env=env,
            limit=_CHUNK_BYTES * 2,
        )
    except Exception as exc:
        raise _Fatal("spawn_failed", f"could not start the external program: {exc}") from exc

    _emit("start", pid=child.pid, sandboxed=not unsandboxed, srt=srt_version)

    try:
        code, out, err = await _supervise(
            child, reader, int(spec["max_output_bytes"]), float(spec["timeout_belt_seconds"])
        )
    except BaseException:
        with contextlib.suppress(Exception):
            child.kill()
        raise

    duration_ms = int((time.monotonic() - started) * 1000)

    if monitor:
        await asyncio.sleep(_VIOLATION_DRAIN_SECONDS)
        lines = _collect_violations()
        if lines:
            _emit("violations", lines=lines)

    _emit(
        "exit",
        code=code,
        duration_ms=duration_ms,
        stdout_bytes=out.total,
        stderr_bytes=err.total,
        stdout_dropped=out.dropped,
        stderr_dropped=err.dropped,
    )
    return 0


async def _main() -> int:
    """Entry point body: run one port call, always resetting srt afterwards."""
    try:
        return await _run()
    except _Fatal as fatal:
        _emit("fatal", kind=fatal.kind, error=fatal.error)
        return 1
    finally:
        if "sandbox_runtime" in sys.modules:
            with contextlib.suppress(Exception):
                from sandbox_runtime import SandboxManager

                await SandboxManager.reset()


def main() -> int:
    """Console entry point."""
    try:
        return asyncio.run(_main())
    except BrokenPipeError:
        # The parent is gone. It owns our process group and will clean up.
        return 1
    except BaseException:
        with contextlib.suppress(Exception):
            _emit("fatal", kind="internal", error=traceback.format_exc()[-4000:])
        return 1


if __name__ == "__main__":
    code = main()
    # Leave immediately rather than unwind. srt registers an atexit hook that opens
    # a *new* event loop to reset a manager we have already reset, and its macOS log
    # monitor's subprocess transport is finalised after the loop closes, printing an
    # "Event loop is closed" traceback. Both would land on our stderr, which the
    # parent quotes back in error messages. We have said everything we have to say.
    with contextlib.suppress(Exception):
        sys.stdout.buffer.flush()
        sys.stderr.flush()
    os._exit(code)
