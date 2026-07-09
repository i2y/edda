"""Minimal synchronous bridge for driving Edda from a sync REPL (xonsh, IPython, python).

Edda is async, and — crucially — a workflow only makes progress while its event
loop keeps running: the app's background tasks resume suspended workflows, fire
timers, and relay the outbox. A synchronous REPL has no such loop, and calling
``asyncio.run()` per command would spin up and tear down a fresh loop each time,
so background progress would stop between calls and loop-bound DB connections
could not persist.

This bridge instead runs ONE persistent event loop on a dedicated daemon thread,
initializes an ``EddaApp`` on it, and lets synchronous code call into Edda via
``submit(coro)``. It is deliberately small and lives in ``examples/`` rather than
the ``edda`` package: it is the thin machinery that makes interactive dogfooding
possible, not a supported public API. Promote it into the framework once its
shape has proven out in real use.

Usage (xonsh)::

    from examples.repl_bridge import connect
    from myproject import fix              # a @workflow; importing registers it

    edda = connect("orchestrator", "sqlite:///edda.db")

    iid = edda.start(fix, issue="auth timeout")   # returns at once (detached)
    edda.status(iid)                              # {'status': 'running', ...}
    patch = edda.result(iid)                      # the live return value, once done
    echo @(patch) > out/fix.diff                  # a Python value, flowed to the shell
    git diff --stat                               # inspect / grep / rerun at the same prompt
    edda.shutdown()                               # (also runs automatically at exit)

``start`` uses ``start_detached`` so a long-running workflow — e.g. one that shells
out to an agent CLI via ``edda.agent.port`` — never blocks your prompt. Fetch its
result later with ``result(iid)``; watch progress with ``status(iid)``; read the raw
port I/O it recorded with ``history(iid)`` (the rows whose ``event_type`` is
``"ExternRecord"``).

Note: the bridge runs Edda on a uvloop loop (``uvloop.new_event_loop()``), which
is what EddaApp expects — and, importantly, what lets subprocess-based activities
(e.g. ``edda.agent.port``) spawn from this background thread. A vanilla asyncio
loop fails there: ``EddaApp.initialize()`` installs uvloop's *policy* process-wide,
and a stdlib loop then asks that policy for a child watcher, which uvloop does not
provide (``NotImplementedError``). Running the loop itself on uvloop sidesteps the
child-watcher machinery entirely.
"""

from __future__ import annotations

import asyncio
import atexit
import threading
from collections.abc import Coroutine
from typing import Any, TypeVar

import uvloop

from edda import EddaApp
from edda.workflow import Workflow, get_all_workflows

T = TypeVar("T")


class EddaBridge:
    """A synchronous handle onto an EddaApp running on a private loop thread."""

    def __init__(self, app: EddaApp) -> None:
        self.app = app
        # A uvloop loop (not asyncio.new_event_loop): EddaApp runs on uvloop, and
        # subprocess-based activities (edda.agent.port) can only spawn here because
        # uvloop handles child processes natively — see the module docstring.
        self._loop = uvloop.new_event_loop()
        self._thread = threading.Thread(target=self._run_loop, name="edda-bridge-loop", daemon=True)
        self._closed = False
        self._thread.start()
        self.submit(app.initialize())
        atexit.register(self.shutdown)

    # -- lifecycle ---------------------------------------------------------

    def _run_loop(self) -> None:
        asyncio.set_event_loop(self._loop)
        self._loop.run_forever()

    def submit(self, coro: Coroutine[Any, Any, T], timeout: float | None = None) -> T:
        """Run an Edda coroutine on the bridge loop and block for its result."""
        return asyncio.run_coroutine_threadsafe(coro, self._loop).result(timeout)

    def shutdown(self) -> None:
        """Shut the EddaApp down cleanly and stop the loop thread. Idempotent."""
        if self._closed:
            return
        self._closed = True
        try:
            self.submit(self.app.shutdown())
        finally:
            self._loop.call_soon_threadsafe(self._loop.stop)
            self._thread.join(timeout=5)

    # -- workflow control (mirrors the MCP durable_tool surface) -----------

    def start(self, workflow: Workflow | str, **kwargs: Any) -> str:
        """Start a workflow in the background and return its instance ID at once."""
        return self.submit(self._resolve(workflow).start_detached(**kwargs))

    def status(self, instance_id: str) -> dict[str, Any]:
        """Return a small status dict: status, current activity, completed count."""
        instance = self._get_instance(instance_id)
        history = self.submit(self.app.storage.get_history(instance_id))
        completed = sum(1 for h in history if h["event_type"] == "ActivityCompleted")
        return {
            "status": instance["status"],
            "current_activity": instance.get("current_activity_id"),
            "completed_activities": completed,
        }

    def result(self, instance_id: str) -> Any:
        """Return the workflow's live return value, or raise if it is not completed."""
        instance = self._get_instance(instance_id)
        status = instance["status"]
        if status != "completed":
            raise RuntimeError(f"workflow {instance_id} is not completed (status={status!r})")
        output = instance.get("output_data") or {}
        return output.get("result")

    def cancel(self, instance_id: str) -> bool:
        """Cancel a running/waiting workflow, running its compensations. Returns success."""
        if self.app.replay_engine is None:  # pragma: no cover - initialized in __init__
            raise RuntimeError("EddaApp is not initialized")
        return self.submit(self.app.replay_engine.cancel_workflow(instance_id, cancelled_by="repl"))

    def history(self, instance_id: str) -> list[dict[str, Any]]:
        """Raw history rows in order, including the ExternRecord rows a port writes."""
        return self.submit(self.app.storage.get_history(instance_id))

    # -- internals ---------------------------------------------------------

    def _get_instance(self, instance_id: str) -> dict[str, Any]:
        instance = self.submit(self.app.storage.get_instance(instance_id))
        if instance is None:
            raise LookupError(f"workflow instance not found: {instance_id}")
        return instance

    @staticmethod
    def _resolve(workflow: Workflow | str) -> Workflow:
        if isinstance(workflow, Workflow):
            return workflow
        registry = get_all_workflows()
        if workflow not in registry:
            raise LookupError(
                f"no workflow named {workflow!r} is registered; "
                f"available: {sorted(registry)}. Import the module that defines it first."
            )
        return registry[workflow]


def connect(service_name: str, db_url: str, **app_kwargs: Any) -> EddaBridge:
    """Build an EddaApp, start its loop on a background thread, and return a bridge."""
    app = EddaApp(service_name=service_name, db_url=db_url, **app_kwargs)
    return EddaBridge(app)
