"""Erlang-style child supervision as a library workflow.

A supervisor is an ordinary actor (workflow) that starts a fixed set of
child actors, watches their statuses, and restarts them according to a
strategy. Being a workflow, it inherits durability for free: if the
supervisor's worker crashes, the stale-lock sweep resumes it, and its
restart bookkeeping survives in the recur-carried state.

Design notes (parity with romancy's supervisor):

- Restart means spawning a NEW instance of the same workflow with the same
  input (the failed instance is terminal). Child instance IDs are
  deterministic — "{supervisor_id}:{child_name}:g{generation}" — so a
  replayed spawn activity is idempotent.
- The poll is authoritative: actual instance rows decide who is dead.
  Monitors/DOWN notifications are not used — edda's recur continues on a
  NEW instance (continued_from), so a direct-channel subscription would
  not survive iterations. sleep() bounds the poll interval instead.
- 'recurred' counts as ALIVE: a child recurring is mid-flight, not dead.
- Restarting a live child (one_for_all / rest_for_one) cancels the running
  instance and immediately spawns the replacement. Cancellation is a
  best-effort kill — the replacement has a distinct generation ID either
  way.
- History stays bounded because each loop iteration ends in recur().

Usage::

    from edda.supervisor import define_supervisor, ChildSpec, Strategy, RestartMode

    sup = define_supervisor(
        "pipeline-sup",
        strategy=Strategy.ONE_FOR_ONE,
        max_restarts=5,
        within_seconds=60,
        children=[
            ChildSpec(name="worker", workflow="my_worker",
                      input={"x": 1}, restart=RestartMode.TRANSIENT),
        ],
    )
    instance_id = await sup.start()  # zero/empty input = initial spawn
"""

from __future__ import annotations

# mypy: disable-error-code="assignment,misc,call-arg,no-untyped-def"
import hashlib
import inspect
import time
from dataclasses import dataclass, field
from enum import IntEnum
from typing import Any, cast

from edda.activity import activity
from edda.channels import sleep
from edda.workflow import workflow


class RestartMode(IntEnum):
    """When a child is restarted after termination."""

    PERMANENT = 0  # always restart, even on normal completion
    TRANSIENT = 1  # restart only on abnormal termination (failed/cancelled)
    TEMPORARY = 2  # never restart


class Strategy(IntEnum):
    """Which children restart when one child dies."""

    ONE_FOR_ONE = 0  # restart only the dead child
    ONE_FOR_ALL = 1  # restart all non-terminal children
    REST_FOR_ONE = 2  # restart the dead child and all started after it


@dataclass
class ChildSpec:
    """One supervised child. input is passed verbatim on every (re)start."""

    name: str
    workflow: str
    input: dict[str, Any] = field(default_factory=dict)
    restart: RestartMode = RestartMode.TRANSIENT


_DEAD = {"completed", "failed", "cancelled"}


def _needs_restart(mode: int, status: str) -> bool:
    if mode == RestartMode.PERMANENT:
        return True
    if mode == RestartMode.TRANSIENT:
        return status in ("failed", "cancelled")
    return False


def define_supervisor(
    name: str,
    *,
    strategy: Strategy = Strategy.ONE_FOR_ONE,
    max_restarts: int = 0,  # 0 = unlimited
    within_seconds: float = 60.0,
    poll_seconds: int = 30,
    children: list[ChildSpec],
):
    """Build a supervisor workflow over children. Start with no arguments::

        instance_id = await sup.start()
    """

    @activity
    async def sync_activity(ctx: Any, state_children: list[dict[str, Any]]) -> dict[str, Any]:
        """One authoritative pass over children in a single transaction."""
        storage = ctx.storage
        now = time.time()
        restarted = 0

        # 1. classify; never-spawned children are spawn-pending (not dead,
        #    no cascade trigger, not counted toward intensity)
        statuses: list[str] = []
        restart: list[bool] = []
        dead_idx = -1
        for i, c in enumerate(state_children):
            if c.get("terminal"):
                statuses.append("terminal")
                restart.append(False)
                continue
            if not c.get("instance_id"):
                statuses.append("")
                restart.append(True)  # initial spawn
                continue
            inst = await storage.get_instance(c["instance_id"])
            status = inst["status"] if inst else "failed"  # row missing = crashed
            statuses.append(status)
            if status not in _DEAD:
                restart.append(False)
                continue
            if _needs_restart(c["spec"]["restart"], status):
                restart.append(True)
                if dead_idx == -1:
                    dead_idx = i
            else:
                c["terminal"] = True
                c["final_status"] = status
                restart.append(False)

        # 2. expand the restart set per strategy
        if dead_idx >= 0:
            if strategy == Strategy.ONE_FOR_ALL:
                for i, c in enumerate(state_children):
                    if not c.get("terminal"):
                        restart[i] = True
            elif strategy == Strategy.REST_FOR_ONE:
                for i in range(dead_idx, len(state_children)):
                    if not state_children[i].get("terminal"):
                        restart[i] = True

        # 3. restart: cancel if alive, spawn next generation
        for i, c in enumerate(state_children):
            if not restart[i]:
                continue
            if c.get("instance_id") and statuses[i] not in _DEAD:
                await storage.cancel_instance(c["instance_id"], "supervisor restart")
            gen = c.get("generation", 0) + 1
            new_id = f"{ctx.instance_id}:{c['spec']['name']}:g{gen}"
            # Resolve the child workflow's source hash — instances are
            # joined to workflow_definitions on (name, hash), so a wrong
            # hash makes the child invisible to the resumable sweep.
            from edda.workflow import _workflow_registry

            wf = _workflow_registry.get(c["spec"]["workflow"])
            source_hash = ""
            if wf is not None:
                try:
                    source_code = inspect.getsource(wf.func)
                except (OSError, TypeError):
                    source_code = ""
                if source_code:
                    source_hash = hashlib.sha256(source_code.encode("utf-8")).hexdigest()
                    await storage.upsert_workflow_definition(
                        workflow_name=c["spec"]["workflow"],
                        source_hash=source_hash,
                        source_code=source_code,
                    )
            await storage.create_instance(
                instance_id=new_id,
                workflow_name=c["spec"]["workflow"],
                source_hash=source_hash,
                owner_service="supervisor",
                input_data=c["spec"]["input"],
            )
            if c.get("instance_id"):
                restarted += 1  # replacement of a spawned child counts
            c["instance_id"] = new_id
            c["generation"] = gen

        alive = sum(1 for c in state_children if not c.get("terminal"))
        return {
            "children": state_children,
            "restarted_count": restarted,
            "alive_count": alive,
            "now": now,
        }

    @workflow
    async def supervisor_workflow(ctx: Any, state: dict[str, Any] | None = None) -> dict[str, Any]:
        if state is None or not state.get("children"):
            state = {
                "children": [
                    {"spec": {"name": c.name, "workflow": c.workflow,
                              "input": c.input, "restart": int(c.restart)},
                     "instance_id": "", "generation": 0}
                    for c in children
                ],
                "restarts": [],
            }

        out = await sync_activity(ctx, state["children"])
        state["children"] = out["children"]

        # restart intensity over the sliding window
        if out["restarted_count"] > 0:
            cutoff = out["now"] - within_seconds
            state["restarts"] = [t for t in state["restarts"] if t >= cutoff]
            state["restarts"].extend([out["now"]] * out["restarted_count"])
            if max_restarts > 0 and len(state["restarts"]) > max_restarts:
                raise RuntimeError("supervisor: restart intensity exceeded")

        if out["alive_count"] == 0:
            return {
                "children": state["children"],
                "total_restarts": len(state["restarts"]),
                "outcome": "completed",
            }

        await sleep(ctx, poll_seconds)
        await ctx.recur(state=state)
        raise RuntimeError("supervisor: unreachable after recur")  # pragma: no cover

    # Workflow.name defaults to the function name; give each supervisor
    # its own identity in the registry.
    from edda.workflow import _workflow_registry

    wf_obj = cast(Any, supervisor_workflow)
    _workflow_registry.pop(wf_obj.name, None)
    wf_obj.name = name
    _workflow_registry[name] = wf_obj
    return supervisor_workflow
