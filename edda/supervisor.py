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
  Monitors/DOWN notifications are not used; sleep() bounds the poll
  interval instead.
- 'recurred' counts as ALIVE: a child recurring is mid-flight, not dead.
- Restarting a live child (one_for_all / rest_for_one) cancels the running
  instance and immediately spawns the replacement; a temporary sibling is
  only stopped. The cancel is conditional: a child that ended on its own
  meanwhile is judged by its actual status, and a compensating child is
  left to finish (the next sync sees how it ends).
- History stays bounded because each loop iteration ends in recur().
- A child may be a workflow of another runtime (Romancy: framework="go",
  Shikibu: framework="ruby"). Each runtime runs only the instances of its
  own framework, so the child row carries the child's framework and an
  empty source hash: this process cannot know the other runtime's hash,
  and an empty one matches any version of it.

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
            # A workflow that Romancy runs
            ChildSpec(name="indexer", workflow="index_repo", framework="go"),
        ],
    )
    instance_id = await sup.start()  # zero/empty input = initial spawn
"""

from __future__ import annotations

# mypy: disable-error-code="assignment,misc,call-arg,no-untyped-def"
import hashlib
import inspect
import time
from dataclasses import dataclass
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
    """One supervised child. input is passed verbatim on every (re)start.

    framework names the runtime that runs the child's workflow: "python"
    (Edda, the default), "go" (Romancy) or "ruby" (Shikibu). Without an
    input, a Go child gets null and a Python or Ruby child an empty dict.
    """

    name: str
    workflow: str
    input: Any = None
    restart: RestartMode = RestartMode.TRANSIENT
    framework: str = "python"


_DEAD = {"completed", "failed", "cancelled"}


async def _child_source_hash(storage: Any, workflow_name: str, framework: str) -> str:
    """Return the source hash for a spawned child's row.

    Instances are joined to workflow_definitions on (name, hash): the
    definition row must exist, and a wrong hash makes the child invisible
    to the resumable sweep.
    """
    if framework != "python":
        # Another runtime runs the child: its hash is unknown here, and an
        # empty one matches any version that runtime registers.
        if await storage.get_workflow_definition(workflow_name, "") is None:
            await storage.upsert_workflow_definition(
                workflow_name=workflow_name, source_hash="", source_code=""
            )
        return ""

    from edda.workflow import _workflow_registry

    wf = _workflow_registry.get(workflow_name)
    if wf is None:
        # Not registered in this process (another Edda worker runs it): use
        # its recorded definition.
        definition = await storage.get_current_workflow_definition(workflow_name)
        if definition is None:
            raise ValueError(f"supervisor child workflow {workflow_name!r} has no definition")
        return str(definition["source_hash"])
    # Same definition start() records (placeholder source when the code
    # cannot be read)
    try:
        source_code = inspect.getsource(wf.func)
    except (OSError, TypeError) as e:
        source_code = f"# Source code not available\n# Workflow: {workflow_name}\n# Error: {e}"
    source_hash = hashlib.sha256(source_code.encode("utf-8")).hexdigest()
    await storage.upsert_workflow_definition(
        workflow_name=workflow_name,
        source_hash=source_hash,
        source_code=source_code,
    )
    return source_hash


def _child_input(spec: dict[str, Any], framework: str) -> Any:
    """Return the child's input, or its runtime's empty input when it has
    none: null for Go (the zero value of the input type), {} for Python and
    Ruby (the input is passed as keyword arguments)."""
    if spec.get("input") is not None:
        return spec["input"]
    return None if framework == "go" else {}


def _needs_restart(mode: int, status: str) -> bool:
    if mode == RestartMode.PERMANENT:
        return True
    if mode == RestartMode.TRANSIENT:
        return status in ("failed", "cancelled")
    return False


async def _stop_child(storage: Any, instance_id: str) -> tuple[str, bool]:
    """Cancel a live child the supervisor replaces or stops; return the
    status the child is in afterwards and whether it is stopped.

    The cancel is conditional (SEMANTICS §7): a child that is no longer
    cancellable ended on its own between the sync's read and the cancel, and
    its actual status -- not an assumed cancellation -- is what its restart
    mode applies to. A child still compensating is not cancelled (its
    compensations run to the end); the next sync sees how it ends. Any
    other failure aborts the sync.
    """
    if await storage.cancel_instance(instance_id, "supervisor restart"):
        return "cancelled", True
    inst = await storage.get_instance(instance_id)
    if inst is None:
        return "failed", True  # row missing: treat as crashed
    return inst["status"], inst["status"] in _DEAD


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

        # 2. expand the restart set per strategy. A temporary sibling caught
        #    in the cascade is stopped (cancelled, terminal) but never
        #    restarted (SEMANTICS §7, as in OTP).
        stop = [False] * len(state_children)

        def cascade(i: int) -> None:
            c = state_children[i]
            if c.get("terminal") or restart[i]:
                return
            if c["spec"]["restart"] == RestartMode.TEMPORARY:
                stop[i] = bool(c.get("instance_id"))
                return
            restart[i] = True

        if dead_idx >= 0:
            if strategy == Strategy.ONE_FOR_ALL:
                for i in range(len(state_children)):
                    cascade(i)
            elif strategy == Strategy.REST_FOR_ONE:
                for i in range(dead_idx, len(state_children)):
                    cascade(i)
        for i, c in enumerate(state_children):
            if stop[i]:
                status, stopped = await _stop_child(storage, c["instance_id"])
                if stopped:
                    c["terminal"] = True
                    c["final_status"] = status

        # 3. restart: cancel if alive, spawn next generation
        for i, c in enumerate(state_children):
            if not restart[i]:
                continue
            if c.get("instance_id") and statuses[i] not in _DEAD:
                status, stopped = await _stop_child(storage, c["instance_id"])
                if not stopped:
                    continue  # still compensating: the next sync sees how it ends
                if not _needs_restart(c["spec"]["restart"], status):
                    # It ended on its own before the cancel landed (a transient
                    # child completing normally): its restart mode decides, as
                    # if the sync had read it terminal.
                    c["terminal"] = True
                    c["final_status"] = status
                    continue
            gen = c.get("generation", 0) + 1
            new_id = f"{ctx.instance_id}:{c['spec']['name']}:g{gen}"
            wf_name = c["spec"]["workflow"]
            # A state recorded before ChildSpec.framework existed has none
            framework = c["spec"].get("framework") or "python"
            await storage.create_instance(
                instance_id=new_id,
                workflow_name=wf_name,
                source_hash=await _child_source_hash(storage, wf_name, framework),
                owner_service="supervisor",
                input_data=_child_input(c["spec"], framework),
                parent_instance_id=ctx.instance_id,
                framework=framework,
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
                    {
                        "spec": {
                            "name": c.name,
                            "workflow": c.workflow,
                            "input": c.input,
                            "restart": int(c.restart),
                            "framework": c.framework,
                        },
                        "instance_id": "",
                        "generation": 0,
                    }
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
