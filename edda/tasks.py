"""Task primitive (Vercel Board-style): durable, claimable units of work
posted to a channel. Workers compete for tasks via ``claim_task`` — the
competing-claim machinery applies; the task ROW is the source of truth.

Typical worker loop::

    await subscribe(ctx, tasks_channel("tasks:build"), mode="broadcast")
    while True:
        task = await claim_task(ctx, "tasks:build")
        if task is None:
            await receive(ctx, tasks_channel("tasks:build"))  # block for a task event
            continue
        result = await do_work(ctx, task["data"])  # an @activity
        await complete_task(ctx, task["id"], result)

Every function here is replay-safe (SEMANTICS §2): called from workflow code
it runs once as a recorded step, and replays return the recorded result or
error; inside an activity body it runs directly, as part of the activity.
An effect (post, claim, complete, fail) commits together with its record,
under the run's lock.
"""

import base64
import json
import math
import time
import uuid
from collections.abc import Awaitable, Callable
from datetime import datetime
from typing import TYPE_CHECKING, Any, cast

from edda.channels import (
    _enforce_channel_capability,
    _enforce_direct_privacy,
    receive,
    subscribe,
)
from edda.exceptions import InvalidArgumentError, TaskNotFoundError

if TYPE_CHECKING:
    from edda.context import WorkflowContext


def tasks_channel(channel: str) -> str:
    """Task-event stream name: all lifecycle notices (posted/claimed/
    done/failed) are published on this broadcast channel — workers and
    waiters subscribe here."""
    return f"{channel}:tasks"


def _task_payload(value: dict[str, Any] | str | None) -> str | None:
    """Encode a task payload; an unserializable one is the caller's error."""
    if value is None or isinstance(value, str):
        return value
    try:
        return json.dumps(value)
    except (TypeError, ValueError) as error:
        raise InvalidArgumentError(f"task payload is not JSON-serializable: {error}") from error


async def post_task(ctx: "WorkflowContext", channel: str, data: dict[str, Any] | str) -> str:
    """Create a task on channel and publish a "task_posted" notice on
    tasks_channel(channel). Returns the task ID."""

    async def _post() -> str:
        payload = _task_payload(data)
        task_id = f"task-{uuid.uuid4()}"
        await ctx.storage.post_task(task_id, channel, cast(str, payload))
        return task_id

    return await ctx._run_step("edda.post_task", _post, effect=True)


async def claim_task(ctx: "WorkflowContext", channel: str) -> dict[str, Any] | None:
    """Atomically claim the oldest open task on channel for this instance.

    Returns the claimed task dict (id/channel/data/state/claimed_by), or
    None when no open task exists.
    """

    async def _claim() -> dict[str, Any] | None:
        return await ctx.storage.claim_task(channel, ctx.instance_id)

    return await ctx._run_step("edda.claim_task", _claim, effect=True)


async def complete_task(
    ctx: "WorkflowContext", task_id: str, result: dict[str, Any] | str | None = None
) -> None:
    """Mark a claimed task done. Only the claimant may complete —
    raises TaskConflictError otherwise, TaskNotFoundError for an unknown ID."""

    async def _complete() -> None:
        payload = _task_payload(result)
        await ctx.storage.complete_task(task_id, ctx.instance_id, payload, failed=False)

    await ctx._run_step("edda.complete_task", _complete, effect=True)


async def fail_task(
    ctx: "WorkflowContext", task_id: str, result: dict[str, Any] | str | None = None
) -> None:
    """Mark a claimed task failed. Only the claimant may fail it
    (TaskConflictError); TaskNotFoundError for an unknown ID."""

    async def _fail() -> None:
        payload = _task_payload(result)
        await ctx.storage.complete_task(task_id, ctx.instance_id, payload, failed=True)

    await ctx._run_step("edda.fail_task", _fail, effect=True)


async def list_tasks(
    ctx: "WorkflowContext", channel: str, state: str | None = None
) -> list[dict[str, Any]]:
    """Return tasks on channel filtered by state (None = all)."""

    async def _list() -> list[dict[str, Any]]:
        return await ctx.storage.list_tasks(channel, state)

    return await ctx._run_step("edda.list_tasks", _list)


def _message_to_json(message: dict[str, Any]) -> dict[str, Any]:
    """JSON-safe form of a channel_messages_since row (recorded in history)."""
    out = dict(message)
    if isinstance(out.get("published_at"), datetime):
        out["published_at"] = out["published_at"].isoformat()
    if isinstance(out.get("data"), (bytes, bytearray, memoryview)):
        out["data"] = base64.b64encode(bytes(out["data"])).decode("ascii")
    return out


def _message_from_json(message: dict[str, Any]) -> dict[str, Any]:
    out = dict(message)
    if isinstance(out.get("published_at"), str):
        out["published_at"] = datetime.fromisoformat(out["published_at"])
    if out.get("data_type") == "binary" and isinstance(out.get("data"), str):
        out["data"] = base64.b64decode(out["data"])
    return out


async def read_channel_since(
    ctx: "WorkflowContext",
    channel: str,
    message_id: str | None = None,
    capability_token: str | None = None,
) -> list[dict[str, Any]]:
    """Return messages on channel published after the message_id cursor
    (None reads all, oldest first). Use the last message's ``message_id``
    as the next cursor for incremental polling.

    A channel sealed for ``channel_sub`` requires a valid capability
    token, exactly as ``receive()`` does.

    Raises:
        CursorNotFoundError: If no message with that ID is on the channel any
            more (consumed, cleaned up, or never existed); use
            ``read_channel_after`` for a cursor that must outlive its message.
    """

    async def _read() -> list[dict[str, Any]]:
        await _enforce_direct_privacy(ctx, channel)
        await _enforce_channel_capability(ctx.storage, "channel_sub", channel, capability_token)
        rows = await ctx.storage.channel_messages_since(channel, message_id)
        return [_message_to_json(row) for row in rows]

    return [_message_from_json(m) for m in await ctx._run_step("edda.read_channel_since", _read)]


async def read_channel_after(
    ctx: "WorkflowContext",
    channel: str,
    after_id: int,
    capability_token: str | None = None,
) -> list[dict[str, Any]]:
    """Return messages on channel whose numeric ``id`` is above after_id, in
    id order (0 reads from the start). Use the last message's ``id`` as the
    next cursor: unlike ``read_channel_since`` it keeps working after the
    cursor message has been deleted.

    Sealing is the same as for ``read_channel_since``.

    Raises:
        InvalidArgumentError: If after_id is not an int
    """

    async def _read() -> list[dict[str, Any]]:
        if not isinstance(after_id, int) or isinstance(after_id, bool):
            raise InvalidArgumentError(f"after_id must be an int, not {after_id!r}")
        await _enforce_direct_privacy(ctx, channel)
        await _enforce_channel_capability(ctx.storage, "channel_sub", channel, capability_token)
        rows = await ctx.storage.channel_messages_after(channel, after_id)
        return [_message_to_json(row) for row in rows]

    return [_message_from_json(m) for m in await ctx._run_step("edda.read_channel_after", _read)]


async def _wait_for_terminal(
    ctx: "WorkflowContext",
    notify_ch: str,
    timeout_seconds: float | None,
    poll: Callable[[], Awaitable[dict[str, Any] | None]],
) -> dict[str, Any]:
    """Subscribe to notify_ch, then poll → receive until the poll answers a
    task in a terminal state.

    Every decision depends only on recorded outcomes, so a replay takes the
    recorded path: polls are steps and the deadline is recorded once — every
    run waits toward the same instant, and a recorded receive timeout ends
    the wait.
    """
    deadline: float | None = None
    if timeout_seconds is not None:
        timeout = timeout_seconds

        async def _deadline() -> float:
            return time.time() + timeout

        deadline = await ctx._run_step("edda.wait_deadline", _deadline)
    # Subscribe before polling: a notice published after the poll is then
    # behind the subscription cursor and wakes the receive.
    await subscribe(ctx, notify_ch, mode="broadcast")
    while True:
        task = await poll()
        if task is not None and task["state"] in ("done", "failed"):
            return task
        try:
            await receive(
                ctx,
                notify_ch,
                timeout_seconds=(
                    None if deadline is None else max(1, math.ceil(deadline - time.time()))
                ),
            )
        except TimeoutError:
            task = await poll()
            if task is not None and task["state"] in ("done", "failed"):
                return task
            raise


async def wait_task(
    ctx: "WorkflowContext", task_id: str, timeout_seconds: float | None = None
) -> dict[str, Any]:
    """Block until the task reaches a terminal state (done or failed).

    Subscribes broadcast to the channel's task-event stream and re-checks
    the task row on every wake — the workflow durably suspends (lock
    released) between notices.

    Raises TaskNotFoundError for unknown IDs, TimeoutError on expiry.
    """

    async def _get() -> dict[str, Any]:
        task = await ctx.storage.get_task(task_id)
        if task is None:
            raise TaskNotFoundError(task_id)
        return task

    async def poll() -> dict[str, Any] | None:
        return await ctx._run_step("edda.get_task", _get)

    task = await ctx._run_step("edda.get_task", _get)  # learns the channel to watch
    return await _wait_for_terminal(ctx, tasks_channel(task["channel"]), timeout_seconds, poll)


async def wait_any_task(
    ctx: "WorkflowContext", channel: str, timeout_seconds: float | None = None
) -> dict[str, Any]:
    """Block until any task on channel reaches a terminal state (earliest
    done, else earliest failed). TimeoutError on expiry."""

    async def _settled() -> dict[str, Any] | None:
        for state in ("done", "failed"):
            tasks = await ctx.storage.list_tasks(channel, state)
            if tasks:
                return tasks[0]
        return None

    async def poll() -> dict[str, Any] | None:
        return await ctx._run_step("edda.find_settled_task", _settled)

    return await _wait_for_terminal(ctx, tasks_channel(channel), timeout_seconds, poll)
