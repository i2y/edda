"""Task primitive (Vercel Board-style): durable, claimable units of work
posted to a channel. Workers compete for tasks via ``claim_task`` — the
competing-claim machinery applies; the task ROW is the source of truth.

Typical worker loop::

    while True:
        task = await claim_task(ctx, "tasks:build")
        if task is None:
            await receive(ctx, "tasks:build")  # block for wake notice
            continue
        result = do_work(task["data"])
        await complete_task(ctx, task["id"], result)
"""

import json
import time
import uuid
from typing import TYPE_CHECKING, Any

from edda.channels import receive, subscribe
from edda.exceptions import TaskNotFoundError

if TYPE_CHECKING:
    from edda.context import WorkflowContext


def tasks_channel(channel: str) -> str:
    """Task-event stream name: all lifecycle notices (posted/claimed/
    done/failed) are published on this broadcast channel — workers and
    waiters subscribe here."""
    return f"{channel}:tasks"


async def post_task(ctx: "WorkflowContext", channel: str, data: dict[str, Any] | str) -> str:
    """Create a task on channel and publish a "task_posted" notice on
    tasks_channel(channel). Returns the task ID."""
    task_id = f"task-{uuid.uuid4()}"
    payload = data if isinstance(data, str) else json.dumps(data)
    await ctx.storage.post_task(task_id, channel, payload)
    return task_id


async def claim_task(ctx: "WorkflowContext", channel: str) -> dict[str, Any] | None:
    """Atomically claim the oldest open task on channel for this instance.

    Returns the claimed task dict (id/channel/data/state/claimed_by), or
    None when no open task exists.
    """
    return await ctx.storage.claim_task(channel, ctx.instance_id)


async def complete_task(
    ctx: "WorkflowContext", task_id: str, result: dict[str, Any] | str | None = None
) -> None:
    """Mark a claimed task done. Only the claimant may complete —
    raises TaskConflictError otherwise."""
    payload = result if isinstance(result, str) or result is None else json.dumps(result)
    await ctx.storage.complete_task(task_id, ctx.instance_id, payload, failed=False)


async def fail_task(
    ctx: "WorkflowContext", task_id: str, result: dict[str, Any] | str | None = None
) -> None:
    """Mark a claimed task failed. Only the claimant may fail it."""
    payload = result if isinstance(result, str) or result is None else json.dumps(result)
    await ctx.storage.complete_task(task_id, ctx.instance_id, payload, failed=True)


async def list_tasks(
    ctx: "WorkflowContext", channel: str, state: str | None = None
) -> list[dict[str, Any]]:
    """Return tasks on channel filtered by state (None = all)."""
    return await ctx.storage.list_tasks(channel, state)


async def read_channel_since(
    ctx: "WorkflowContext", channel: str, message_id: str | None = None
) -> list[dict[str, Any]]:
    """Return messages on channel published after the message_id cursor
    (None reads all, oldest first). Use the last message's ``message_id``
    as the next cursor for incremental polling."""
    return await ctx.storage.channel_messages_since(channel, message_id)


async def wait_task(
    ctx: "WorkflowContext", task_id: str, timeout_seconds: float | None = None
) -> dict[str, Any]:
    """Block until the task reaches a terminal state (done or failed).

    Subscribes broadcast to the channel's task-event stream and re-checks
    the task row on every wake — the workflow durably suspends (lock
    released) between notices.

    Raises TaskNotFoundError for unknown IDs, TimeoutError on expiry.
    """
    task = await ctx.storage.get_task(task_id)
    if task is None:
        raise TaskNotFoundError(task_id)
    notify_ch = tasks_channel(task["channel"])
    deadline = time.monotonic() + timeout_seconds if timeout_seconds is not None else None
    while task["state"] not in ("done", "failed"):
        await subscribe(ctx, notify_ch, mode="broadcast")
        remaining = None if deadline is None else deadline - time.monotonic()
        if remaining is not None and remaining <= 0:
            raise TimeoutError(f"timeout waiting for task {task_id}")
        try:
            await receive(
                ctx,
                notify_ch,
                timeout_seconds=None if remaining is None else int(remaining) + 1,
            )
        except TimeoutError:
            if deadline is None or time.monotonic() < deadline:
                continue
            task = await ctx.storage.get_task(task_id)
            if task is not None and task["state"] in ("done", "failed"):
                return task
            raise
        got = await ctx.storage.get_task(task_id)
        if got is None:
            raise TaskNotFoundError(task_id)
        task = got
    return task


async def wait_any_task(
    ctx: "WorkflowContext", channel: str, timeout_seconds: float | None = None
) -> dict[str, Any]:
    """Block until any task on channel reaches a terminal state (earliest
    done, else earliest failed). TimeoutError on expiry."""
    notify_ch = tasks_channel(channel)
    deadline = time.monotonic() + timeout_seconds if timeout_seconds is not None else None

    async def _terminal() -> dict[str, Any] | None:
        for state in ("done", "failed"):
            tasks = await ctx.storage.list_tasks(channel, state)
            if tasks:
                return tasks[0]
        return None

    while True:
        task = await _terminal()
        if task is not None:
            return task
        await subscribe(ctx, notify_ch, mode="broadcast")
        remaining = None if deadline is None else deadline - time.monotonic()
        if remaining is not None and remaining <= 0:
            raise TimeoutError(f"timeout waiting for task on {channel}")
        try:
            await receive(
                ctx,
                notify_ch,
                timeout_seconds=None if remaining is None else int(remaining) + 1,
            )
        except TimeoutError:
            if deadline is None or time.monotonic() < deadline:
                continue
            task = await _terminal()
            if task is not None:
                return task
            raise
