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

import contextlib
import json
import uuid
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from edda.context import WorkflowContext


async def post_task(ctx: "WorkflowContext", channel: str, data: dict[str, Any] | str) -> str:
    """Create a task on channel and publish a best-effort wake notice.

    Returns the task ID. The task row is the source of truth; the notice
    only wakes competing subscribers so they can claim promptly.
    """
    task_id = f"task-{uuid.uuid4()}"
    payload = data if isinstance(data, str) else json.dumps(data)
    await ctx.storage.post_task(task_id, channel, payload)
    with contextlib.suppress(Exception):
        await ctx.storage.publish_to_channel(channel, {"type": "task_posted", "task_id": task_id})
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
