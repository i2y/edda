"""
Actor-style API: name registry (Erlang register/whereis) and monitors
(Erlang monitor/'DOWN' messages) layered on the durable kernel.

A DOWN notification is delivered as a channel message
``{"type": "DOWN", "actor": "<instance_id>", "status": "<terminal_status>"}``
on the watcher's direct channel ``"{channel}:{watcher_instance_id}"`` —
subscribe with ``mode="direct"`` on the channel to receive it, exactly as
with ``send_to()``. Emission is performed by the leader's periodic sweep,
so a notification arrives at most one sweep interval after termination.
"""

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from edda.context import WorkflowContext


async def register_name(ctx: "WorkflowContext", name: str) -> None:
    """
    Bind a unique actor name to this workflow instance.

    Args:
        ctx: Workflow context
        name: Unique actor name

    Raises:
        Exception: If the name is already registered by another instance
    """
    await ctx.storage.register_actor_name(name, ctx.instance_id, framework="python")


async def unregister_name(ctx: "WorkflowContext", name: str) -> None:
    """Remove an actor name binding (idempotent)."""
    await ctx.storage.unregister_actor_name(name)


async def whereis(ctx: "WorkflowContext", name: str) -> str | None:
    """
    Resolve an actor name to an instance ID.

    Returns:
        Instance ID, or None if the name is unbound
    """
    return await ctx.storage.whereis_actor(name)


async def monitor(ctx: "WorkflowContext", target_instance_id: str, channel: str) -> None:
    """
    Register a monitor on target_instance_id. When the target reaches a
    terminal status, a DOWN message is published to this instance's direct
    channel ``"{channel}:{instance_id}"`` — the same channel ``send_to()``
    uses, so receive it via ``receive()`` on ``channel`` with a ``direct``
    subscription.

    Args:
        ctx: Workflow context
        target_instance_id: Actor to monitor
        channel: Channel name for DOWN delivery (direct channel is used)
    """
    down_channel = f"{channel}:{ctx.instance_id}"
    await ctx.storage.monitor_actor(ctx.instance_id, target_instance_id, down_channel)


async def unmonitor(ctx: "WorkflowContext", target_instance_id: str) -> None:
    """Remove a monitor registration (idempotent)."""
    await ctx.storage.unmonitor_actor(ctx.instance_id, target_instance_id)
