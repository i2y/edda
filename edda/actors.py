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

import hashlib
import secrets
import uuid
from typing import TYPE_CHECKING

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


_CAP_KINDS = {"actor", "channel_pub", "channel_sub", "fs_write"}


def _hash_secret(secret: str) -> str:
    return hashlib.sha256(secret.encode()).hexdigest()


async def issue_capability(
    ctx: "WorkflowContext", kind: str, target: str, expires_at: str | None = None
) -> tuple[str, str]:
    """
    Mint a capability for (kind, target). Returns ``(token, capability_id)``
    — the bearer token ``cap:{kind}:{target}:{secret}`` and the grant ID
    needed by ``revoke_capability``. Only the hash of the secret is stored;
    the plaintext is returned exactly once here.
    """
    if kind not in _CAP_KINDS:
        raise ValueError(f"unknown capability kind: {kind}")
    secret = secrets.token_hex(24)
    cap_id = f"cap-{uuid.uuid4()}"
    await ctx.storage.grant_capability(
        capability_id=cap_id,
        kind=kind,
        target=target,
        secret_hash=_hash_secret(secret),
        granted_by=ctx.instance_id,
        expires_at=expires_at,
    )
    return f"cap:{kind}:{target}:{secret}", cap_id


async def check_capability(ctx: "WorkflowContext", kind: str, target: str, token: str) -> bool:
    """
    Verify a capability token for (kind, target). Accepts either the raw
    secret or the full ``cap:kind:target:secret`` token form.
    """
    if token.startswith("cap:"):
        token = token.rsplit(":", 1)[-1]
    return await ctx.storage.check_capability(kind, target, _hash_secret(token))


async def revoke_capability(ctx: "WorkflowContext", capability_id: str) -> None:
    """Remove a capability grant by ID (idempotent)."""
    await ctx.storage.revoke_capability(capability_id)
