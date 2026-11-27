"""
Actor-style API: name registry (Erlang register/whereis) and monitors
(Erlang monitor/'DOWN' messages) layered on the durable kernel.

A DOWN notification is delivered as a channel message
``{"type": "DOWN", "actor": "<instance_id>", "status": "<terminal_status>"}``
on the watcher's direct channel ``"{channel}:{watcher_instance_id}"`` —
subscribe with ``mode="direct"`` on the channel to receive it, exactly as
with ``send_to()``. Emission is performed by the leader's periodic sweep,
so a notification arrives at most one sweep interval after termination.

Every function here is replay-safe (SEMANTICS §2): called from workflow code
it runs once as a recorded step, and replays return the recorded result or
error; inside an activity body it runs directly, as part of the activity.
An effect (register, monitor, issue, revoke) commits together with its
record, under the run's lock.
"""

import hashlib
import hmac
import secrets
import uuid
from typing import TYPE_CHECKING, Any

from edda.exceptions import InvalidArgumentError, SandboxedError

if TYPE_CHECKING:
    from edda.context import WorkflowContext


async def register_name(ctx: "WorkflowContext", name: str) -> None:
    """
    Bind a unique actor name to this workflow instance.

    Registering a name this instance already holds is a no-op.

    Args:
        ctx: Workflow context
        name: Unique actor name

    Raises:
        ValueError: If the name is already registered by another instance
    """

    async def _register() -> None:
        await ctx.storage.register_actor_name(name, ctx.instance_id, framework="python")

    await ctx._run_step("edda.register_name", _register, effect=True)


async def unregister_name(ctx: "WorkflowContext", name: str) -> None:
    """Remove an actor name binding held by this instance (idempotent).

    Raises:
        NotOwnerError: If another instance holds the name
    """

    async def _unregister() -> None:
        await ctx.storage.unregister_own_actor_name(name, ctx.instance_id)

    await ctx._run_step("edda.unregister_name", _unregister, effect=True)


async def whereis(ctx: "WorkflowContext", name: str) -> str | None:
    """
    Resolve an actor name to an instance ID.

    Returns:
        Instance ID, or None if the name is unbound
    """

    async def _whereis() -> str | None:
        return await ctx.storage.whereis_actor(name)

    return await ctx._run_step("edda.whereis", _whereis)


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

    async def _monitor() -> None:
        await ctx.storage.monitor_actor(ctx.instance_id, target_instance_id, down_channel)

    await ctx._run_step("edda.monitor", _monitor, effect=True)


async def unmonitor(ctx: "WorkflowContext", target_instance_id: str) -> None:
    """Remove a monitor registration (idempotent)."""

    async def _unmonitor() -> None:
        await ctx.storage.unmonitor_actor(ctx.instance_id, target_instance_id)

    await ctx._run_step("edda.unmonitor", _unmonitor, effect=True)


_CAP_KINDS = {"actor", "channel_pub", "channel_sub", "fs_write"}


def _hash_secret(secret: str) -> str:
    return hashlib.sha256(secret.encode()).hexdigest()


def _derive_secret(key: bytes, capability_id: str) -> str:
    return hmac.new(key, capability_id.encode(), hashlib.sha256).hexdigest()


async def issue_capability(
    ctx: "WorkflowContext", kind: str, target: str, expires_at: str | None = None
) -> tuple[str, str]:
    """
    Mint a capability for (kind, target). Returns ``(token, capability_id)``
    — the bearer token ``cap:{kind}:{target}:{secret}`` and the grant ID
    needed by ``revoke_capability``. Only the hash of the secret is stored.

    With a capability key (``EddaApp(capability_key=...)`` or the
    ``DURAX_CAPABILITY_KEY`` environment variable) the secret is the HMAC of
    the capability ID and history records only ``{"id", "kind", "target",
    "derived": true}``: a replay derives the token again, so no token is ever
    written to history. Without a key the token itself is recorded in the
    step and stays readable in history. Keep the key the same on every worker
    while instances that issued capabilities are alive.
    """

    async def _issue() -> tuple[str, str] | dict[str, Any]:
        if kind not in _CAP_KINDS:
            raise InvalidArgumentError(f"unknown capability kind: {kind}")
        instance = await ctx.storage.get_instance(ctx.instance_id)
        if instance is None:
            raise ValueError(f"workflow instance {ctx.instance_id} not found")
        if instance.get("sandboxed"):
            raise SandboxedError(f"sandboxed workflow {ctx.instance_id} cannot issue capabilities")
        cap_id = f"cap-{uuid.uuid4()}"
        key = ctx._capability_key
        secret = _derive_secret(key, cap_id) if key else secrets.token_hex(24)
        await ctx.storage.grant_capability(
            capability_id=cap_id,
            kind=kind,
            target=target,
            secret_hash=_hash_secret(secret),
            granted_by=ctx.instance_id,
            expires_at=expires_at,
        )
        if key:
            return {"id": cap_id, "kind": kind, "target": target, "derived": True}
        return f"cap:{kind}:{target}:{secret}", cap_id

    # A replay returns the recorded value: a pair (a list in JSON history), or
    # the derived record, from which the token is rebuilt with the key.
    recorded = await ctx._run_step("edda.issue_capability", _issue, effect=True)
    if isinstance(recorded, dict):
        key = ctx._capability_key
        if not key:
            raise ValueError(
                "a capability key is required to replay a derived capability "
                "(EddaApp(capability_key=...) or DURAX_CAPABILITY_KEY)"
            )
        cap_id = recorded["id"]
        secret = _derive_secret(key, cap_id)
        return f"cap:{recorded['kind']}:{recorded['target']}:{secret}", cap_id
    token, cap_id = recorded
    return token, cap_id


async def check_capability(ctx: "WorkflowContext", kind: str, target: str, token: str) -> bool:
    """
    Verify a capability token for (kind, target). Accepts either the raw
    secret or the full ``cap:kind:target:secret`` token form.
    """
    if token.startswith("cap:"):
        token = token.rsplit(":", 1)[-1]
    secret_hash = _hash_secret(token)

    async def _check() -> bool:
        return await ctx.storage.check_capability(kind, target, secret_hash)

    return await ctx._run_step("edda.check_capability", _check)


async def revoke_capability(ctx: "WorkflowContext", capability_id: str) -> None:
    """Remove a capability grant issued by this instance (idempotent).

    Raises:
        NotOwnerError: If another instance issued the grant
    """

    async def _revoke() -> None:
        await ctx.storage.revoke_own_capability(capability_id, ctx.instance_id)

    await ctx._run_step("edda.revoke_capability", _revoke, effect=True)
