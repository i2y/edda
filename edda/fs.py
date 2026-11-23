"""Shared actor filesystem: versioned files under "/actor/{id}/" (private)
and "/shared/" (common space).

Writes are CAS on version — ``fs_read`` returns the version, pass it to
``fs_write`` as ``expected_version``; a mismatch raises FSConflictError.

Permission model:
  - ``/actor/{id}/...`` : only the owning instance may read or write
    (ownership derived from the path — no capability needed)
  - ``/shared/...``     : read is open; write requires an ``fs_write``
    capability token for the path or for "/shared/"

FS operations are non-deterministic I/O and MUST run inside an activity;
calls outside an activity (or during replay) raise FSOutsideActivityError.
"""

from typing import TYPE_CHECKING

from edda.actors import check_capability
from edda.exceptions import FSForbiddenError, FSOutsideActivityError

if TYPE_CHECKING:
    from edda.context import WorkflowContext


def _check_activity(ctx: "WorkflowContext") -> None:
    if ctx.is_replaying or ctx._activity_depth == 0:
        raise FSOutsideActivityError("fs operations must run inside an activity")


async def _authorize(ctx: "WorkflowContext", path: str, capability_token: str | None) -> None:
    if path.startswith("/actor/"):
        owner = path[len("/actor/") :].split("/", 1)[0]
        if owner != ctx.instance_id:
            raise FSForbiddenError(f"/actor/{owner} is owned by another instance")
        return
    if path.startswith("/shared/"):
        if not capability_token:
            raise FSForbiddenError("/shared/ writes require an fs_write capability")
        ok = await check_capability(ctx, "fs_write", path, capability_token)
        if not ok:
            ok = await check_capability(ctx, "fs_write", "/shared/", capability_token)
        if not ok:
            raise FSForbiddenError(f"invalid fs_write capability for {path}")
        return
    raise FSForbiddenError("path must be under /actor/{id}/ or /shared/")


async def fs_read(ctx: "WorkflowContext", path: str) -> tuple[bytes | None, int]:
    """
    Read path, returning (data, version); (None, 0) when absent.
    Private actor paths may only be read by their owner; /shared/ is open.
    """
    _check_activity(ctx)
    if path.startswith("/actor/"):
        owner = path[len("/actor/") :].split("/", 1)[0]
        if owner != ctx.instance_id:
            raise FSForbiddenError("cannot read another actor's private path")
    obj = await ctx.storage.fs_get(path)
    if obj is None:
        return None, 0
    return obj["data"], obj["version"]


async def fs_write(
    ctx: "WorkflowContext",
    path: str,
    data: bytes,
    expected_version: int,
    capability_token: str | None = None,
) -> None:
    """
    CAS write: expected_version 0 creates (fails if exists), >0 updates
    only if the stored version matches. Raises FSConflictError on mismatch.
    """
    _check_activity(ctx)
    await _authorize(ctx, path, capability_token)
    await ctx.storage.fs_put(path, data, ctx.instance_id, expected_version)


async def fs_delete(
    ctx: "WorkflowContext",
    path: str,
    expected_version: int,
    capability_token: str | None = None,
) -> None:
    """Delete path (expected_version 0 = unconditional)."""
    _check_activity(ctx)
    await _authorize(ctx, path, capability_token)
    await ctx.storage.fs_delete(path, expected_version)
