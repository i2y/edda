"""Tests for the shared actor filesystem (CAS + fs_write capability)."""

import pytest

from edda import actors, fs
from edda.context import WorkflowContext
from edda.exceptions import FSConflictError, FSForbiddenError, FSOutsideActivityError


def _ctx(storage, iid="fs-actor-1"):
    return WorkflowContext(instance_id=iid, workflow_name="wf", storage=storage, worker_id="w1")


def _in_activity(ctx):
    ctx._activity_depth = 1
    return ctx


@pytest.mark.asyncio
async def test_fs_cas_roundtrip(sqlite_storage):
    assert (await sqlite_storage.fs_get("/shared/x")) is None
    await sqlite_storage.fs_put("/shared/x", b"v1", "wf-1", 0)
    with pytest.raises(FSConflictError):
        await sqlite_storage.fs_put("/shared/x", b"x", "wf-1", 0)
    obj = await sqlite_storage.fs_get("/shared/x")
    assert obj["data"] == b"v1" and obj["version"] == 1
    with pytest.raises(FSConflictError):
        await sqlite_storage.fs_put("/shared/x", b"v2", "wf-1", 9)
    await sqlite_storage.fs_put("/shared/x", b"v2", "wf-1", 1)
    assert (await sqlite_storage.fs_get("/shared/x"))["version"] == 2
    with pytest.raises(FSConflictError):
        await sqlite_storage.fs_delete("/shared/x", 1)
    await sqlite_storage.fs_delete("/shared/x", 2)
    assert (await sqlite_storage.fs_get("/shared/x")) is None


@pytest.mark.asyncio
async def test_fs_outside_activity_rejected(sqlite_storage):
    ctx = _ctx(sqlite_storage)
    with pytest.raises(FSOutsideActivityError):
        await fs.fs_write(ctx, "/actor/fs-actor-1/f", b"d", 0)
    with pytest.raises(FSOutsideActivityError):
        await fs.fs_read(ctx, "/shared/f")


@pytest.mark.asyncio
async def test_fs_permissions(sqlite_storage):
    me = _in_activity(_ctx(sqlite_storage, "actor-a"))
    other = _in_activity(_ctx(sqlite_storage, "actor-b"))

    # own private path OK
    await fs.fs_write(me, "/actor/actor-a/notes", b"n", 0)
    data, ver = await fs.fs_read(me, "/actor/actor-a/notes")
    assert data == b"n" and ver == 1

    # other actor's private path forbidden (read + write)
    with pytest.raises(FSForbiddenError):
        await fs.fs_write(other, "/actor/actor-a/notes", b"x", 1)
    with pytest.raises(FSForbiddenError):
        await fs.fs_read(other, "/actor/actor-a/notes")

    # shared write requires capability
    with pytest.raises(FSForbiddenError):
        await fs.fs_write(me, "/shared/doc", b"s", 0)
    token = await actors.issue_capability(me, "fs_write", "/shared/")
    await fs.fs_write(me, "/shared/doc", b"s", 0, capability_token=token)
    data, _ = await fs.fs_read(other, "/shared/doc")  # shared read open
    assert data == b"s"

    # bad path rejected
    with pytest.raises(FSForbiddenError):
        await fs.fs_write(me, "/etc/passwd", b"x", 0)
