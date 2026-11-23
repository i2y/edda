"""Tests for the content-addressed blob store."""

import hashlib

import pytest

from edda import blobs
from edda.context import WorkflowContext


@pytest.mark.asyncio
async def test_put_get_blob(sqlite_storage):
    data = b"large-payload-\x00\x01-binary"
    h = hashlib.sha256(data).hexdigest()
    await sqlite_storage.put_blob(h, data, created_by="wf-1")
    await sqlite_storage.put_blob(h, data, created_by="wf-1")  # idempotent
    assert await sqlite_storage.get_blob(h) == data
    assert await sqlite_storage.get_blob("missing") is None


@pytest.mark.asyncio
async def test_store_load_payload(sqlite_storage):
    ctx = WorkflowContext(
        instance_id="blob-wf-1",
        workflow_name="wf",
        storage=sqlite_storage,
        worker_id="w1",
    )
    ref = await blobs.store_payload(ctx, b"payload-bytes")
    assert ref.startswith("blob:sha256:")
    assert await blobs.load_payload(ctx, ref) == b"payload-bytes"
    # bare hash form also resolves
    assert await blobs.load_payload(ctx, ref.removeprefix("blob:sha256:")) == b"payload-bytes"
    with pytest.raises(KeyError, match="blob not found"):
        await blobs.load_payload(ctx, "blob:sha256:deadbeef")
