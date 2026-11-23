"""Tests for capability primitives (grant/check/revoke/issue)."""

import hashlib
from datetime import UTC, datetime, timedelta

import pytest

from edda import actors


def _hash(s: str) -> str:
    return hashlib.sha256(s.encode()).hexdigest()


@pytest.mark.asyncio
async def test_capability_roundtrip(sqlite_storage):
    await sqlite_storage.grant_capability("cap-1", "channel_pub", "orders", _hash("sekret"))
    assert await sqlite_storage.check_capability("channel_pub", "orders", _hash("sekret"))
    assert not await sqlite_storage.check_capability("channel_pub", "orders", _hash("wrong"))
    assert not await sqlite_storage.check_capability("channel_sub", "orders", _hash("sekret"))
    assert not await sqlite_storage.check_capability("channel_pub", "other", _hash("sekret"))

    await sqlite_storage.revoke_capability("cap-1")
    assert not await sqlite_storage.check_capability("channel_pub", "orders", _hash("sekret"))
    await sqlite_storage.revoke_capability("cap-1")  # idempotent


@pytest.mark.asyncio
async def test_capability_expiry(sqlite_storage):
    past = (datetime.now(UTC) - timedelta(hours=1)).isoformat()
    await sqlite_storage.grant_capability(
        "cap-x", "channel_pub", "orders", _hash("s2"), expires_at=past
    )
    assert not await sqlite_storage.check_capability("channel_pub", "orders", _hash("s2"))


@pytest.mark.asyncio
async def test_issue_and_check_token(sqlite_storage):
    from edda.context import WorkflowContext

    ctx = WorkflowContext(
        instance_id="cap-wf-1",
        workflow_name="wf",
        storage=sqlite_storage,
        worker_id="w1",
    )
    workflow_context = ctx
    token = await actors.issue_capability(workflow_context, "channel_pub", "orders")
    kind, target, secret = token.split(":")[1], token.split(":")[2], token.split(":")[3]
    assert kind == "channel_pub" and target == "orders"
    assert await actors.check_capability(workflow_context, "channel_pub", "orders", token)
    # raw secret form also accepted
    assert await actors.check_capability(workflow_context, "channel_pub", "orders", secret)
    assert not await actors.check_capability(workflow_context, "channel_pub", "orders", "bogus")


@pytest.mark.asyncio
async def test_issue_rejects_unknown_kind(sqlite_storage):
    from edda.context import WorkflowContext

    workflow_context = WorkflowContext(
        instance_id="cap-wf-2",
        workflow_name="wf",
        storage=sqlite_storage,
        worker_id="w1",
    )
    with pytest.raises(ValueError, match="unknown capability kind"):
        await actors.issue_capability(workflow_context, "root", "everything")
