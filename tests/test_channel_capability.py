"""Sealed-channel capability enforcement tests."""

import pytest

from edda.channels import _enforce_channel_capability, publish
from edda.exceptions import ChannelPermissionError

pytestmark = pytest.mark.asyncio


async def _grant(storage, kind, target, secret="s3cret"):
    from edda.actors import _hash_secret

    await storage.grant_capability(
        capability_id=f"cap-test-{kind}-{target}",
        kind=kind,
        target=target,
        secret_hash=_hash_secret(secret),
        granted_by="test",
    )
    return f"cap:{kind}:{target}:{secret}"


async def test_open_channel_allows_anonymous_publish(sqlite_storage):
    # no grants — publish without any token
    msg_id = await publish(sqlite_storage, "open-ch", {"n": 1})
    assert msg_id


async def test_sealed_publish_requires_token(sqlite_storage):
    token = await _grant(sqlite_storage, "channel_pub", "sealed-pub")

    with pytest.raises(ChannelPermissionError):
        await publish(sqlite_storage, "sealed-pub", {"n": 1})

    with pytest.raises(ChannelPermissionError):
        await publish(
            sqlite_storage,
            "sealed-pub",
            {"n": 1},
            capability_token="cap:channel_pub:sealed-pub:wrong",
        )

    msg_id = await publish(sqlite_storage, "sealed-pub", {"n": 1}, capability_token=token)
    assert msg_id


async def test_sealed_receive_requires_token(sqlite_storage):
    await _grant(sqlite_storage, "channel_sub", "sealed-sub")

    with pytest.raises(ChannelPermissionError):
        await _enforce_channel_capability(sqlite_storage, "channel_sub", "sealed-sub", None)


async def test_sealing_one_channel_does_not_affect_others(sqlite_storage):
    await _grant(sqlite_storage, "channel_pub", "other-ch")

    # plain-ch has no grants — enforcement is a no-op
    await _enforce_channel_capability(sqlite_storage, "channel_pub", "plain-ch", None)
    msg_id = await publish(sqlite_storage, "plain-ch", {"n": 1})
    assert msg_id


async def test_revoked_capability_unseals_channel(sqlite_storage):
    token = await _grant(sqlite_storage, "channel_pub", "temp-ch")
    cap_id = "cap-test-channel_pub-temp-ch"

    with pytest.raises(ChannelPermissionError):
        await publish(sqlite_storage, "temp-ch", {"n": 1})

    await sqlite_storage.revoke_capability(cap_id)
    # no grants left — open again
    msg_id = await publish(sqlite_storage, "temp-ch", {"n": 1})
    assert msg_id
    _ = token
