"""Regression tests for epoch-based lock fencing.

A lock-holder identity is ``worker_id#<epoch>`` — the same worker process
acquiring the same instance in two epochs writes under different identities.
Once a new holder owns the lock, all writes made under the old identity must
be rejected.
"""

import pytest

from edda.locking import LockLostError, new_lock_identity


@pytest.mark.asyncio
async def test_epoch_identity_unique():
    wid = "worker-1"
    assert new_lock_identity(wid) != new_lock_identity(wid)
    assert new_lock_identity(wid).startswith(f"{wid}#")


@pytest.mark.asyncio
async def test_stale_epoch_cannot_update_status(sqlite_storage, create_test_instance):
    iid = await create_test_instance("wf-fence", "wf", {})

    epoch_a = new_lock_identity("worker-A")
    epoch_b = new_lock_identity("worker-B")

    assert await sqlite_storage.try_acquire_lock(iid, epoch_a)
    # Take over: release A's lease and re-acquire under B's epoch
    await sqlite_storage.release_lock(iid, epoch_a)
    assert await sqlite_storage.try_acquire_lock(iid, epoch_b)

    with pytest.raises(LockLostError):
        await sqlite_storage.update_instance_status(iid, "failed", lock_id=epoch_a)
    # New holder can still write
    await sqlite_storage.update_instance_status(iid, "running", lock_id=epoch_b)


@pytest.mark.asyncio
async def test_stale_epoch_cannot_append_history(sqlite_storage, create_test_instance):
    iid = await create_test_instance("wf-fence-hist", "wf", {})

    epoch_a = new_lock_identity("worker-A")
    epoch_b = new_lock_identity("worker-A")  # same worker, new epoch

    assert await sqlite_storage.try_acquire_lock(iid, epoch_a)
    await sqlite_storage.release_lock(iid, epoch_a)
    assert await sqlite_storage.try_acquire_lock(iid, epoch_b)

    with pytest.raises(LockLostError):
        await sqlite_storage.append_history(
            iid, "a:1", "ActivityCompleted", {"r": 1}, lock_id=epoch_a
        )
    await sqlite_storage.append_history(iid, "a:1", "ActivityCompleted", {"r": 1}, lock_id=epoch_b)


@pytest.mark.asyncio
async def test_stale_epoch_cannot_update_activity(sqlite_storage, create_test_instance):
    iid = await create_test_instance("wf-fence-act", "wf", {})

    epoch_a = new_lock_identity("worker-A")
    await sqlite_storage.release_lock(iid, "plain-holder")
    assert await sqlite_storage.try_acquire_lock(iid, epoch_a)
    await sqlite_storage.release_lock(iid, epoch_a)

    with pytest.raises(LockLostError):
        await sqlite_storage.update_instance_activity(iid, "x:1", lock_id=epoch_a)


@pytest.mark.asyncio
async def test_reacquire_same_worker_different_epoch(sqlite_storage, create_test_instance):
    """Same worker losing then re-acquiring gets a different holder identity."""
    iid = await create_test_instance("wf-reacq", "wf", {})

    epoch_1 = new_lock_identity("worker-A")
    assert await sqlite_storage.try_acquire_lock(iid, epoch_1)
    await sqlite_storage.release_lock(iid, epoch_1)

    epoch_2 = new_lock_identity("worker-A")
    assert await sqlite_storage.try_acquire_lock(iid, epoch_2)
    # Old epoch fenced, new epoch writes
    with pytest.raises(LockLostError):
        await sqlite_storage.update_instance_status(iid, "failed", lock_id=epoch_1)
    await sqlite_storage.update_instance_status(iid, "running", lock_id=epoch_2)


@pytest.mark.asyncio
async def test_claim_resumable_atomic(sqlite_storage, create_test_instance):
    """claim_resumable_workflows claims and locks in one step."""
    iid = await create_test_instance("wf-claim", "wf", {})
    other = await create_test_instance("wf-claim2", "wf", {})
    # claim_resumable_workflows only picks up status='running'
    await sqlite_storage.update_instance_status(iid, "running")
    await sqlite_storage.update_instance_status(other, "running")

    epoch = new_lock_identity("worker-A")
    claimed = await sqlite_storage.claim_resumable_workflows(worker_id=epoch, limit=10)
    ids = {r["instance_id"] for r in claimed}
    assert iid in ids and other in ids

    # Rows are already locked under our epoch — a second claimer gets nothing
    claimed2 = await sqlite_storage.claim_resumable_workflows(
        worker_id=new_lock_identity("worker-B"), limit=10
    )
    assert claimed2 == []

    # Fenced write works under the claim identity; release frees it
    await sqlite_storage.update_instance_status(iid, "running", lock_id=epoch)
    await sqlite_storage.release_lock(iid, epoch)
    claimed3 = await sqlite_storage.claim_resumable_workflows(
        worker_id=new_lock_identity("worker-B"), limit=10
    )
    assert [r["instance_id"] for r in claimed3] == [iid]
