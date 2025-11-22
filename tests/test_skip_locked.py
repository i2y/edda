"""
Tests for SELECT FOR UPDATE SKIP LOCKED behavior.

This module tests that SKIP LOCKED prevents blocking when multiple workers
try to acquire locks simultaneously.

Note: These tests primarily target PostgreSQL and MySQL, as SQLite has limited
support for SELECT FOR UPDATE SKIP LOCKED due to its table-level locking mechanism.
"""

import asyncio
import time

import pytest

from edda.locking import generate_worker_id


@pytest.mark.skip(reason="SQLite does not fully support SELECT FOR UPDATE SKIP LOCKED")
class TestSkipLockedBehavior:
    """Tests for SKIP LOCKED preventing blocking (PostgreSQL/MySQL only)."""

    async def test_skip_locked_no_blocking(
        self, sqlite_storage, sample_workflow_data, create_test_instance
    ):
        """Test that SKIP LOCKED prevents blocking when lock is already held."""
        await create_test_instance(**sample_workflow_data)

        worker_id_A = generate_worker_id("worker-A")
        worker_id_B = generate_worker_id("worker-B")

        # Worker A acquires lock
        acquired_A = await sqlite_storage.try_acquire_lock(
            sample_workflow_data["instance_id"], worker_id_A, timeout_seconds=300
        )
        assert acquired_A is True

        # Worker B attempts to acquire lock - should return False immediately (no blocking)
        start_time = time.time()
        acquired_B = await sqlite_storage.try_acquire_lock(
            sample_workflow_data["instance_id"], worker_id_B, timeout_seconds=300
        )
        elapsed_time = time.time() - start_time

        assert acquired_B is False
        # Should return immediately (< 100ms) without blocking
        assert elapsed_time < 0.1, f"Lock acquisition took {elapsed_time:.3f}s, expected < 0.1s"

        # Release lock from worker A
        await sqlite_storage.release_lock(sample_workflow_data["instance_id"], worker_id_A)

    async def test_skip_locked_concurrent_acquisition(
        self, sqlite_storage, sample_workflow_data, create_test_instance
    ):
        """Test that only one worker can acquire lock when multiple workers try simultaneously."""
        await create_test_instance(**sample_workflow_data)

        worker_id_A = generate_worker_id("worker-A")
        worker_id_B = generate_worker_id("worker-B")
        worker_id_C = generate_worker_id("worker-C")

        # All workers try to acquire lock simultaneously
        results = await asyncio.gather(
            sqlite_storage.try_acquire_lock(
                sample_workflow_data["instance_id"], worker_id_A, timeout_seconds=300
            ),
            sqlite_storage.try_acquire_lock(
                sample_workflow_data["instance_id"], worker_id_B, timeout_seconds=300
            ),
            sqlite_storage.try_acquire_lock(
                sample_workflow_data["instance_id"], worker_id_C, timeout_seconds=300
            ),
        )

        # Exactly one should succeed
        success_count = sum(results)
        assert success_count == 1, f"Expected 1 success, got {success_count}"

        # Find which worker succeeded
        if results[0]:
            winner = worker_id_A
        elif results[1]:
            winner = worker_id_B
        else:
            winner = worker_id_C

        # Verify instance is locked by winner
        instance = await sqlite_storage.get_instance(sample_workflow_data["instance_id"])
        assert instance["locked_by"] == winner

        # Release lock
        await sqlite_storage.release_lock(sample_workflow_data["instance_id"], winner)

    async def test_skip_locked_parallel_workflows(
        self, sqlite_storage, sample_workflow_data, create_test_instance
    ):
        """Test that different workers can process different workflows in parallel."""
        # Create two workflow instances
        instance_id_1 = sample_workflow_data["instance_id"]
        instance_id_2 = f"{sample_workflow_data['instance_id']}-2"

        await create_test_instance(**sample_workflow_data)
        await create_test_instance(
            instance_id=instance_id_2,
            workflow_name=sample_workflow_data["workflow_name"],
            source_hash=sample_workflow_data["source_hash"],
            owner_service=sample_workflow_data["owner_service"],
            input_data=sample_workflow_data["input_data"],
            step=sample_workflow_data["step"],
        )

        worker_id_A = generate_worker_id("worker-A")
        worker_id_B = generate_worker_id("worker-B")

        # Worker A locks instance 1
        acquired_A = await sqlite_storage.try_acquire_lock(
            instance_id_1, worker_id_A, timeout_seconds=300
        )
        assert acquired_A is True

        # Worker B should be able to lock instance 2 (different workflow)
        acquired_B = await sqlite_storage.try_acquire_lock(
            instance_id_2, worker_id_B, timeout_seconds=300
        )
        assert acquired_B is True

        # Both workflows are locked by different workers
        instance_1 = await sqlite_storage.get_instance(instance_id_1)
        instance_2 = await sqlite_storage.get_instance(instance_id_2)

        assert instance_1["locked_by"] == worker_id_A
        assert instance_2["locked_by"] == worker_id_B

        # Release locks
        await sqlite_storage.release_lock(instance_id_1, worker_id_A)
        await sqlite_storage.release_lock(instance_id_2, worker_id_B)

    async def test_skip_locked_after_lock_release(
        self, sqlite_storage, sample_workflow_data, create_test_instance
    ):
        """Test that lock can be acquired immediately after release."""
        await create_test_instance(**sample_workflow_data)

        worker_id_A = generate_worker_id("worker-A")
        worker_id_B = generate_worker_id("worker-B")

        # Worker A acquires and releases lock
        acquired_A = await sqlite_storage.try_acquire_lock(
            sample_workflow_data["instance_id"], worker_id_A, timeout_seconds=300
        )
        assert acquired_A is True

        await sqlite_storage.release_lock(sample_workflow_data["instance_id"], worker_id_A)

        # Worker B should be able to acquire lock immediately
        start_time = time.time()
        acquired_B = await sqlite_storage.try_acquire_lock(
            sample_workflow_data["instance_id"], worker_id_B, timeout_seconds=300
        )
        elapsed_time = time.time() - start_time

        assert acquired_B is True
        # Should be very fast (< 100ms)
        assert elapsed_time < 0.1, f"Lock acquisition took {elapsed_time:.3f}s, expected < 0.1s"

        # Verify instance is locked by worker B
        instance = await sqlite_storage.get_instance(sample_workflow_data["instance_id"])
        assert instance["locked_by"] == worker_id_B

        # Release lock
        await sqlite_storage.release_lock(sample_workflow_data["instance_id"], worker_id_B)

    async def test_skip_locked_stale_lock_acquisition(
        self, sqlite_storage, sample_workflow_data, create_test_instance
    ):
        """Test that stale locks can be acquired by another worker."""
        from datetime import UTC, datetime, timedelta

        await create_test_instance(**sample_workflow_data)

        worker_id_A = generate_worker_id("worker-A")
        worker_id_B = generate_worker_id("worker-B")

        # Worker A acquires lock
        acquired_A = await sqlite_storage.try_acquire_lock(
            sample_workflow_data["instance_id"], worker_id_A, timeout_seconds=300
        )
        assert acquired_A is True

        # Simulate stale lock by manually updating locked_at to an old timestamp
        from sqlalchemy import update
        from sqlalchemy.ext.asyncio import AsyncSession

        from edda.storage.models import WorkflowInstance

        async with AsyncSession(sqlite_storage.engine) as session:
            # Set locked_at to 6 minutes ago (beyond 5-minute timeout)
            old_timestamp = datetime.now(UTC).replace(tzinfo=None) - timedelta(minutes=6)
            await session.execute(
                update(WorkflowInstance)
                .where(WorkflowInstance.instance_id == sample_workflow_data["instance_id"])
                .values(locked_at=old_timestamp)
            )
            await session.commit()

        # Worker B should be able to acquire the stale lock immediately
        start_time = time.time()
        acquired_B = await sqlite_storage.try_acquire_lock(
            sample_workflow_data["instance_id"], worker_id_B, timeout_seconds=300
        )
        elapsed_time = time.time() - start_time

        assert acquired_B is True
        # Should be very fast (< 100ms) - no blocking
        assert elapsed_time < 0.1, f"Lock acquisition took {elapsed_time:.3f}s, expected < 0.1s"

        # Verify instance is now locked by worker B
        instance = await sqlite_storage.get_instance(sample_workflow_data["instance_id"])
        assert instance["locked_by"] == worker_id_B

        # Release lock
        await sqlite_storage.release_lock(sample_workflow_data["instance_id"], worker_id_B)


class TestSkipLockedMultiDatabase:
    """Test SKIP LOCKED behavior across PostgreSQL and MySQL."""

    async def test_skip_locked_no_blocking_all_databases(self, db_storage, sample_workflow_data):
        """Test SKIP LOCKED across PostgreSQL and MySQL."""
        # This test will run multiple times (once for each database)
        # due to the parametrized db_storage fixture
        # Note: SQLite tests are skipped in the fixture if PostgreSQL/MySQL is not available

        # Create instance directly using db_storage (no create_test_instance fixture)
        await db_storage.create_instance(
            instance_id=sample_workflow_data["instance_id"],
            workflow_name=sample_workflow_data["workflow_name"],
            source_hash=sample_workflow_data["source_hash"],
            owner_service=sample_workflow_data["owner_service"],
            input_data=sample_workflow_data["input_data"],
        )

        worker_id_A = generate_worker_id("worker-A")
        worker_id_B = generate_worker_id("worker-B")

        # Worker A acquires lock
        acquired_A = await db_storage.try_acquire_lock(
            sample_workflow_data["instance_id"], worker_id_A, timeout_seconds=300
        )
        assert acquired_A is True

        # Worker B should fail immediately without blocking
        start_time = time.time()
        acquired_B = await db_storage.try_acquire_lock(
            sample_workflow_data["instance_id"], worker_id_B, timeout_seconds=300
        )
        elapsed_time = time.time() - start_time

        assert acquired_B is False
        # Should return immediately (< 100ms)
        assert elapsed_time < 0.1, f"Lock acquisition took {elapsed_time:.3f}s, expected < 0.1s"

        # Release lock
        await db_storage.release_lock(sample_workflow_data["instance_id"], worker_id_A)
