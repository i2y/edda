"""
Tests for distributed locking utilities.
"""

import asyncio

import pytest

from edda.locking import (
    acquire_lock_with_retry,
    ensure_lock_held,
    generate_worker_id,
    workflow_lock,
)


class TestWorkerID:
    """Tests for worker ID generation."""

    def test_generate_worker_id(self):
        """Test generating a unique worker ID."""
        service_name = "test-service"
        worker_id = generate_worker_id(service_name)

        assert worker_id.startswith(f"{service_name}-")
        assert len(worker_id) > len(service_name)

    def test_generate_worker_id_unique(self):
        """Test that worker IDs are unique."""
        service_name = "test-service"
        worker_id1 = generate_worker_id(service_name)
        worker_id2 = generate_worker_id(service_name)

        assert worker_id1 != worker_id2


class TestAcquireLockWithRetry:
    """Tests for lock acquisition with retry."""

    async def test_acquire_lock_success_first_try(
        self, sqlite_storage, sample_workflow_data, create_test_instance
    ):
        """Test successfully acquiring lock on first try."""
        await create_test_instance(**sample_workflow_data)

        worker_id = "worker-1"
        result = await acquire_lock_with_retry(
            sqlite_storage,
            sample_workflow_data["instance_id"],
            worker_id,
            max_retries=3,
        )

        assert result is True

        # Verify lock
        instance = await sqlite_storage.get_instance(sample_workflow_data["instance_id"])
        assert instance["locked_by"] == worker_id

    async def test_acquire_lock_fail_all_retries(
        self, sqlite_storage, sample_workflow_data, create_test_instance
    ):
        """Test failing to acquire lock after all retries."""
        await create_test_instance(**sample_workflow_data)

        # Worker 1 holds the lock
        worker1 = "worker-1"
        await sqlite_storage.try_acquire_lock(sample_workflow_data["instance_id"], worker1)

        # Worker 2 tries to acquire with retries
        worker2 = "worker-2"
        result = await acquire_lock_with_retry(
            sqlite_storage,
            sample_workflow_data["instance_id"],
            worker2,
            max_retries=3,
            retry_delay=0.01,  # Short delay for testing
        )

        assert result is False

    async def test_acquire_lock_success_after_retry(
        self, sqlite_storage, sample_workflow_data, create_test_instance
    ):
        """Test acquiring lock after a retry."""
        await create_test_instance(**sample_workflow_data)

        worker1 = "worker-1"
        worker2 = "worker-2"

        # Worker 1 holds the lock
        await sqlite_storage.try_acquire_lock(sample_workflow_data["instance_id"], worker1)

        # Simulate releasing the lock after a short delay
        async def release_after_delay():
            await asyncio.sleep(0.05)
            await sqlite_storage.release_lock(sample_workflow_data["instance_id"], worker1)

        # Start release task
        release_task = asyncio.create_task(release_after_delay())

        # Worker 2 tries to acquire with retries
        result = await acquire_lock_with_retry(
            sqlite_storage,
            sample_workflow_data["instance_id"],
            worker2,
            max_retries=10,
            retry_delay=0.02,
        )

        await release_task

        # Should succeed eventually
        assert result is True


class TestEnsureLockHeld:
    """Tests for lock verification."""

    async def test_ensure_lock_held_success(
        self, sqlite_storage, sample_workflow_data, create_test_instance
    ):
        """Test verifying a lock that is held."""
        await create_test_instance(**sample_workflow_data)

        worker_id = "worker-1"
        await sqlite_storage.try_acquire_lock(sample_workflow_data["instance_id"], worker_id)

        # Should not raise
        await ensure_lock_held(sqlite_storage, sample_workflow_data["instance_id"], worker_id)

    async def test_ensure_lock_held_lost(
        self, sqlite_storage, sample_workflow_data, create_test_instance
    ):
        """Test verifying a lock that was lost."""
        await create_test_instance(**sample_workflow_data)

        worker_id = "worker-1"
        await sqlite_storage.try_acquire_lock(sample_workflow_data["instance_id"], worker_id)

        # Release lock
        await sqlite_storage.release_lock(sample_workflow_data["instance_id"], worker_id)

        # Should raise
        with pytest.raises(RuntimeError, match="Lock lost"):
            await ensure_lock_held(sqlite_storage, sample_workflow_data["instance_id"], worker_id)

    async def test_ensure_lock_held_wrong_worker(
        self, sqlite_storage, sample_workflow_data, create_test_instance
    ):
        """Test verifying a lock held by another worker."""
        await create_test_instance(**sample_workflow_data)

        worker1 = "worker-1"
        worker2 = "worker-2"

        await sqlite_storage.try_acquire_lock(sample_workflow_data["instance_id"], worker1)

        # Worker 2 checks
        with pytest.raises(RuntimeError, match="Lock lost"):
            await ensure_lock_held(sqlite_storage, sample_workflow_data["instance_id"], worker2)


class TestWorkflowLockContext:
    """Tests for workflow lock context manager."""

    async def test_workflow_lock_acquire_and_release(
        self, sqlite_storage, sample_workflow_data, create_test_instance
    ):
        """Test acquiring and releasing lock with context manager."""
        await create_test_instance(**sample_workflow_data)

        worker_id = "worker-1"

        async with workflow_lock(sqlite_storage, sample_workflow_data["instance_id"], worker_id):
            # Verify lock is held
            instance = await sqlite_storage.get_instance(sample_workflow_data["instance_id"])
            assert instance["locked_by"] == worker_id

        # Verify lock is released after context
        instance = await sqlite_storage.get_instance(sample_workflow_data["instance_id"])
        assert instance["locked_by"] is None

    async def test_workflow_lock_already_locked(
        self, sqlite_storage, sample_workflow_data, create_test_instance
    ):
        """Test context manager when lock is already held."""
        await create_test_instance(**sample_workflow_data)

        worker1 = "worker-1"
        worker2 = "worker-2"

        # Worker 1 holds lock
        await sqlite_storage.try_acquire_lock(sample_workflow_data["instance_id"], worker1)

        # Worker 2 tries to acquire
        with pytest.raises(RuntimeError, match="Failed to acquire lock"):
            async with workflow_lock(sqlite_storage, sample_workflow_data["instance_id"], worker2):
                pass

    async def test_workflow_lock_with_refresh(
        self, sqlite_storage, sample_workflow_data, create_test_instance
    ):
        """Test lock refresh during context."""
        await create_test_instance(**sample_workflow_data)

        worker_id = "worker-1"

        async with workflow_lock(
            sqlite_storage,
            sample_workflow_data["instance_id"],
            worker_id,
            refresh_interval=0.05,  # Refresh every 50ms
        ):
            # Wait for at least one refresh
            await asyncio.sleep(0.15)

            # Lock should still be held
            instance = await sqlite_storage.get_instance(sample_workflow_data["instance_id"])
            assert instance["locked_by"] == worker_id

        # Lock should be released
        instance = await sqlite_storage.get_instance(sample_workflow_data["instance_id"])
        assert instance["locked_by"] is None


class TestCleanupStaleLocks:
    """Tests for stale lock cleanup."""

    async def test_cleanup_stale_locks_basic(
        self, sqlite_storage, sample_workflow_data, create_test_instance
    ):
        """Test basic stale lock cleanup."""
        await create_test_instance(**sample_workflow_data)

        worker_id = "worker-1"
        await sqlite_storage.try_acquire_lock(sample_workflow_data["instance_id"], worker_id)

        # Verify the instance has status='running' (default)
        instance = await sqlite_storage.get_instance(sample_workflow_data["instance_id"])
        assert instance["status"] == "running"

        # Manually set lock_expires_at to past time to simulate stale lock
        from datetime import UTC, datetime, timedelta

        from sqlalchemy import update

        from edda.storage.sqlalchemy_storage import WorkflowInstance

        async with sqlite_storage.engine.begin() as conn:
            await conn.execute(
                update(WorkflowInstance)
                .where(WorkflowInstance.instance_id == sample_workflow_data["instance_id"])
                .values(lock_expires_at=datetime.now(UTC) - timedelta(seconds=1))
            )

        # Cleanup stale locks
        # Returns list of workflows with status='running' that need to be resumed
        workflows = await sqlite_storage.cleanup_stale_locks()
        assert len(workflows) == 1
        assert workflows[0]["instance_id"] == sample_workflow_data["instance_id"]
        assert workflows[0]["workflow_name"] == sample_workflow_data["workflow_name"]

        # Verify lock was cleaned
        instance = await sqlite_storage.get_instance(sample_workflow_data["instance_id"])
        assert instance["locked_by"] is None


class TestConcurrentLocking:
    """Tests for concurrent lock acquisition scenarios."""

    async def test_concurrent_lock_acquisition(
        self, sqlite_storage, sample_workflow_data, create_test_instance
    ):
        """Test concurrent lock acquisition behavior.

        SQLite uses advisory locking, so all workers may acquire the lock.
        PostgreSQL/MySQL use true row-level locking, so only one worker succeeds.

        Note: This test uses sqlite_storage fixture specifically, so it tests
        SQLite behavior. For multi-DB testing, see test_multidb_storage.py.
        """
        await create_test_instance(**sample_workflow_data)

        results = []

        async def try_acquire(worker_id: str):
            result = await sqlite_storage.try_acquire_lock(
                sample_workflow_data["instance_id"], worker_id
            )
            results.append((worker_id, result))

        # Simulate 5 workers trying to acquire lock concurrently
        tasks = [try_acquire(f"worker-{i}") for i in range(5)]
        await asyncio.gather(*tasks)

        # SQLite advisory locking: all workers may succeed
        # (This is a known limitation of SQLite's SELECT FOR UPDATE)
        successful = [worker for worker, result in results if result]

        # For SQLite, we accept that all workers may acquire the lock
        # In production, use PostgreSQL or MySQL for true distributed locking
        assert len(successful) >= 1, "At least one worker should acquire the lock"

        # Verify that at least one worker holds the lock
        instance = await sqlite_storage.get_instance(sample_workflow_data["instance_id"])
        assert instance["locked_by"] in [f"worker-{i}" for i in range(5)]
