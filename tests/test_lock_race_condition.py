"""
Test for lock acquisition race condition in PostgreSQL.

This test investigates why PostgreSQL allows two workers to acquire
the same lock concurrently when SQLite does not.
"""

import asyncio


class TestLockRaceCondition:
    """Tests to investigate PostgreSQL lock race condition."""

    async def test_sequential_lock_acquisition_postgres(
        self, postgresql_storage, sample_workflow_data
    ):
        """Test sequential lock acquisition (should work)."""
        await postgresql_storage.create_instance(**sample_workflow_data)

        # Worker 1 acquires lock
        worker1 = "worker-1"
        result1 = await postgresql_storage.try_acquire_lock(
            sample_workflow_data["instance_id"], worker1
        )
        assert result1 is True, "Worker 1 should acquire lock"

        # Check lock is held
        instance = await postgresql_storage.get_instance(sample_workflow_data["instance_id"])
        assert instance["locked_by"] == worker1

        # Worker 2 tries to acquire lock (should fail)
        worker2 = "worker-2"
        result2 = await postgresql_storage.try_acquire_lock(
            sample_workflow_data["instance_id"], worker2
        )
        assert result2 is False, "Worker 2 should NOT acquire lock"

        # Verify lock still held by worker 1
        instance = await postgresql_storage.get_instance(sample_workflow_data["instance_id"])
        assert instance["locked_by"] == worker1

    async def test_lock_with_delay_postgres(self, postgresql_storage, sample_workflow_data):
        """Test lock acquisition with small delay between attempts."""
        await postgresql_storage.create_instance(**sample_workflow_data)

        # Worker 1 acquires lock
        worker1 = "worker-1"
        result1 = await postgresql_storage.try_acquire_lock(
            sample_workflow_data["instance_id"], worker1
        )
        assert result1 is True

        # Add small delay (simulating real-world timing)
        await asyncio.sleep(0.01)  # 10ms

        # Worker 2 tries to acquire lock
        worker2 = "worker-2"
        result2 = await postgresql_storage.try_acquire_lock(
            sample_workflow_data["instance_id"], worker2
        )
        assert result2 is False, "Worker 2 should NOT acquire lock after delay"

    async def test_concurrent_lock_acquisition_postgres(
        self, postgresql_storage, sample_workflow_data
    ):
        """Test truly concurrent lock acquisition attempts."""
        await postgresql_storage.create_instance(**sample_workflow_data)

        worker1 = "worker-1"
        worker2 = "worker-2"

        # Launch both lock attempts concurrently
        results = await asyncio.gather(
            postgresql_storage.try_acquire_lock(sample_workflow_data["instance_id"], worker1),
            postgresql_storage.try_acquire_lock(sample_workflow_data["instance_id"], worker2),
        )

        # Exactly one should succeed
        assert sum(results) == 1, (
            f"Exactly one worker should acquire lock, "
            f"but got: worker1={results[0]}, worker2={results[1]}"
        )

        # Check final state
        instance = await postgresql_storage.get_instance(sample_workflow_data["instance_id"])
        assert instance["locked_by"] in [
            worker1,
            worker2,
        ], f"Lock should be held by one worker, got: {instance['locked_by']}"

    async def test_check_postgres_state_between_locks(
        self, postgresql_storage, sample_workflow_data
    ):
        """Test to inspect database state between lock attempts."""
        await postgresql_storage.create_instance(**sample_workflow_data)

        # Worker 1 acquires lock
        worker1 = "worker-1"
        result1 = await postgresql_storage.try_acquire_lock(
            sample_workflow_data["instance_id"], worker1
        )
        print("\n=== After Worker 1 lock attempt ===")
        print(f"Worker 1 result: {result1}")

        instance = await postgresql_storage.get_instance(sample_workflow_data["instance_id"])
        print(f"Instance locked_by: {instance['locked_by']}")
        print(f"Instance locked_at: {instance['locked_at']}")

        # Worker 2 tries to acquire lock
        worker2 = "worker-2"
        result2 = await postgresql_storage.try_acquire_lock(
            sample_workflow_data["instance_id"], worker2
        )
        print("\n=== After Worker 2 lock attempt ===")
        print(f"Worker 2 result: {result2}")

        instance = await postgresql_storage.get_instance(sample_workflow_data["instance_id"])
        print(f"Instance locked_by: {instance['locked_by']}")
        print(f"Instance locked_at: {instance['locked_at']}")

        assert result1 is True
        assert result2 is False, (
            f"Worker 2 should NOT acquire lock! " f"Final lock holder: {instance['locked_by']}"
        )

    async def test_sqlite_comparison(self, sqlite_storage, sample_workflow_data):
        """Same test with SQLite for comparison."""
        await sqlite_storage.create_instance(**sample_workflow_data)

        # Worker 1 acquires lock
        worker1 = "worker-1"
        result1 = await sqlite_storage.try_acquire_lock(
            sample_workflow_data["instance_id"], worker1
        )
        assert result1 is True

        # Worker 2 tries to acquire lock
        worker2 = "worker-2"
        result2 = await sqlite_storage.try_acquire_lock(
            sample_workflow_data["instance_id"], worker2
        )
        assert result2 is False

        # Verify lock still held by worker 1
        instance = await sqlite_storage.get_instance(sample_workflow_data["instance_id"])
        assert instance["locked_by"] == worker1
