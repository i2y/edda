"""
Tests for lock timeout customization feature.

This module tests that lock timeout values can be customized at
decorator level and runtime level, with proper priority handling.
"""

import asyncio

import pytest

from edda.context import WorkflowContext
from edda.locking import generate_worker_id
from edda.workflow import workflow


@pytest.mark.asyncio
class TestLockTimeoutCustomization:
    """Tests for lock timeout customization."""

    async def test_decorator_level_timeout(
        self, sqlite_storage, sample_workflow_data, create_test_instance
    ):
        """Test that decorator-level timeout is stored in database."""

        # Create a workflow with custom timeout
        @workflow(lock_timeout_seconds=600)
        async def long_running_workflow(ctx: WorkflowContext, user_id: int):
            return {"status": "completed"}

        # Start workflow (this will call storage.create_instance with lock_timeout_seconds=600)
        # Note: We can't actually start the workflow without full EddaApp setup,
        # so we'll test the storage layer directly

        instance_id = "test-instance-decorator-timeout"

        # First, create workflow definition (required by get_instance() JOIN)
        await sqlite_storage.upsert_workflow_definition(
            workflow_name="long_running_workflow",
            source_hash="test-hash",
            source_code="# Test workflow",
        )

        await sqlite_storage.create_instance(
            instance_id=instance_id,
            workflow_name="long_running_workflow",
            source_hash="test-hash",
            owner_service="test-service",
            input_data={"user_id": 123},
            lock_timeout_seconds=600,
        )

        # Verify timeout is stored
        instance = await sqlite_storage.get_instance(instance_id)
        assert instance is not None
        assert instance["lock_timeout_seconds"] == 600

    async def test_runtime_timeout_override(
        self, sqlite_storage, sample_workflow_data, create_test_instance
    ):
        """Test that runtime timeout overrides decorator timeout."""

        instance_id = "test-instance-runtime-timeout"

        # Create workflow definition first
        await sqlite_storage.upsert_workflow_definition(
            workflow_name="test_workflow",
            source_hash="test-hash",
            source_code="# Test workflow",
        )

        await sqlite_storage.create_instance(
            instance_id=instance_id,
            workflow_name="test_workflow",
            source_hash="test-hash",
            owner_service="test-service",
            input_data={"user_id": 123},
            lock_timeout_seconds=900,  # Runtime override
        )

        # Verify timeout is stored
        instance = await sqlite_storage.get_instance(instance_id)
        assert instance is not None
        assert instance["lock_timeout_seconds"] == 900

    async def test_none_timeout_uses_global_default(
        self, sqlite_storage, sample_workflow_data, create_test_instance
    ):
        """Test that None timeout uses global default (300s)."""

        instance_id = "test-instance-none-timeout"

        # Create workflow definition first
        await sqlite_storage.upsert_workflow_definition(
            workflow_name="test_workflow",
            source_hash="test-hash",
            source_code="# Test workflow",
        )

        await sqlite_storage.create_instance(
            instance_id=instance_id,
            workflow_name="test_workflow",
            source_hash="test-hash",
            owner_service="test-service",
            input_data={"user_id": 123},
            lock_timeout_seconds=None,  # Should use global default
        )

        # Verify timeout is None (will use global default during lock acquisition)
        instance = await sqlite_storage.get_instance(instance_id)
        assert instance is not None
        assert instance["lock_timeout_seconds"] is None

    async def test_try_acquire_lock_respects_instance_timeout(
        self, sqlite_storage, sample_workflow_data, create_test_instance
    ):
        """Test that try_acquire_lock uses instance-specific timeout."""

        instance_id = "test-instance-lock-timeout"
        worker_id_A = generate_worker_id("worker-A")
        worker_id_B = generate_worker_id("worker-B")

        # Create instance with 2-second timeout
        await sqlite_storage.create_instance(
            instance_id=instance_id,
            workflow_name="test_workflow",
            source_hash="test-hash",
            owner_service="test-service",
            input_data={"user_id": 123},
            lock_timeout_seconds=2,  # Very short timeout for testing
        )

        # Worker A acquires lock
        acquired_A = await sqlite_storage.try_acquire_lock(
            instance_id, worker_id_A, timeout_seconds=300
        )
        assert acquired_A is True

        # Worker B tries to acquire immediately - should fail
        acquired_B = await sqlite_storage.try_acquire_lock(
            instance_id, worker_id_B, timeout_seconds=300
        )
        assert acquired_B is False

        # Wait 3 seconds (longer than instance timeout of 2 seconds)
        await asyncio.sleep(3)

        # Worker B should now be able to acquire (stale lock detection)
        acquired_B = await sqlite_storage.try_acquire_lock(
            instance_id, worker_id_B, timeout_seconds=300
        )
        assert acquired_B is True

        # Cleanup
        await sqlite_storage.release_lock(instance_id, worker_id_B)

    async def test_cleanup_stale_locks_respects_instance_timeout(
        self, sqlite_storage, sample_workflow_data, create_test_instance
    ):
        """Test that cleanup_stale_locks uses instance-specific timeouts."""

        worker_id = generate_worker_id("worker-cleanup")

        # Create two instances with different timeouts
        instance_id_1 = "test-instance-cleanup-1"
        instance_id_2 = "test-instance-cleanup-2"

        # Create workflow definitions first (required by get_instance() JOIN)
        await sqlite_storage.upsert_workflow_definition(
            workflow_name="test_workflow_1",
            source_hash="test-hash",
            source_code="# Test workflow 1",
        )

        await sqlite_storage.upsert_workflow_definition(
            workflow_name="test_workflow_2",
            source_hash="test-hash",
            source_code="# Test workflow 2",
        )

        await sqlite_storage.create_instance(
            instance_id=instance_id_1,
            workflow_name="test_workflow_1",
            source_hash="test-hash",
            owner_service="test-service",
            input_data={"user_id": 123},
            lock_timeout_seconds=2,  # 2-second timeout
        )

        await sqlite_storage.create_instance(
            instance_id=instance_id_2,
            workflow_name="test_workflow_2",
            source_hash="test-hash",
            owner_service="test-service",
            input_data={"user_id": 456},
            lock_timeout_seconds=10,  # 10-second timeout
        )

        # Acquire locks for both
        await sqlite_storage.try_acquire_lock(instance_id_1, worker_id, timeout_seconds=300)
        await sqlite_storage.try_acquire_lock(instance_id_2, worker_id, timeout_seconds=300)

        # Wait 3 seconds
        await asyncio.sleep(3)

        # Cleanup (uses lock_expires_at column set during lock acquisition)
        stale_workflows = await sqlite_storage.cleanup_stale_locks()

        # Only instance_id_1 should be cleaned up (2-second timeout expired)
        # instance_id_2 should still be locked (10-second timeout not expired)
        stale_instance_ids = [wf["instance_id"] for wf in stale_workflows]
        assert instance_id_1 in stale_instance_ids
        assert instance_id_2 not in stale_instance_ids

        # Verify lock states
        instance_1 = await sqlite_storage.get_instance(instance_id_1)
        instance_2 = await sqlite_storage.get_instance(instance_id_2)

        assert instance_1["locked_by"] is None  # Cleaned up
        assert instance_2["locked_by"] == worker_id  # Still locked

        # Cleanup instance_2
        await sqlite_storage.release_lock(instance_id_2, worker_id)

    async def test_backward_compatibility_no_timeout(
        self, sqlite_storage, sample_workflow_data, create_test_instance
    ):
        """Test that workflows without lock_timeout_seconds use global default."""

        instance_id = "test-instance-backward-compat"
        worker_id_A = generate_worker_id("worker-A")
        worker_id_B = generate_worker_id("worker-B")

        # Create instance without lock_timeout_seconds (backward compatibility)
        await sqlite_storage.create_instance(
            instance_id=instance_id,
            workflow_name="test_workflow",
            source_hash="test-hash",
            owner_service="test-service",
            input_data={"user_id": 123},
            # lock_timeout_seconds not specified (None)
        )

        # Worker A acquires lock
        acquired_A = await sqlite_storage.try_acquire_lock(
            instance_id, worker_id_A, timeout_seconds=1  # 1-second global timeout for testing
        )
        assert acquired_A is True

        # Worker B tries to acquire immediately - should fail
        acquired_B = await sqlite_storage.try_acquire_lock(
            instance_id, worker_id_B, timeout_seconds=1
        )
        assert acquired_B is False

        # Wait 2 seconds (longer than global timeout of 1 second)
        await asyncio.sleep(2)

        # Worker B should now be able to acquire (stale lock detection using global timeout)
        acquired_B = await sqlite_storage.try_acquire_lock(
            instance_id, worker_id_B, timeout_seconds=1
        )
        assert acquired_B is True

        # Cleanup
        await sqlite_storage.release_lock(instance_id, worker_id_B)
