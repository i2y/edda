"""
Tests for automatic workflow recovery after Stale Lock cleanup.

This module tests the automatic resumption of workflows that were interrupted
due to worker crashes, ensuring robustness in distributed deployments.
"""

import asyncio
import contextlib
from datetime import UTC, datetime, timedelta
from typing import Any
from unittest.mock import AsyncMock, MagicMock

from sqlalchemy import text, update
from sqlalchemy.ext.asyncio import AsyncSession

from edda import workflow
from edda.activity import WorkflowContext, activity
from edda.locking import auto_resume_stale_workflows_periodically, cleanup_stale_locks_periodically
from edda.replay import ReplayEngine
from edda.storage.sqlalchemy_storage import WorkflowInstance
from edda.workflow import set_replay_engine


async def set_lock_expired(storage, instance_id: str) -> None:
    """Helper to set lock_expires_at to past time to simulate stale lock."""
    async with storage.engine.begin() as conn:
        await conn.execute(
            update(WorkflowInstance)
            .where(WorkflowInstance.instance_id == instance_id)
            .values(lock_expires_at=datetime.now(UTC) - timedelta(seconds=1))
        )


# Test workflows and activities (prefixed with 'recovery_' to avoid pytest confusion)
@activity
async def recovery_activity(_ctx: WorkflowContext, value: int) -> int:
    """Test activity that returns doubled value."""
    return value * 2


@workflow
async def recovery_workflow(ctx: WorkflowContext, value: int) -> int:
    """Test workflow that calls recovery_activity."""
    result = await recovery_activity(ctx, value)
    return result


class TestStaleLocksCleanup:
    """Tests for cleanup_stale_locks() method."""

    async def test_cleanup_returns_running_workflows_only(
        self, sqlite_storage, create_test_instance
    ):
        """
        Test that cleanup_stale_locks() returns only status='running' workflows.

        This verifies the filtering logic that ensures we only auto-resume
        workflows that were actively running (not waiting for events).
        """
        # Create multiple workflow instances with different statuses
        await create_test_instance(
            instance_id="running-1",
            workflow_name="recovery_workflow",
            owner_service="test-service",
            input_data={"value": 1},
        )
        await create_test_instance(
            instance_id="waiting-1",
            workflow_name="recovery_workflow",
            owner_service="test-service",
            input_data={"value": 2},
        )
        await create_test_instance(
            instance_id="completed-1",
            workflow_name="recovery_workflow",
            owner_service="test-service",
            input_data={"value": 3},
        )

        # Acquire locks on all instances (simulating crash scenario)
        worker_id = "crashed-worker-123"

        # Acquire locks and set them as stale
        await sqlite_storage.try_acquire_lock("running-1", worker_id)
        await sqlite_storage.try_acquire_lock("waiting-1", worker_id)
        await sqlite_storage.try_acquire_lock("completed-1", worker_id)

        # Update statuses
        async with AsyncSession(sqlite_storage.engine, expire_on_commit=False) as conn:
            # Running workflow with Stale Lock
            await conn.execute(
                text(
                    "UPDATE workflow_instances SET status = 'running' WHERE instance_id = 'running-1'"
                )
            )
            # Waiting workflow with Stale Lock (should not be resumed)
            await conn.execute(
                text(
                    "UPDATE workflow_instances SET status = 'waiting_for_event' WHERE instance_id = 'waiting-1'"
                )
            )
            # Completed workflow with Stale Lock (should not be resumed)
            await conn.execute(
                text(
                    "UPDATE workflow_instances SET status = 'completed' WHERE instance_id = 'completed-1'"
                )
            )
            await conn.commit()

        # Make all locks stale
        await set_lock_expired(sqlite_storage, "running-1")
        await set_lock_expired(sqlite_storage, "waiting-1")
        await set_lock_expired(sqlite_storage, "completed-1")

        # Clean up Stale Locks
        workflows_to_resume = await sqlite_storage.cleanup_stale_locks()

        # Verify only status='running' workflow is returned
        assert len(workflows_to_resume) == 1
        assert workflows_to_resume[0]["instance_id"] == "running-1"
        assert workflows_to_resume[0]["workflow_name"] == "recovery_workflow"

        # Verify all locks were released (regardless of status)
        for instance_id in ["running-1", "waiting-1", "completed-1"]:
            instance = await sqlite_storage.get_instance(instance_id)
            assert instance is not None
            assert instance["locked_by"] is None
            assert instance["locked_at"] is None

    async def test_cleanup_does_not_affect_fresh_locks(self, sqlite_storage, create_test_instance):
        """
        Test that cleanup_stale_locks() does not clean up recent locks.

        This ensures that actively executing workflows are not interrupted.
        """
        # Create workflow instance
        await create_test_instance(
            instance_id="active-1",
            workflow_name="recovery_workflow",
            owner_service="test-service",
            input_data={"value": 1},
        )

        # Acquire lock with recent timestamp using SQLite datetime functions
        worker_id = "active-worker-456"

        async with AsyncSession(sqlite_storage.engine, expire_on_commit=False) as conn:
            await conn.execute(
                text(
                    """
                UPDATE workflow_instances
                SET status = 'running', locked_by = :worker_id, locked_at = datetime('now', '-30 seconds')
                WHERE instance_id = 'active-1'
                """
                ),
                {"worker_id": worker_id},
            )
            await conn.commit()

        # Clean up Stale Locks (timeout = 300 seconds)
        workflows_to_resume = await sqlite_storage.cleanup_stale_locks()

        # Verify no workflows were cleaned up
        assert len(workflows_to_resume) == 0

        # Verify lock is still held
        instance = await sqlite_storage.get_instance("active-1")
        assert instance is not None
        assert instance["locked_by"] == worker_id


class TestStaleWorkflowRecovery:
    """Tests for automatic workflow recovery."""

    async def test_periodic_cleanup_without_replay_engine(
        self, sqlite_storage, create_test_instance
    ):
        """
        Test that cleanup without replay_engine only cleans locks.

        This tests backward compatibility - if replay_engine is None,
        the behavior should be the same as before (just cleanup, no resume).
        """
        # Create workflow instance with Stale Lock
        await create_test_instance(
            instance_id="stale-1",
            workflow_name="recovery_workflow",
            owner_service="test-service",
            input_data={"value": 1},
        )

        worker_id = "crashed-worker-789"

        # Acquire lock and make it stale
        await sqlite_storage.try_acquire_lock("stale-1", worker_id)

        # Update status to running
        async with AsyncSession(sqlite_storage.engine, expire_on_commit=False) as conn:
            await conn.execute(
                text(
                    "UPDATE workflow_instances SET status = 'running' WHERE instance_id = 'stale-1'"
                )
            )
            await conn.commit()

        # Make lock stale
        await set_lock_expired(sqlite_storage, "stale-1")

        # Create a task that runs cleanup once and then exits
        cleanup_task = asyncio.create_task(
            cleanup_stale_locks_periodically(
                storage=sqlite_storage,
                interval=0.1,  # Fast interval for testing
            )
        )

        # Wait for cleanup to run
        await asyncio.sleep(0.2)

        # Cancel the task
        cleanup_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await cleanup_task

        # Verify lock was cleaned
        instance = await sqlite_storage.get_instance("stale-1")
        assert instance is not None
        assert instance["locked_by"] is None

        # Verify status is still 'running' (not resumed)
        assert instance["status"] == "running"

    async def test_periodic_cleanup_with_replay_engine(self, sqlite_storage, create_test_instance):
        """
        Test that auto_resume_stale_workflows_periodically auto-resumes workflows.

        This is the main test for the auto-resume feature.
        """
        # Create workflow instance with Stale Lock
        await create_test_instance(
            instance_id="stale-2",
            workflow_name="recovery_workflow",
            owner_service="test-service",
            input_data={"value": 5},
        )

        worker_id = "crashed-worker-101"

        # Acquire lock and make it stale
        await sqlite_storage.try_acquire_lock("stale-2", worker_id)

        # Update status to running
        async with AsyncSession(sqlite_storage.engine, expire_on_commit=False) as conn:
            await conn.execute(
                text(
                    "UPDATE workflow_instances SET status = 'running' WHERE instance_id = 'stale-2'"
                )
            )
            await conn.commit()

        # Make lock stale
        await set_lock_expired(sqlite_storage, "stale-2")

        # Create mock replay engine
        mock_replay_engine = MagicMock()
        mock_replay_engine.resume_by_name = AsyncMock()

        # Create a task that runs cleanup once and then exits
        cleanup_task = asyncio.create_task(
            auto_resume_stale_workflows_periodically(
                storage=sqlite_storage,
                replay_engine=mock_replay_engine,
                interval=0.1,
            )
        )

        # Wait for cleanup to run
        await asyncio.sleep(0.2)

        # Cancel the task
        cleanup_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await cleanup_task

        # Verify lock was cleaned
        instance = await sqlite_storage.get_instance("stale-2")
        assert instance is not None
        assert instance["locked_by"] is None

        # Verify replay_engine.resume_by_name() was called
        mock_replay_engine.resume_by_name.assert_called_once_with("stale-2", "recovery_workflow")

    async def test_auto_resume_continues_on_error(self, sqlite_storage, create_test_instance):
        """
        Test that failed resume of one workflow doesn't block others.

        This ensures robustness - if one workflow fails to resume,
        the cleanup process should continue with other workflows.
        """
        # Create multiple workflow instances with Stale Locks
        for i in range(3):
            await create_test_instance(
                instance_id=f"stale-{i}",
                workflow_name="recovery_workflow",
                owner_service="test-service",
                input_data={"value": i},
            )

        worker_id = "crashed-worker-202"

        # Acquire locks for all instances
        for i in range(3):
            await sqlite_storage.try_acquire_lock(f"stale-{i}", worker_id)

        # Update status to running
        async with AsyncSession(sqlite_storage.engine, expire_on_commit=False) as conn:
            for i in range(3):
                await conn.execute(
                    text(
                        "UPDATE workflow_instances SET status = 'running' WHERE instance_id = :instance_id"
                    ),
                    {"instance_id": f"stale-{i}"},
                )
            await conn.commit()

        # Make all locks stale
        for i in range(3):
            await set_lock_expired(sqlite_storage, f"stale-{i}")

        # Create mock replay engine that fails on the second workflow
        mock_replay_engine = MagicMock()
        resume_calls: list[tuple[str, str]] = []

        async def mock_resume(instance_id: str, workflow_name: str) -> Any:
            resume_calls.append((instance_id, workflow_name))
            if instance_id == "stale-1":
                raise Exception("Simulated resume failure")

        mock_replay_engine.resume_by_name = AsyncMock(side_effect=mock_resume)

        # Create a task that runs cleanup once and then exits
        cleanup_task = asyncio.create_task(
            auto_resume_stale_workflows_periodically(
                storage=sqlite_storage,
                replay_engine=mock_replay_engine,
                interval=0.1,
            )
        )

        # Wait for cleanup to run
        await asyncio.sleep(0.2)

        # Cancel the task
        cleanup_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await cleanup_task

        # Verify all locks were cleaned
        for i in range(3):
            instance = await sqlite_storage.get_instance(f"stale-{i}")
            assert instance is not None
            assert instance["locked_by"] is None

        # Verify all resume calls were attempted
        assert len(resume_calls) == 3
        assert ("stale-0", "recovery_workflow") in resume_calls
        assert ("stale-1", "recovery_workflow") in resume_calls
        assert ("stale-2", "recovery_workflow") in resume_calls

    async def test_integration_with_real_replay_engine(self, sqlite_storage, create_test_instance):
        """
        Integration test with real ReplayEngine.

        This test verifies the end-to-end workflow recovery with actual
        workflow execution and replay.
        """
        # Create a real replay engine
        replay_engine = ReplayEngine(
            storage=sqlite_storage,
            service_name="test-service",
            worker_id="test-worker",
        )

        # Set the replay engine globally so workflow.start() works
        set_replay_engine(replay_engine)

        # Start a workflow
        instance_id = await recovery_workflow.start(value=10)

        # Wait a moment for execution to complete
        await asyncio.sleep(0.2)

        # Verify workflow completed successfully  first
        instance = await sqlite_storage.get_instance(instance_id)
        assert instance is not None
        assert instance["status"] == "completed"
        assert instance["output_data"]["result"] == 20  # 10 * 2

        # Now simulate crash by manually resetting to running state with Stale Lock
        worker_id = "crashed-worker"

        # Acquire lock
        await sqlite_storage.try_acquire_lock(instance_id, worker_id)

        # Update status back to running
        async with AsyncSession(sqlite_storage.engine, expire_on_commit=False) as conn:
            await conn.execute(
                text(
                    "UPDATE workflow_instances SET status = 'running' WHERE instance_id = :instance_id"
                ),
                {"instance_id": instance_id},
            )
            await conn.commit()

        # Make lock stale
        await set_lock_expired(sqlite_storage, instance_id)

        # Verify workflow is in crashed state
        instance = await sqlite_storage.get_instance(instance_id)
        assert instance is not None
        assert instance["locked_by"] == "crashed-worker"
        assert instance["status"] == "running"

        # Run cleanup with auto-resume
        workflows_to_resume = await sqlite_storage.cleanup_stale_locks()
        assert len(workflows_to_resume) == 1

        # Manually trigger resume (simulating what the periodic task does)
        await replay_engine.resume_by_name(instance_id, "recovery_workflow")

        # Wait for workflow to complete
        await asyncio.sleep(0.2)

        # Verify workflow completed successfully after recovery
        instance = await sqlite_storage.get_instance(instance_id)
        assert instance is not None
        assert instance["status"] == "completed"
        assert instance["output_data"]["result"] == 20  # 10 * 2
