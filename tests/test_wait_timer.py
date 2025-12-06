"""
Tests for timer functionality.

Tests cover:
- wait_timer functionality
- Timer subscription registration
- Timer-based workflow resumption
- Atomic timer subscription and lock release
- Background timer check
"""

import asyncio
from datetime import UTC, datetime, timedelta

import pytest
import pytest_asyncio
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from edda import workflow
from edda.channels import WaitForTimerException, wait_timer
from edda.context import WorkflowContext
from edda.replay import ReplayEngine
from edda.workflow import set_replay_engine


@pytest.mark.asyncio
class TestWaitTimer:
    """Test suite for wait_timer functionality."""

    @pytest_asyncio.fixture
    async def workflow_instance(self, sqlite_storage, create_test_instance):
        """Create a workflow instance for testing."""
        instance_id = "test-timer-instance-001"
        await create_test_instance(
            instance_id=instance_id,
            workflow_name="test_workflow",
            owner_service="test-service",
            input_data={"test": "data"},
        )
        await sqlite_storage.update_instance_status(instance_id, "running")
        return instance_id

    async def test_wait_timer_raises_exception_during_normal_execution(
        self, sqlite_storage, workflow_instance
    ):
        """Test that wait_timer raises WaitForTimerException during normal execution."""
        ctx = WorkflowContext(
            instance_id=workflow_instance,
            workflow_name="test_workflow",
            storage=sqlite_storage,
            worker_id="worker-1",
            is_replaying=False,
        )

        # Should raise exception to pause workflow
        with pytest.raises(WaitForTimerException) as exc_info:
            await wait_timer(
                ctx,
                seconds=60,
            )

        # Verify exception details
        assert exc_info.value.duration_seconds == 60
        assert exc_info.value.timer_id.startswith("sleep:")  # Auto-generated timer_id
        assert exc_info.value.activity_id.startswith("sleep:")

    async def test_wait_timer_with_custom_timer_id(self, sqlite_storage, workflow_instance):
        """Test that wait_timer accepts custom timer_id."""
        ctx = WorkflowContext(
            instance_id=workflow_instance,
            workflow_name="test_workflow",
            storage=sqlite_storage,
            worker_id="worker-1",
            is_replaying=False,
        )

        # Call wait_timer with custom timer_id
        with pytest.raises(WaitForTimerException) as exc_info:
            await wait_timer(
                ctx,
                seconds=120,
                timer_id="custom_timer",
            )

        # Verify exception contains custom timer_id
        assert exc_info.value.timer_id == "custom_timer"
        assert exc_info.value.duration_seconds == 120

    async def test_wait_timer_generates_activity_id(self, sqlite_storage, workflow_instance):
        """Test that wait_timer generates activity_id and tracks it."""
        ctx = WorkflowContext(
            instance_id=workflow_instance,
            workflow_name="test_workflow",
            storage=sqlite_storage,
            worker_id="worker-1",
            is_replaying=False,
        )

        assert len(ctx.executed_activity_ids) == 0

        # First wait_timer call
        with pytest.raises(WaitForTimerException):
            await wait_timer(ctx, seconds=30)

        assert len(ctx.executed_activity_ids) == 1
        assert "sleep:1" in ctx.executed_activity_ids

        # Second wait_timer call
        with pytest.raises(WaitForTimerException):
            await wait_timer(ctx, seconds=30)

        assert len(ctx.executed_activity_ids) == 2
        assert "sleep:2" in ctx.executed_activity_ids

    async def test_wait_timer_returns_immediately_during_replay(
        self, sqlite_storage, workflow_instance
    ):
        """Test that wait_timer returns immediately during replay."""
        # Add timer expiration to history
        await sqlite_storage.append_history(
            workflow_instance,
            activity_id="test_timer",
            event_type="TimerExpired",
            event_data={"result": None},
        )

        ctx = WorkflowContext(
            instance_id=workflow_instance,
            workflow_name="test_workflow",
            storage=sqlite_storage,
            worker_id="worker-1",
            is_replaying=True,
        )

        # Load history into context (properly)
        await ctx._load_history()

        # Should return immediately without raising exception
        result = await wait_timer(ctx, seconds=60, timer_id="test_timer")
        assert result is None  # wait_timer returns None
        assert "test_timer" in ctx.executed_activity_ids


@pytest.mark.asyncio
class TestTimerSubscription:
    """Test suite for timer subscription registration."""

    @pytest_asyncio.fixture
    async def workflow_instance(self, sqlite_storage, create_test_instance):
        """Create a workflow instance for testing."""
        instance_id = "test-timer-sub-001"
        await create_test_instance(
            instance_id=instance_id,
            workflow_name="test_workflow",
            owner_service="test-service",
            input_data={"test": "data"},
        )
        await sqlite_storage.update_instance_status(instance_id, "running")
        return instance_id

    async def test_register_timer_subscription_and_release_lock(
        self, sqlite_storage, workflow_instance
    ):
        """Test atomic timer subscription registration and lock release."""
        worker_id = "worker-1"

        # Acquire lock first
        lock_acquired = await sqlite_storage.try_acquire_lock(
            workflow_instance, worker_id, timeout_seconds=30
        )
        assert lock_acquired

        # Register timer subscription and release lock atomically
        expires_at = datetime.now(UTC) + timedelta(seconds=60)
        await sqlite_storage.register_timer_subscription_and_release_lock(
            instance_id=workflow_instance,
            worker_id=worker_id,
            timer_id="test_timer",
            expires_at=expires_at,
            activity_id="test_timer",
        )

        # Verify lock was released
        instance = await sqlite_storage.get_instance(workflow_instance)
        assert instance["locked_by"] is None
        assert instance["locked_at"] is None

        # Verify timer subscription was registered
        expired_timers = await sqlite_storage.find_expired_timers()
        # Timer should not be expired yet
        assert len(expired_timers) == 0

    async def test_find_expired_timers(self, sqlite_storage, workflow_instance):
        """Test finding expired timers."""
        worker_id = "worker-1"

        # Acquire lock
        await sqlite_storage.try_acquire_lock(workflow_instance, worker_id, timeout_seconds=30)

        # Register timer subscription with past expiration time
        expires_at = datetime.now(UTC) - timedelta(seconds=10)  # Expired 10 seconds ago
        await sqlite_storage.register_timer_subscription_and_release_lock(
            instance_id=workflow_instance,
            worker_id=worker_id,
            timer_id="expired_timer",
            expires_at=expires_at,
            activity_id="expired_timer",
        )

        # Update status to waiting_for_timer
        await sqlite_storage.update_instance_status(workflow_instance, "waiting_for_timer")

        # Find expired timers
        expired_timers = await sqlite_storage.find_expired_timers()
        assert len(expired_timers) == 1
        assert expired_timers[0]["instance_id"] == workflow_instance
        assert expired_timers[0]["timer_id"] == "expired_timer"
        assert expired_timers[0]["workflow_name"] == "test_workflow"

    async def test_remove_timer_subscription(self, sqlite_storage, workflow_instance):
        """Test removing timer subscription."""
        worker_id = "worker-1"

        # Acquire lock and register timer
        await sqlite_storage.try_acquire_lock(workflow_instance, worker_id, timeout_seconds=30)
        expires_at = datetime.now(UTC) + timedelta(seconds=60)
        await sqlite_storage.register_timer_subscription_and_release_lock(
            instance_id=workflow_instance,
            worker_id=worker_id,
            timer_id="test_timer",
            expires_at=expires_at,
            activity_id="test_timer",
        )

        # Remove timer subscription
        await sqlite_storage.remove_timer_subscription(workflow_instance, "test_timer")

        # Verify timer subscription was removed
        # (find_expired_timers joins with workflow_instances, so we can't use it directly)
        # Instead, we'll try to register the same timer again (should work if removed)
        await sqlite_storage.try_acquire_lock(workflow_instance, worker_id, timeout_seconds=30)
        await sqlite_storage.register_timer_subscription_and_release_lock(
            instance_id=workflow_instance,
            worker_id=worker_id,
            timer_id="test_timer",
            expires_at=expires_at,
            activity_id="test_timer",
        )


@pytest.mark.asyncio
class TestTimerWorkflow:
    """Test suite for timer-based workflow execution."""

    async def test_timer_workflow_end_to_end(self, sqlite_storage):
        """Test complete timer workflow execution."""

        # Define a workflow that waits for a timer
        @workflow
        async def timer_workflow(ctx: WorkflowContext, wait_seconds: int):
            """Workflow that waits for a timer."""
            # Wait for timer
            await wait_timer(ctx, seconds=wait_seconds, timer_id="test_timer")

            # Continue execution after timer expires
            return {"status": "completed"}

        # Create replay engine
        replay_engine = ReplayEngine(
            storage=sqlite_storage,
            service_name="test-service",
            worker_id="worker-1",
        )
        set_replay_engine(replay_engine)

        # Start workflow
        instance_id = await timer_workflow.start(wait_seconds=1)  # 1 second timer

        # Verify workflow is waiting for timer
        instance = await sqlite_storage.get_instance(instance_id)
        assert instance["status"] == "waiting_for_timer"

        # Wait for timer to expire (plus some buffer)
        await asyncio.sleep(2)

        # Simulate timer check (would be done by background task in production)
        expired_timers = await sqlite_storage.find_expired_timers()
        assert len(expired_timers) == 1
        assert expired_timers[0]["instance_id"] == instance_id

        # Resume workflow after timer expiration
        timer = expired_timers[0]
        await sqlite_storage.append_history(
            instance_id,
            activity_id=timer["activity_id"],
            event_type="TimerExpired",
            event_data={"result": None},
        )
        await sqlite_storage.remove_timer_subscription(instance_id, timer["timer_id"])

        # Resume workflow
        await replay_engine.resume_workflow(instance_id, timer_workflow)

        # Verify workflow completed
        instance = await sqlite_storage.get_instance(instance_id)
        assert instance["status"] == "completed"

    async def test_timer_workflow_with_cancellation(self, sqlite_storage):
        """Test timer workflow with cancellation before timer expires."""

        # Define a workflow that waits for a timer
        @workflow
        async def cancellable_timer_workflow(ctx: WorkflowContext):
            """Workflow that waits for a long timer."""
            await wait_timer(ctx, seconds=300, timer_id="long_timer")
            return {"status": "completed"}

        # Create replay engine
        replay_engine = ReplayEngine(
            storage=sqlite_storage,
            service_name="test-service",
            worker_id="worker-1",
        )
        set_replay_engine(replay_engine)

        # Start workflow
        instance_id = await cancellable_timer_workflow.start()

        # Verify workflow is waiting for timer
        instance = await sqlite_storage.get_instance(instance_id)
        assert instance["status"] == "waiting_for_timer"

        # Verify timer subscription exists before cancellation
        await sqlite_storage.find_expired_timers()
        # Timer should exist but not expired yet
        # (find_expired_timers returns empty since timer hasn't expired)

        # Cancel workflow
        await replay_engine.cancel_workflow(instance_id)

        # Verify workflow is cancelled
        instance = await sqlite_storage.get_instance(instance_id)
        assert instance["status"] == "cancelled"

        # Verify timer subscription was removed during cancellation
        # Check by trying to find any timers for this instance
        from edda.storage.sqlalchemy_storage import WorkflowTimerSubscription

        async with AsyncSession(sqlite_storage.engine) as conn:
            result = await conn.execute(
                select(WorkflowTimerSubscription).where(
                    WorkflowTimerSubscription.instance_id == instance_id
                )
            )
            subscriptions = result.scalars().all()
            assert len(subscriptions) == 0  # Should be removed
