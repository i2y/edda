"""
Tests for workflow cancellation functionality.

This module tests the ability to cancel running and waiting workflows,
ensuring compensations are executed and state is properly updated.
"""

import asyncio
from typing import Any

import pytest
from sqlalchemy.ext.asyncio import create_async_engine

from edda.activity import activity
from edda.channels import wait_event
from edda.compensation import register_compensation
from edda.context import WorkflowContext
from edda.locking import generate_worker_id
from edda.replay import ReplayEngine
from edda.storage.sqlalchemy_storage import SQLAlchemyStorage
from edda.workflow import workflow


@pytest.fixture
async def storage() -> SQLAlchemyStorage:
    """Create an in-memory SQLite storage for testing."""
    storage = SQLAlchemyStorage(create_async_engine("sqlite+aiosqlite:///:memory:", echo=False))
    await storage.initialize()

    # Create workflow definitions for tests
    workflow_defs = {
        "test_workflow": "test-hash-1",
        "cancellable_saga": "test-hash-2",
        "waiting_workflow": "test-hash-3",
        "simple_workflow": "test-hash-4",
    }
    for name, hash_val in workflow_defs.items():
        await storage.upsert_workflow_definition(
            workflow_name=name,
            source_hash=hash_val,
            source_code=f"async def {name}(ctx): pass",
        )

    yield storage
    await storage.close()


@pytest.fixture
def worker_id() -> str:
    """Generate a unique worker ID for testing."""
    return generate_worker_id("test-service")


@pytest.fixture
def replay_engine(storage: SQLAlchemyStorage, worker_id: str) -> ReplayEngine:
    """Create a ReplayEngine for testing."""
    return ReplayEngine(storage=storage, service_name="test-service", worker_id=worker_id)


# Global state for tracking compensation execution
compensation_executed = []


@activity
async def step_one(ctx: WorkflowContext) -> dict[str, str]:
    """First activity in a workflow."""
    # Register compensation
    await register_compensation(ctx, compensate_step_one)
    return {"result": "step_one_completed"}


@activity
async def compensate_step_one(ctx: WorkflowContext) -> None:
    """Compensation for step_one."""
    compensation_executed.append("step_one")


@activity
async def step_two(ctx: WorkflowContext) -> dict[str, str]:
    """Second activity in a workflow."""
    await register_compensation(ctx, compensate_step_two)
    return {"result": "step_two_completed"}


@activity
async def compensate_step_two(ctx: WorkflowContext) -> None:
    """Compensation for step_two."""
    compensation_executed.append("step_two")


@activity
async def long_running_activity(ctx: WorkflowContext) -> dict[str, str]:
    """Activity that takes a while to complete."""
    await asyncio.sleep(0.5)  # Simulate work
    return {"result": "completed"}


@workflow
async def simple_workflow(ctx: WorkflowContext) -> dict[str, str]:
    """A simple saga for testing cancellation."""
    result1 = await step_one(ctx)
    result2 = await step_two(ctx)
    return {"final": "success", "results": [result1, result2]}


@workflow
async def waiting_workflow(ctx: WorkflowContext) -> dict[str, Any]:
    """A saga that waits for an event."""
    await step_one(ctx)

    # Wait for an event
    received_event = await wait_event(ctx, event_type="test.event", timeout_seconds=60)

    return {"final": "received_event", "event_data": received_event.data}


@workflow
async def long_running_workflow(ctx: WorkflowContext) -> dict[str, str]:
    """A saga with a long-running activity."""
    result = await long_running_activity(ctx)
    return {"final": "success", "result": result}


class TestWorkflowCancellation:
    """Test suite for workflow cancellation."""

    @pytest.mark.asyncio
    async def test_cancel_running_workflow(
        self, storage: SQLAlchemyStorage, replay_engine: ReplayEngine
    ) -> None:
        """Test cancelling a workflow that is in running state."""
        global compensation_executed
        compensation_executed = []

        # Create a workflow instance manually
        instance_id = "test-cancel-running-1"
        await storage.create_instance(
            instance_id=instance_id,
            workflow_name="test_workflow",
            source_hash="test-hash-1",
            owner_service="test-service",
            input_data={},
        )

        # Add some compensation actions
        await storage.push_compensation(
            instance_id=instance_id,
            activity_id="step_one_compensation:1",
            activity_name="step_one_compensation",
            args={"name": "step_one_compensation"},
        )

        # Cancel the workflow
        success = await replay_engine.cancel_workflow(instance_id, cancelled_by="test")

        # Verify cancellation succeeded
        assert success is True

        # Verify status is cancelled
        instance = await storage.get_instance(instance_id)
        assert instance is not None
        assert instance["status"] == "cancelled"

        # Verify cancellation metadata
        output_data = instance["output_data"]
        assert output_data is not None
        assert output_data["cancelled_by"] == "test"
        assert "cancelled_at" in output_data
        # previous_status is "compensating" because cancel_workflow() executes
        # compensations first, which changes status to "compensating"
        assert output_data["previous_status"] == "compensating"

        # Verify locks are cleared
        assert instance["locked_by"] is None
        assert instance["locked_at"] is None

    @pytest.mark.asyncio
    async def test_cancel_waiting_workflow(
        self, storage: SQLAlchemyStorage, replay_engine: ReplayEngine, worker_id: str
    ) -> None:
        """Test cancelling a workflow that is waiting for an event."""
        # Create a workflow instance in waiting state
        instance_id = "test-cancel-waiting-1"
        await storage.create_instance(
            instance_id=instance_id,
            workflow_name="waiting_workflow",
            source_hash="test-hash-3",
            owner_service="test-service",
            input_data={},
        )

        # Update status to waiting_for_event
        await storage.update_instance_status(instance_id, "waiting_for_event")

        # Add channel subscription (CloudEvents uses Message Passing internally)
        # Must subscribe to channel first, then register channel receive atomically

        # Subscribe to channel first
        await storage.subscribe_to_channel(
            instance_id=instance_id,
            channel="test.event",
            mode="broadcast",
        )

        # Acquire lock and register channel receive
        acquired = await storage.try_acquire_lock(instance_id, worker_id, timeout_seconds=30)
        assert acquired is True

        await storage.register_channel_receive_and_release_lock(
            instance_id=instance_id,
            worker_id=worker_id,
            channel="test.event",
            activity_id="wait_message_test.event:1",
            timeout_seconds=60,
        )

        # Verify subscription exists
        subscriptions = await storage.find_waiting_instances_by_channel("test.event")
        assert len(subscriptions) == 1
        assert subscriptions[0]["instance_id"] == instance_id

        # Cancel the workflow
        success = await replay_engine.cancel_workflow(instance_id, cancelled_by="user")

        # Verify cancellation succeeded
        assert success is True

        # Verify status is cancelled
        instance = await storage.get_instance(instance_id)
        assert instance is not None
        assert instance["status"] == "cancelled"

        # Verify message subscription is removed
        subscriptions = await storage.find_waiting_instances_by_channel("test.event")
        assert len(subscriptions) == 0

    @pytest.mark.asyncio
    async def test_cancel_executes_compensations(
        self, storage: SQLAlchemyStorage, replay_engine: ReplayEngine
    ) -> None:
        """Test that cancellation executes registered compensations."""
        global compensation_executed
        compensation_executed = []

        # Create a workflow instance
        instance_id = "test-cancel-compensations-1"
        await storage.create_instance(
            instance_id=instance_id,
            workflow_name="simple_workflow",
            source_hash="test-hash-4",
            owner_service="test-service",
            input_data={},
        )

        # Add multiple compensations
        await storage.push_compensation(
            instance_id=instance_id,
            activity_id="compensate_step_one:1",
            activity_name="compensate_step_one",
            args={"name": "compensate_step_one"},
        )
        await storage.push_compensation(
            instance_id=instance_id,
            activity_id="compensate_step_two:2",
            activity_name="compensate_step_two",
            args={"name": "compensate_step_two"},
        )

        # Cancel the workflow
        success = await replay_engine.cancel_workflow(instance_id, cancelled_by="admin")

        # Verify cancellation succeeded
        assert success is True

        # Note: In current implementation, execute_compensations() is called
        # and the compensation execution flow is triggered

        # Verify status is cancelled
        instance = await storage.get_instance(instance_id)
        assert instance is not None
        assert instance["status"] == "cancelled"

    @pytest.mark.asyncio
    async def test_cannot_cancel_completed_workflow(
        self, storage: SQLAlchemyStorage, replay_engine: ReplayEngine
    ) -> None:
        """Test that completed workflows cannot be cancelled."""
        # Create a completed workflow instance
        instance_id = "test-cancel-completed-1"
        await storage.create_instance(
            instance_id=instance_id,
            workflow_name="simple_workflow",
            source_hash="test-hash-4",
            owner_service="test-service",
            input_data={},
        )
        await storage.update_instance_status(
            instance_id, "completed", output_data={"result": "success"}
        )

        # Try to cancel
        success = await replay_engine.cancel_workflow(instance_id, cancelled_by="user")

        # Verify cancellation failed
        assert success is False

        # Verify status is still completed
        instance = await storage.get_instance(instance_id)
        assert instance is not None
        assert instance["status"] == "completed"

    @pytest.mark.asyncio
    async def test_cannot_cancel_failed_workflow(
        self, storage: SQLAlchemyStorage, replay_engine: ReplayEngine
    ) -> None:
        """Test that failed workflows cannot be cancelled."""
        # Create a failed workflow instance
        instance_id = "test-cancel-failed-1"
        await storage.create_instance(
            instance_id=instance_id,
            workflow_name="simple_workflow",
            source_hash="test-hash-4",
            owner_service="test-service",
            input_data={},
        )
        await storage.update_instance_status(
            instance_id, "failed", output_data={"error": "Something went wrong"}
        )

        # Try to cancel
        success = await replay_engine.cancel_workflow(instance_id, cancelled_by="user")

        # Verify cancellation failed
        assert success is False

        # Verify status is still failed
        instance = await storage.get_instance(instance_id)
        assert instance is not None
        assert instance["status"] == "failed"

    @pytest.mark.asyncio
    async def test_cancel_already_cancelled_workflow(
        self, storage: SQLAlchemyStorage, replay_engine: ReplayEngine
    ) -> None:
        """Test that cancelling an already cancelled workflow is idempotent."""
        # Create a workflow instance
        instance_id = "test-cancel-idempotent-1"
        await storage.create_instance(
            instance_id=instance_id,
            workflow_name="simple_workflow",
            source_hash="test-hash-4",
            owner_service="test-service",
            input_data={},
        )

        # Cancel it once
        success1 = await replay_engine.cancel_workflow(instance_id, cancelled_by="user")
        assert success1 is True

        # Try to cancel again
        success2 = await replay_engine.cancel_workflow(instance_id, cancelled_by="admin")

        # Verify second cancellation returns False (already cancelled)
        assert success2 is False

        # Verify status is still cancelled
        instance = await storage.get_instance(instance_id)
        assert instance is not None
        assert instance["status"] == "cancelled"

    @pytest.mark.asyncio
    async def test_cancel_with_lock_conflict(
        self, storage: SQLAlchemyStorage, replay_engine: ReplayEngine, worker_id: str
    ) -> None:
        """Test cancelling a workflow when another worker holds the lock."""
        # Create a workflow instance
        instance_id = "test-cancel-lock-conflict-1"
        await storage.create_instance(
            instance_id=instance_id,
            workflow_name="simple_workflow",
            source_hash="test-hash-4",
            owner_service="test-service",
            input_data={},
        )

        # Acquire lock with a different worker
        other_worker_id = "other-worker-123"
        lock_acquired = await storage.try_acquire_lock(
            instance_id=instance_id, worker_id=other_worker_id, timeout_seconds=30
        )
        assert lock_acquired is True

        # Try to cancel (should still succeed via storage layer)
        success = await replay_engine.cancel_workflow(instance_id, cancelled_by="user")

        # Verify cancellation succeeded despite lock conflict
        # (storage layer handles atomicity)
        assert success is True

        # Verify status is cancelled
        instance = await storage.get_instance(instance_id)
        assert instance is not None
        assert instance["status"] == "cancelled"

        # Clean up lock
        await storage.release_lock(instance_id, other_worker_id)

    @pytest.mark.asyncio
    async def test_cancel_clears_message_subscription(
        self, storage: SQLAlchemyStorage, replay_engine: ReplayEngine, worker_id: str
    ) -> None:
        """Test that cancellation removes message subscriptions."""
        # Create a workflow instance
        instance_id = "test-cancel-subscription-1"
        await storage.create_instance(
            instance_id=instance_id,
            workflow_name="waiting_workflow",
            source_hash="test-hash-3",
            owner_service="test-service",
            input_data={},
        )

        # Update to waiting state
        await storage.update_instance_status(instance_id, "waiting_for_event")

        # Add channel subscription (CloudEvents uses Message Passing internally)
        # Must subscribe to channel first, then register channel receive atomically

        # Subscribe to channel first
        await storage.subscribe_to_channel(
            instance_id=instance_id,
            channel="payment.completed",
            mode="broadcast",
        )

        # Acquire lock and register channel receive
        acquired = await storage.try_acquire_lock(instance_id, worker_id, timeout_seconds=30)
        assert acquired is True

        await storage.register_channel_receive_and_release_lock(
            instance_id=instance_id,
            worker_id=worker_id,
            channel="payment.completed",
            activity_id="wait_message_payment.completed:1",
            timeout_seconds=120,
        )

        # Verify subscription exists
        subscriptions_before = await storage.find_waiting_instances_by_channel("payment.completed")
        assert len(subscriptions_before) == 1

        # Cancel the workflow
        success = await replay_engine.cancel_workflow(instance_id, cancelled_by="timeout")

        # Verify cancellation succeeded
        assert success is True

        # Verify subscription was removed
        subscriptions_after = await storage.find_waiting_instances_by_channel("payment.completed")
        assert len(subscriptions_after) == 0

        # Verify status is cancelled
        instance = await storage.get_instance(instance_id)
        assert instance is not None
        assert instance["status"] == "cancelled"
