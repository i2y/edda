"""
Tests for distributed event delivery.

These tests verify that only ONE worker processes an event at a time,
preventing race conditions in multi-Pod K8s environments.

Note: CloudEvents internally uses Channel-based Message Queue (receive),
so these tests use channel subscriptions for event delivery.
"""

import asyncio
import contextlib

import pytest
from sqlalchemy.ext.asyncio import create_async_engine

from edda import workflow
from edda.channels import wait_event
from edda.context import WorkflowContext
from edda.replay import ReplayEngine
from edda.storage.sqlalchemy_storage import SQLAlchemyStorage


@pytest.fixture
async def sqlite_storage_dist():
    """Create a fresh SQLite storage for each test."""
    storage = SQLAlchemyStorage(create_async_engine("sqlite+aiosqlite:///:memory:", echo=False))
    await storage.initialize()

    # Create workflow definitions for testing
    await storage.upsert_workflow_definition(
        workflow_name="waiting_workflow",
        source_hash="test-hash-dist",
        source_code="async def waiting_workflow(ctx): pass",
    )

    yield storage
    await storage.close()


@pytest.fixture
async def kairo_app_dist(sqlite_storage_dist):
    """Create a EddaApp instance for testing."""
    # EddaApp needs a db_url, not a storage object
    # For these tests, we'll test the methods directly on storage/replay engine
    # This fixture is kept for compatibility but returns None
    yield None


class TestDistributedEventDelivery:
    """Test distributed event delivery with lock-first pattern."""

    @pytest.mark.asyncio
    async def test_lock_first_prevents_race_condition(self, sqlite_storage_dist):
        """Test that lock-first pattern prevents multiple workers from processing same event."""

        # Define a workflow that waits for an event
        @workflow
        async def waiting_workflow(ctx: WorkflowContext, order_id: str):
            # Wait for payment event
            received_event = await wait_event(ctx, event_type="payment.completed")
            return {"status": "completed", "payment": received_event.data}

        # Start workflow instance
        engine1 = ReplayEngine(
            storage=sqlite_storage_dist, service_name="test-service", worker_id="worker-1"
        )
        instance_id = await engine1.start_workflow(
            workflow_func=waiting_workflow,
            workflow_name="waiting_workflow",
            input_data={"order_id": "order-123"},
        )

        # Verify workflow is waiting
        instance = await sqlite_storage_dist.get_instance(instance_id)
        assert instance is not None
        assert instance["status"] == "waiting_for_message"
        assert instance["locked_by"] is None  # Lock should be released

        # Simulate two workers trying to deliver the same event simultaneously
        worker1_acquired = []
        worker2_acquired = []

        async def worker1_deliver():
            # Worker 1 tries to acquire lock
            acquired = await sqlite_storage_dist.try_acquire_lock(
                instance_id, "worker-1", timeout_seconds=30
            )
            worker1_acquired.append(acquired)
            if acquired:
                await asyncio.sleep(0.1)  # Simulate processing time
                await sqlite_storage_dist.release_lock(instance_id, "worker-1")

        async def worker2_deliver():
            # Worker 2 tries to acquire lock (should fail if worker 1 has it)
            await asyncio.sleep(0.01)  # Slight delay to ensure worker 1 goes first
            acquired = await sqlite_storage_dist.try_acquire_lock(
                instance_id, "worker-2", timeout_seconds=30
            )
            worker2_acquired.append(acquired)
            if acquired:
                await sqlite_storage_dist.release_lock(instance_id, "worker-2")

        # Run both workers concurrently
        await asyncio.gather(worker1_deliver(), worker2_deliver())

        # Verify only ONE worker acquired the lock
        assert worker1_acquired == [True]
        assert worker2_acquired == [False]

    @pytest.mark.asyncio
    async def test_event_delivery_acquires_lock_first(self, sqlite_storage_dist):
        """Test that event delivery logic acquires lock before processing."""

        # Define a workflow that waits for an event
        @workflow
        async def payment_workflow(ctx: WorkflowContext, order_id: str):
            received_event = await wait_event(ctx, event_type="payment.completed")
            return {"payment_received": received_event.data}

        # Create engine and start workflow
        engine = ReplayEngine(
            storage=sqlite_storage_dist, service_name="test-service", worker_id="worker-1"
        )

        # Need to add workflow definition for payment_workflow
        await sqlite_storage_dist.upsert_workflow_definition(
            workflow_name="payment_workflow",
            source_hash="test-hash-payment",
            source_code="async def payment_workflow(ctx): pass",
        )

        instance_id = await engine.start_workflow(
            workflow_func=payment_workflow,
            workflow_name="payment_workflow",
            input_data={"order_id": "order-456"},
        )

        # Verify workflow is waiting and lock is released
        instance = await sqlite_storage_dist.get_instance(instance_id)
        assert instance["status"] == "waiting_for_message"
        assert instance["locked_by"] is None

        # Manually acquire lock (simulating another worker)
        acquired = await sqlite_storage_dist.try_acquire_lock(
            instance_id, "other-worker", timeout_seconds=30
        )
        assert acquired is True

        # Try to resume workflow (should fail because lock is held by other-worker)
        event_data = {"amount": 100, "currency": "USD"}

        # Add message to history manually (simulating message delivery)
        await sqlite_storage_dist.append_history(
            instance_id,
            activity_id="receive_payment.completed:1",
            event_type="ChannelMessageReceived",
            event_data={
                "data": event_data,
                "channel": "payment.completed",
                "id": "test-msg-1",
                "metadata": {},
            },
        )
        await sqlite_storage_dist.unsubscribe_from_channel(instance_id, "payment.completed")

        # Try to resume with a different worker (should fail due to lock)
        engine2 = ReplayEngine(
            storage=sqlite_storage_dist, service_name="test-service", worker_id="worker-2"
        )
        try:
            # This should fail because other-worker holds the lock
            await engine2.resume_workflow(
                instance_id=instance_id,
                workflow_func=payment_workflow,
            )
            resumed = True
        except Exception:
            resumed = False

        # Should have failed because lock was held
        assert resumed is False

        # Release the lock
        await sqlite_storage_dist.release_lock(instance_id, "other-worker")

        # Now resume should succeed
        await engine.resume_workflow(
            instance_id=instance_id,
            workflow_func=payment_workflow,
        )

        # Verify workflow completed
        instance = await sqlite_storage_dist.get_instance(instance_id)
        assert instance["status"] == "completed"

    @pytest.mark.asyncio
    async def test_multiple_waiting_workflows_one_lock_per_workflow(self, sqlite_storage_dist):
        """Test that each workflow gets its own lock when multiple workflows wait."""

        @workflow
        async def order_workflow(ctx: WorkflowContext, order_id: str):
            received_event = await wait_event(ctx, event_type="order.payment")
            return {"order": order_id, "payment": received_event.data}

        engine = ReplayEngine(
            storage=sqlite_storage_dist, service_name="test-service", worker_id="worker-1"
        )

        # Add workflow definition for order_workflow
        await sqlite_storage_dist.upsert_workflow_definition(
            workflow_name="order_workflow",
            source_hash="test-hash-order",
            source_code="async def order_workflow(ctx): pass",
        )

        # Start 3 workflows
        instance_ids = []
        for i in range(3):
            instance_id = await engine.start_workflow(
                workflow_func=order_workflow,
                workflow_name="order_workflow",
                input_data={"order_id": f"order-{i}"},
            )
            instance_ids.append(instance_id)

        # Verify all are waiting with no locks
        for instance_id in instance_ids:
            instance = await sqlite_storage_dist.get_instance(instance_id)
            assert instance["status"] == "waiting_for_message"
            assert instance["locked_by"] is None

        # Acquire lock for first workflow
        acquired = await sqlite_storage_dist.try_acquire_lock(
            instance_ids[0], "worker-1", timeout_seconds=30
        )
        assert acquired is True

        # Should be able to acquire locks for other workflows
        acquired = await sqlite_storage_dist.try_acquire_lock(
            instance_ids[1], "worker-2", timeout_seconds=30
        )
        assert acquired is True

        acquired = await sqlite_storage_dist.try_acquire_lock(
            instance_ids[2], "worker-3", timeout_seconds=30
        )
        assert acquired is True

    @pytest.mark.asyncio
    async def test_lock_released_after_event_delivery_error(self, sqlite_storage_dist):
        """Test that lock is released even if event delivery fails."""

        @workflow
        async def failing_workflow(ctx: WorkflowContext):
            await wait_event(ctx, event_type="test.event")
            # This will cause an error during resume
            raise ValueError("Simulated error")

        engine = ReplayEngine(
            storage=sqlite_storage_dist, service_name="test-service", worker_id="worker-1"
        )

        # Add workflow definition
        await sqlite_storage_dist.upsert_workflow_definition(
            workflow_name="failing_workflow",
            source_hash="test-hash-failing",
            source_code="async def failing_workflow(ctx): pass",
        )

        instance_id = await engine.start_workflow(
            workflow_func=failing_workflow,
            workflow_name="failing_workflow",
            input_data={},
        )

        # Verify waiting
        instance = await sqlite_storage_dist.get_instance(instance_id)
        assert instance["status"] == "waiting_for_message"

        # Simulate message delivery and resume (will fail)
        await sqlite_storage_dist.append_history(
            instance_id,
            activity_id="receive_test.event:1",
            event_type="ChannelMessageReceived",
            event_data={
                "data": {"data": "test"},
                "channel": "test.event",
                "id": "test-msg-2",
                "metadata": {},
            },
        )
        await sqlite_storage_dist.unsubscribe_from_channel(instance_id, "test.event")

        # Try to resume (will fail with error)
        with contextlib.suppress(Exception):
            await engine.resume_workflow(
                instance_id=instance_id,
                workflow_func=failing_workflow,
            )

        # Verify lock was released despite error
        instance = await sqlite_storage_dist.get_instance(instance_id)
        assert instance["locked_by"] is None

        # Verify workflow marked as failed
        assert instance["status"] == "failed"
