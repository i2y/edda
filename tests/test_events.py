"""
Tests for event handling module.

Tests cover:
- wait_event functionality
- Event subscription registration
- Event-based workflow resumption
- Event filtering
"""

import pytest
import pytest_asyncio

from edda import workflow
from edda.context import WorkflowContext
from edda.events import WaitForEventException, wait_event
from edda.replay import ReplayEngine
from edda.workflow import set_replay_engine


@pytest.mark.asyncio
class TestWaitEvent:
    """Test suite for wait_event functionality."""

    @pytest_asyncio.fixture
    async def workflow_instance(self, sqlite_storage, create_test_instance):
        """Create a workflow instance for testing."""
        instance_id = "test-event-instance-001"
        await create_test_instance(
            instance_id=instance_id,
            workflow_name="test_workflow",
            owner_service="test-service",
            input_data={"test": "data"},
        )
        await sqlite_storage.update_instance_status(instance_id, "running")
        return instance_id

    async def test_wait_event_raises_exception_during_normal_execution(
        self, sqlite_storage, workflow_instance, create_test_instance
    ):
        """Test that wait_event raises WaitForEventException during normal execution."""
        ctx = WorkflowContext(
            instance_id=workflow_instance,
            workflow_name="test_workflow",
            storage=sqlite_storage,
            worker_id="worker-1",
            is_replaying=False,
        )

        # Should raise exception to pause workflow
        with pytest.raises(WaitForEventException) as exc_info:
            await wait_event(
                ctx,
                event_type="payment.completed",
                timeout_seconds=300,
            )

        # Verify exception details
        assert exc_info.value.event_type == "payment.completed"
        assert exc_info.value.timeout_seconds == 300

    async def test_wait_event_raises_exception_with_subscription_details(
        self, sqlite_storage, workflow_instance, create_test_instance
    ):
        """Test that wait_event raises exception with subscription details.

        Note: Event subscription registration is handled atomically by the
        ReplayEngine, not by wait_event directly.
        """
        ctx = WorkflowContext(
            instance_id=workflow_instance,
            workflow_name="test_workflow",
            storage=sqlite_storage,
            worker_id="worker-1",
            is_replaying=False,
        )

        # Call wait_event (will raise exception with subscription details)
        with pytest.raises(WaitForEventException) as exc_info:
            await wait_event(
                ctx,
                event_type="order.created",
                timeout_seconds=600,
            )

        # Verify exception contains subscription details
        assert exc_info.value.event_type == "order.created"
        assert exc_info.value.timeout_seconds == 600
        assert exc_info.value.activity_id == "wait_event_order.created:1"  # First auto-generated activity_id

        # Subscription is NOT registered yet (handled by ReplayEngine atomically)
        subscriptions = await sqlite_storage.find_waiting_instances("order.created")
        assert len(subscriptions) == 0

    async def test_wait_event_generates_activity_id(
        self, sqlite_storage, workflow_instance, create_test_instance
    ):
        """Test that wait_event generates activity_id and tracks it."""
        ctx = WorkflowContext(
            instance_id=workflow_instance,
            workflow_name="test_workflow",
            storage=sqlite_storage,
            worker_id="worker-1",
            is_replaying=False,
        )

        assert len(ctx.executed_activity_ids) == 0

        # Call wait_event
        with pytest.raises(WaitForEventException):
            await wait_event(ctx, event_type="test.event")

        # Activity ID should be tracked
        assert len(ctx.executed_activity_ids) == 1
        assert "wait_event_test.event:1" in ctx.executed_activity_ids

    async def test_wait_event_returns_cached_data_during_replay(
        self, sqlite_storage, workflow_instance, create_test_instance
    ):
        """Test that wait_event returns cached event data during replay."""
        # Add event data to history
        await sqlite_storage.append_history(
            workflow_instance,
            activity_id="wait_event_test.event:1",
            event_type="EventReceived",
            event_data={
                "event_data": {
                    "order_id": "ORDER-123",
                    "status": "completed",
                }
            },
        )

        # Create replay context
        ctx = WorkflowContext(
            instance_id=workflow_instance,
            workflow_name="test_workflow",
            storage=sqlite_storage,
            worker_id="worker-1",
            is_replaying=True,
        )

        # Load history
        await ctx._load_history()

        # wait_event should return ReceivedEvent without raising exception
        received_event = await wait_event(ctx, event_type="test.event")

        # Verify ReceivedEvent properties
        assert received_event.data == {
            "order_id": "ORDER-123",
            "status": "completed",
        }
        # For backward compatibility, old format still works
        assert received_event.type == "unknown"  # Old format has no metadata
        assert received_event.source == "unknown"


@pytest.mark.asyncio
class TestEventBasedWorkflowResumption:
    """Test suite for event-based workflow resumption."""

    @pytest.fixture
    def replay_engine(self, sqlite_storage):
        """Create and configure ReplayEngine."""
        engine = ReplayEngine(
            storage=sqlite_storage,
            service_name="test-service",
            worker_id="worker-event-test",
        )
        set_replay_engine(engine)
        return engine

    async def test_workflow_pauses_on_wait_event(
        self, replay_engine, sqlite_storage, create_test_instance
    ):
        """Test that workflow pauses when wait_event is called."""

        @workflow
        async def event_waiting_workflow(ctx: WorkflowContext, order_id: str) -> dict:
            # Wait for an event
            received_event = await wait_event(
                ctx,
                event_type="payment.completed",
            )

            return {"event_received": received_event.data}

        # Start workflow
        instance_id = await event_waiting_workflow.start(order_id="ORDER-123")

        # Verify workflow is in waiting_for_event status
        instance = await sqlite_storage.get_instance(instance_id)
        assert instance["status"] == "waiting_for_event"

        # Verify event subscription was registered
        subscriptions = await sqlite_storage.find_waiting_instances("payment.completed")
        assert len(subscriptions) == 1
        assert subscriptions[0]["instance_id"] == instance_id

    async def test_workflow_resumes_after_event_arrives(
        self, replay_engine, sqlite_storage, create_test_instance
    ):
        """Test that workflow resumes and completes after event arrives."""

        @workflow
        async def event_waiting_workflow(ctx: WorkflowContext, order_id: str) -> dict:
            # Wait for an event
            received_event = await wait_event(
                ctx,
                event_type="payment.completed",
            )

            # Continue execution after event
            return {
                "order_id": order_id,
                "payment_status": received_event.data.get("status"),
                "completed": True,
            }

        # Start workflow (will pause at wait_event)
        instance_id = await event_waiting_workflow.start(order_id="ORDER-456")

        # Verify workflow is waiting
        instance = await sqlite_storage.get_instance(instance_id)
        assert instance["status"] == "waiting_for_event"

        # Simulate event arrival by recording it in history (new format with metadata)
        await sqlite_storage.append_history(
            instance_id,
            activity_id="wait_event_payment.completed:1",
            event_type="EventReceived",
            event_data={
                "payload": {
                    "order_id": "ORDER-456",
                    "status": "success",
                    "amount": 99.99,
                },
                "metadata": {
                    "type": "payment.completed",
                    "source": "test-service",
                    "id": "test-event-123",
                    "time": "2025-10-29T12:34:56Z",
                },
                "extensions": {},
            },
        )

        # Resume workflow
        await replay_engine.resume_workflow(
            instance_id=instance_id,
            workflow_func=event_waiting_workflow.func,
        )

        # Verify workflow completed
        instance = await sqlite_storage.get_instance(instance_id)
        assert instance["status"] == "completed"
        assert instance["output_data"]["result"]["payment_status"] == "success"
        assert instance["output_data"]["result"]["completed"] is True


@pytest.mark.asyncio
class TestEventRecording:
    """Test suite for event data recording."""

    @pytest_asyncio.fixture
    async def workflow_instance(self, sqlite_storage, create_test_instance):
        """Create a workflow instance for testing."""
        instance_id = "test-event-recording-001"
        await create_test_instance(
            instance_id=instance_id,
            workflow_name="test_workflow",
            owner_service="test-service",
            input_data={},
        )
        return instance_id

    async def test_record_event_received(
        self, sqlite_storage, workflow_instance, create_test_instance
    ):
        """Test recording received event data."""
        ctx = WorkflowContext(
            instance_id=workflow_instance,
            workflow_name="test_workflow",
            storage=sqlite_storage,
            worker_id="worker-1",
            is_replaying=False,
        )

        event_data = {
            "order_id": "ORDER-789",
            "payment_id": "PAY-123",
            "amount": 199.99,
        }

        # Record event (using explicit activity_id)
        await ctx._record_event_received(activity_id="wait_event_test:1", event_data=event_data)

        # Verify it was recorded in history
        history = await sqlite_storage.get_history(workflow_instance)
        assert len(history) == 1
        assert history[0]["activity_id"] == "wait_event_test:1"
        assert history[0]["event_type"] == "EventReceived"
        assert history[0]["event_data"]["event_data"] == event_data
