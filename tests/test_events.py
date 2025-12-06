"""
Tests for event handling module.

Tests cover:
- wait_event functionality
- Event subscription registration (via channel subscriptions)
- Event-based workflow resumption
- Event filtering

Note: CloudEvents (wait_event) internally uses Channel-based Message Queue (receive),
so these tests verify the underlying channel subscription behavior.
"""

import pytest
import pytest_asyncio

from edda import workflow
from edda.channels import WaitForChannelMessageException, wait_event
from edda.context import WorkflowContext
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
        """Test that wait_event raises WaitForChannelMessageException during normal execution.

        Note: wait_event() internally uses receive(), so it raises
        WaitForChannelMessageException instead of WaitForEventException.
        """
        ctx = WorkflowContext(
            instance_id=workflow_instance,
            workflow_name="test_workflow",
            storage=sqlite_storage,
            worker_id="worker-1",
            is_replaying=False,
        )

        # Should raise exception to pause workflow
        with pytest.raises(WaitForChannelMessageException) as exc_info:
            await wait_event(
                ctx,
                event_type="payment.completed",
                timeout_seconds=300,
            )

        # Verify exception details (channel = event_type in channel layer)
        assert exc_info.value.channel == "payment.completed"
        assert exc_info.value.timeout_seconds == 300

    async def test_wait_event_raises_exception_with_subscription_details(
        self, sqlite_storage, workflow_instance, create_test_instance
    ):
        """Test that wait_event raises exception with subscription details.

        Note: Channel subscription registration is handled atomically by the
        ReplayEngine, not by wait_event/receive directly.
        """
        ctx = WorkflowContext(
            instance_id=workflow_instance,
            workflow_name="test_workflow",
            storage=sqlite_storage,
            worker_id="worker-1",
            is_replaying=False,
        )

        # Call wait_event (will raise exception with subscription details)
        with pytest.raises(WaitForChannelMessageException) as exc_info:
            await wait_event(
                ctx,
                event_type="order.created",
                timeout_seconds=600,
            )

        # Verify exception contains subscription details
        assert exc_info.value.channel == "order.created"
        assert exc_info.value.timeout_seconds == 600
        assert (
            exc_info.value.activity_id == "receive_order.created:1"
        )  # First auto-generated activity_id (via receive)

        # Subscription is NOT registered yet (handled by ReplayEngine atomically)
        subscriptions = await sqlite_storage.get_channel_subscribers_waiting("order.created")
        assert len(subscriptions) == 0

    async def test_wait_event_generates_activity_id(
        self, sqlite_storage, workflow_instance, create_test_instance
    ):
        """Test that wait_event generates activity_id and tracks it.

        Note: wait_event() uses receive() internally, so the activity_id
        is generated with receive_ prefix.
        """
        ctx = WorkflowContext(
            instance_id=workflow_instance,
            workflow_name="test_workflow",
            storage=sqlite_storage,
            worker_id="worker-1",
            is_replaying=False,
        )

        assert len(ctx.executed_activity_ids) == 0

        # Call wait_event
        with pytest.raises(WaitForChannelMessageException):
            await wait_event(ctx, event_type="test.event")

        # Activity ID should be tracked (receive_ prefix)
        assert len(ctx.executed_activity_ids) == 1
        assert "receive_test.event:1" in ctx.executed_activity_ids

    async def test_wait_event_returns_cached_data_during_replay(
        self, sqlite_storage, workflow_instance, create_test_instance
    ):
        """Test that wait_event returns cached event data during replay.

        Note: CloudEvents use ChannelMessageReceived event type in history,
        with CloudEvents metadata stored in the message metadata field.
        """
        # Add message data to history (new format with ChannelMessageReceived)
        await sqlite_storage.append_history(
            workflow_instance,
            activity_id="receive_test.event:1",
            event_type="ChannelMessageReceived",
            event_data={
                "data": {
                    "order_id": "ORDER-123",
                    "status": "completed",
                },
                "channel": "test.event",
                "id": "test-msg-1",
                "metadata": {
                    "ce_source": "test-service",
                    "ce_id": "test-event-123",
                    "ce_time": "2025-10-29T12:34:56Z",
                },
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
        # CloudEvents metadata is extracted from message metadata
        assert received_event.type == "test.event"
        assert received_event.source == "test-service"
        assert received_event.id == "test-event-123"


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
        """Test that workflow pauses when wait_event is called.

        Note: CloudEvents use channel subscriptions internally.
        """

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

        # Verify workflow is in waiting_for_message status (channels use this status)
        instance = await sqlite_storage.get_instance(instance_id)
        assert instance["status"] == "waiting_for_message"

        # Verify channel subscription was registered
        subscriptions = await sqlite_storage.get_channel_subscribers_waiting("payment.completed")
        assert len(subscriptions) == 1
        assert subscriptions[0]["instance_id"] == instance_id

    async def test_workflow_resumes_after_event_arrives(
        self, replay_engine, sqlite_storage, create_test_instance
    ):
        """Test that workflow resumes and completes after event arrives.

        Note: CloudEvents use ChannelMessageReceived event type with CloudEvents
        metadata stored in the message metadata field.
        """

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
        assert instance["status"] == "waiting_for_message"

        # Simulate event arrival by recording it in history (ChannelMessageReceived format)
        await sqlite_storage.append_history(
            instance_id,
            activity_id="receive_payment.completed:1",
            event_type="ChannelMessageReceived",
            event_data={
                "data": {
                    "order_id": "ORDER-456",
                    "status": "success",
                    "amount": 99.99,
                },
                "channel": "payment.completed",
                "id": "test-msg-123",
                "metadata": {
                    "ce_type": "payment.completed",
                    "ce_source": "test-service",
                    "ce_id": "test-event-123",
                    "ce_time": "2025-10-29T12:34:56Z",
                },
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
class TestMessageRecording:
    """Test suite for message data recording.

    Note: CloudEvents use Channel-based Message Queue internally. Event recording
    is handled by the storage layer's deliver_channel_message() method, not by
    WorkflowContext directly.
    """

    @pytest_asyncio.fixture
    async def workflow_instance(self, sqlite_storage, create_test_instance):
        """Create a workflow instance for testing."""
        instance_id = "test-message-recording-001"
        await create_test_instance(
            instance_id=instance_id,
            workflow_name="test_workflow",
            owner_service="test-service",
            input_data={},
        )
        return instance_id

    async def test_message_delivery_records_history(
        self, sqlite_storage, workflow_instance, create_test_instance
    ):
        """Test that message delivery records history via storage layer.

        Note: Message recording is now done by storage.deliver_channel_message(),
        not by WorkflowContext._record_event_received().
        """
        # First, subscribe to the channel
        await sqlite_storage.subscribe_to_channel(
            instance_id=workflow_instance,
            channel="test.channel",
            mode="broadcast",
        )

        # Acquire lock and set waiting state
        await sqlite_storage.try_acquire_lock(workflow_instance, "worker-1", timeout_seconds=300)
        await sqlite_storage.register_channel_receive_and_release_lock(
            instance_id=workflow_instance,
            worker_id="worker-1",
            channel="test.channel",
            activity_id="receive_test.channel:1",
        )

        # Deliver a message to the workflow
        message_data = {
            "order_id": "ORDER-789",
            "payment_id": "PAY-123",
            "amount": 199.99,
        }

        result = await sqlite_storage.deliver_channel_message(
            instance_id=workflow_instance,
            channel="test.channel",
            message_id="test-msg-001",
            data=message_data,
            metadata={"source": "test-service"},
            worker_id="worker-1",
        )

        # Verify message was delivered
        assert result is not None

        # Verify it was recorded in history
        history = await sqlite_storage.get_history(workflow_instance)
        assert len(history) == 1
        assert history[0]["activity_id"] == "receive_test.channel:1"
        assert history[0]["event_type"] == "ChannelMessageReceived"
        assert history[0]["event_data"]["data"] == message_data
