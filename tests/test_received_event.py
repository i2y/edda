"""
Tests for ReceivedEvent class and CloudEvents metadata functionality.

This module tests that CloudEvents metadata is properly stored, retrieved,
and accessible through the ReceivedEvent class.
"""

import pytest

from edda import workflow
from edda.context import WorkflowContext
from edda.events import ReceivedEvent, wait_event
from edda.replay import ReplayEngine
from edda.workflow import set_replay_engine


class TestReceivedEvent:
    """Test suite for ReceivedEvent class."""

    def test_received_event_creation(self):
        """Test ReceivedEvent can be created with all properties."""
        event = ReceivedEvent(
            data={"order_id": "ORDER-123", "amount": 99.99},
            type="payment.completed",
            source="payment-service",
            id="event-123",
            time="2025-10-29T12:34:56Z",
            datacontenttype="application/json",
            subject="order/ORDER-123",
            extensions={"custom_attr": "value"},
        )

        assert event.data == {"order_id": "ORDER-123", "amount": 99.99}
        assert event.type == "payment.completed"
        assert event.source == "payment-service"
        assert event.id == "event-123"
        assert event.time == "2025-10-29T12:34:56Z"
        assert event.datacontenttype == "application/json"
        assert event.subject == "order/ORDER-123"
        assert event.extensions == {"custom_attr": "value"}

    def test_received_event_with_minimal_metadata(self):
        """Test ReceivedEvent with only required fields."""
        event = ReceivedEvent(
            data={"test": "data"},
            type="test.event",
            source="test-source",
            id="test-id",
        )

        assert event.data == {"test": "data"}
        assert event.type == "test.event"
        assert event.source == "test-source"
        assert event.id == "test-id"
        assert event.time is None
        assert event.datacontenttype is None
        assert event.subject is None
        assert event.extensions == {}

    def test_received_event_is_immutable(self):
        """Test that ReceivedEvent is immutable (frozen dataclass)."""
        event = ReceivedEvent(
            data={"test": "data"},
            type="test.event",
            source="test-source",
            id="test-id",
        )

        with pytest.raises((AttributeError, TypeError)):  # FrozenInstanceError or AttributeError
            event.type = "modified"  # type: ignore[misc]


@pytest.mark.asyncio
class TestEventMetadataStorage:
    """Test suite for CloudEvents metadata storage and retrieval."""

    @pytest.fixture
    def replay_engine(self, sqlite_storage):
        """Create and configure ReplayEngine."""
        engine = ReplayEngine(
            storage=sqlite_storage,
            service_name="test-service",
            worker_id="worker-metadata-test",
        )
        set_replay_engine(engine)
        return engine

    async def test_event_metadata_stored_in_history(
        self, replay_engine, sqlite_storage, create_test_instance
    ):
        """Test that CloudEvents metadata is stored when event is delivered."""

        @workflow
        async def metadata_test_workflow(ctx: WorkflowContext) -> dict:
            event = await wait_event(ctx, event_type="test.event")
            return {"event_source": event.source, "event_id": event.id}

        # Start workflow
        instance_id = await metadata_test_workflow.start()

        # Verify workflow is waiting
        instance = await sqlite_storage.get_instance(instance_id)
        assert instance["status"] == "waiting_for_event"

        # Simulate event delivery with metadata
        await sqlite_storage.append_history(
            instance_id,
            activity_id="wait_event_test.event:1",
            event_type="EventReceived",
            event_data={
                "payload": {"order_id": "ORDER-123", "status": "completed"},
                "metadata": {
                    "type": "test.event",
                    "source": "test-service",
                    "id": "event-abc123",
                    "time": "2025-10-29T12:34:56Z",
                    "datacontenttype": "application/json",
                    "subject": "test/subject",
                },
                "extensions": {"custom": "value"},
            },
        )

        # Resume workflow
        await replay_engine.resume_workflow(
            instance_id=instance_id,
            workflow_func=metadata_test_workflow.func,
        )

        # Verify workflow completed and metadata was accessible
        instance = await sqlite_storage.get_instance(instance_id)
        assert instance["status"] == "completed"
        result = instance["output_data"]["result"]
        assert result["event_source"] == "test-service"
        assert result["event_id"] == "event-abc123"

    async def test_received_event_metadata_during_replay(
        self, replay_engine, sqlite_storage, create_test_instance
    ):
        """Test that ReceivedEvent metadata is accessible during replay."""

        @workflow
        async def replay_metadata_workflow(ctx: WorkflowContext) -> dict:
            event = await wait_event(ctx, event_type="payment.completed")
            return {
                "payload": event.data,
                "event_type": event.type,
                "event_source": event.source,
                "event_id": event.id,
                "event_time": event.time,
            }

        # Start workflow
        instance_id = await replay_metadata_workflow.start()

        # Simulate event delivery
        await sqlite_storage.append_history(
            instance_id,
            activity_id="wait_event_payment.completed:1",
            event_type="EventReceived",
            event_data={
                "payload": {"order_id": "ORDER-456", "amount": 150.00},
                "metadata": {
                    "type": "payment.completed",
                    "source": "payment-gateway",
                    "id": "pay-xyz789",
                    "time": "2025-10-29T15:00:00Z",
                },
                "extensions": {},
            },
        )

        # Resume workflow
        await replay_engine.resume_workflow(
            instance_id=instance_id,
            workflow_func=replay_metadata_workflow.func,
        )

        # Verify metadata was accessible
        instance = await sqlite_storage.get_instance(instance_id)
        result = instance["output_data"]["result"]
        assert result["payload"] == {"order_id": "ORDER-456", "amount": 150.00}
        assert result["event_type"] == "payment.completed"
        assert result["event_source"] == "payment-gateway"
        assert result["event_id"] == "pay-xyz789"
        assert result["event_time"] == "2025-10-29T15:00:00Z"

    async def test_backward_compatibility_with_old_format(
        self, replay_engine, sqlite_storage, create_test_instance
    ):
        """Test backward compatibility with old event_data format."""

        @workflow
        async def backward_compat_workflow(ctx: WorkflowContext) -> dict:
            event = await wait_event(ctx, event_type="legacy.event")
            return {"payload": event.data}

        # Start workflow
        instance_id = await backward_compat_workflow.start()

        # Simulate event delivery using OLD format (event_data directly)
        await sqlite_storage.append_history(
            instance_id,
            activity_id="wait_event_legacy.event:1",
            event_type="EventReceived",
            event_data={"event_data": {"order_id": "LEGACY-123", "status": "success"}},
        )

        # Resume workflow
        await replay_engine.resume_workflow(
            instance_id=instance_id,
            workflow_func=backward_compat_workflow.func,
        )

        # Verify workflow completed and data was accessible
        instance = await sqlite_storage.get_instance(instance_id)
        assert instance["status"] == "completed"
        assert instance["output_data"]["result"]["payload"] == {
            "order_id": "LEGACY-123",
            "status": "success",
        }
