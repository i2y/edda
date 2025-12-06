"""
Tests for ReceivedEvent class and CloudEvents metadata functionality.

This module tests that CloudEvents metadata is properly stored, retrieved,
and accessible through the ReceivedEvent class.
"""

import pytest

from edda import workflow
from edda.channels import ReceivedEvent, wait_event
from edda.context import WorkflowContext
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
        assert instance["status"] == "waiting_for_message"

        # Simulate event delivery with metadata (using ChannelMessageReceived format)
        # Note: CloudEvents internally uses Message Passing,
        # so metadata uses ce_ prefix for CloudEvents attributes.
        await sqlite_storage.append_history(
            instance_id,
            activity_id="receive_test.event:1",
            event_type="ChannelMessageReceived",
            event_data={
                "data": {"order_id": "ORDER-123", "status": "completed"},
                "channel": "test.event",
                "id": "msg-001",
                "metadata": {
                    "ce_type": "test.event",
                    "ce_source": "test-service",
                    "ce_id": "event-abc123",
                    "ce_time": "2025-10-29T12:34:56Z",
                    "ce_datacontenttype": "application/json",
                    "ce_subject": "test/subject",
                    "ce_extensions": {"custom": "value"},
                },
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

        # Simulate event delivery (using ChannelMessageReceived format with CloudEvents metadata)
        await sqlite_storage.append_history(
            instance_id,
            activity_id="receive_payment.completed:1",
            event_type="ChannelMessageReceived",
            event_data={
                "data": {"order_id": "ORDER-456", "amount": 150.00},
                "channel": "payment.completed",
                "id": "msg-002",
                "metadata": {
                    "ce_type": "payment.completed",
                    "ce_source": "payment-gateway",
                    "ce_id": "pay-xyz789",
                    "ce_time": "2025-10-29T15:00:00Z",
                },
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

    async def test_minimal_cloudevents_metadata(
        self, replay_engine, sqlite_storage, create_test_instance
    ):
        """Test handling of minimal CloudEvents metadata (only required fields)."""

        @workflow
        async def minimal_metadata_workflow(ctx: WorkflowContext) -> dict:
            event = await wait_event(ctx, event_type="minimal.event")
            return {
                "payload": event.data,
                "source": event.source,
                "event_id": event.id,
            }

        # Start workflow
        instance_id = await minimal_metadata_workflow.start()

        # Simulate event delivery with minimal CloudEvents metadata
        # (no time, datacontenttype, subject, or extensions)
        await sqlite_storage.append_history(
            instance_id,
            activity_id="receive_minimal.event:1",
            event_type="ChannelMessageReceived",
            event_data={
                "data": {"order_id": "MINIMAL-123", "status": "success"},
                "channel": "minimal.event",
                "id": "msg-003",
                "metadata": {
                    "ce_source": "minimal-service",
                    "ce_id": "event-minimal-123",
                },
            },
        )

        # Resume workflow
        await replay_engine.resume_workflow(
            instance_id=instance_id,
            workflow_func=minimal_metadata_workflow.func,
        )

        # Verify workflow completed and minimal metadata was accessible
        instance = await sqlite_storage.get_instance(instance_id)
        assert instance["status"] == "completed"
        result = instance["output_data"]["result"]
        assert result["payload"] == {"order_id": "MINIMAL-123", "status": "success"}
        assert result["source"] == "minimal-service"
        assert result["event_id"] == "event-minimal-123"
