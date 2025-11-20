"""
Tests for binary data (bytes) storage support.

This module tests the BLOB-based binary data support for:
- WorkflowHistory (append_history, get_history)
- OutboxEvent (add_outbox_event, get_pending_outbox_events)
"""

import pytest

from edda.storage.sqlalchemy_storage import SQLAlchemyStorage


class TestBinaryDataStorage:
    """Test binary data storage in WorkflowHistory and OutboxEvent tables."""

    @pytest.mark.asyncio
    async def test_append_history_with_binary_data(self, sqlite_storage: SQLAlchemyStorage):
        """Test appending binary data to workflow history."""
        # Store workflow definition first
        await sqlite_storage.upsert_workflow_definition(
            workflow_name="test_workflow",
            source_hash="hash123",
            source_code="# test code",
        )

        # Create workflow instance
        instance_id = "test-binary-history"
        await sqlite_storage.create_instance(
            instance_id=instance_id,
            workflow_name="test_workflow",
            source_hash="hash123",
            owner_service="test-service",
            input_data={"test": "data"},
        )

        # Binary data (e.g., Protobuf serialized message)
        binary_data = b"\x08\x96\x01\x12\x0bHello World"

        # Append binary data to history
        await sqlite_storage.append_history(
            instance_id=instance_id,
            activity_id="binary_activity:1",
            event_type="ActivityCompleted",
            event_data=binary_data,
        )

        # Retrieve history
        history = await sqlite_storage.get_history(instance_id)

        # Verify
        assert len(history) == 1
        assert history[0]["activity_id"] == "binary_activity:1"
        assert history[0]["event_type"] == "ActivityCompleted"
        assert history[0]["event_data"] == binary_data
        assert isinstance(history[0]["event_data"], bytes)

    @pytest.mark.asyncio
    async def test_append_history_with_json_data(self, sqlite_storage: SQLAlchemyStorage):
        """Test appending JSON data to workflow history (existing behavior)."""
        # Store workflow definition first
        await sqlite_storage.upsert_workflow_definition(
            workflow_name="test_workflow",
            source_hash="hash123",
            source_code="# test code",
        )

        # Create workflow instance
        instance_id = "test-json-history"
        await sqlite_storage.create_instance(
            instance_id=instance_id,
            workflow_name="test_workflow",
            source_hash="hash123",
            owner_service="test-service",
            input_data={"test": "data"},
        )

        # JSON data
        json_data = {"status": "completed", "result": 42}

        # Append JSON data to history
        await sqlite_storage.append_history(
            instance_id=instance_id,
            activity_id="json_activity:1",
            event_type="ActivityCompleted",
            event_data=json_data,
        )

        # Retrieve history
        history = await sqlite_storage.get_history(instance_id)

        # Verify
        assert len(history) == 1
        assert history[0]["activity_id"] == "json_activity:1"
        assert history[0]["event_type"] == "ActivityCompleted"
        assert history[0]["event_data"] == json_data
        assert isinstance(history[0]["event_data"], dict)

    @pytest.mark.asyncio
    async def test_append_history_mixed_data_types(self, sqlite_storage: SQLAlchemyStorage):
        """Test appending both binary and JSON data to the same workflow."""
        # Store workflow definition first
        await sqlite_storage.upsert_workflow_definition(
            workflow_name="test_workflow",
            source_hash="hash123",
            source_code="# test code",
        )

        # Create workflow instance
        instance_id = "test-mixed-history"
        await sqlite_storage.create_instance(
            instance_id=instance_id,
            workflow_name="test_workflow",
            source_hash="hash123",
            owner_service="test-service",
            input_data={"test": "data"},
        )

        # Append binary data
        binary_data = b"\x08\x96\x01"
        await sqlite_storage.append_history(
            instance_id=instance_id,
            activity_id="activity:1",
            event_type="ActivityCompleted",
            event_data=binary_data,
        )

        # Append JSON data
        json_data = {"status": "completed"}
        await sqlite_storage.append_history(
            instance_id=instance_id,
            activity_id="activity:2",
            event_type="ActivityCompleted",
            event_data=json_data,
        )

        # Retrieve history
        history = await sqlite_storage.get_history(instance_id)

        # Verify
        assert len(history) == 2
        assert history[0]["event_data"] == binary_data
        assert isinstance(history[0]["event_data"], bytes)
        assert history[1]["event_data"] == json_data
        assert isinstance(history[1]["event_data"], dict)

    @pytest.mark.asyncio
    async def test_add_outbox_event_with_binary_data(self, sqlite_storage: SQLAlchemyStorage):
        """Test adding binary data to outbox events."""
        # Binary data (e.g., Protobuf CloudEvent)
        binary_data = b"\x08\x96\x01\x12\x0bHello World"

        # Add binary event to outbox
        await sqlite_storage.add_outbox_event(
            event_id="binary-event-1",
            event_type="test.binary.event",
            event_source="test-service",
            event_data=binary_data,
            content_type="application/protobuf",
        )

        # Retrieve pending events
        events = await sqlite_storage.get_pending_outbox_events(limit=10)

        # Verify
        assert len(events) == 1
        assert events[0]["event_id"] == "binary-event-1"
        assert events[0]["event_type"] == "test.binary.event"
        assert events[0]["event_data"] == binary_data
        assert isinstance(events[0]["event_data"], bytes)
        assert events[0]["content_type"] == "application/protobuf"

    @pytest.mark.asyncio
    async def test_add_outbox_event_with_json_data(self, sqlite_storage: SQLAlchemyStorage):
        """Test adding JSON data to outbox events (existing behavior)."""
        # JSON data
        json_data = {"message": "Hello World", "count": 42}

        # Add JSON event to outbox
        await sqlite_storage.add_outbox_event(
            event_id="json-event-1",
            event_type="test.json.event",
            event_source="test-service",
            event_data=json_data,
            content_type="application/json",
        )

        # Retrieve pending events
        events = await sqlite_storage.get_pending_outbox_events(limit=10)

        # Verify
        assert len(events) == 1
        assert events[0]["event_id"] == "json-event-1"
        assert events[0]["event_type"] == "test.json.event"
        assert events[0]["event_data"] == json_data
        assert isinstance(events[0]["event_data"], dict)
        assert events[0]["content_type"] == "application/json"

    @pytest.mark.asyncio
    async def test_add_outbox_event_mixed_data_types(self, sqlite_storage: SQLAlchemyStorage):
        """Test adding both binary and JSON events to the outbox."""
        # Add binary event
        binary_data = b"\x08\x96\x01"
        await sqlite_storage.add_outbox_event(
            event_id="event-1",
            event_type="test.binary",
            event_source="test-service",
            event_data=binary_data,
            content_type="application/protobuf",
        )

        # Add JSON event
        json_data = {"status": "ok"}
        await sqlite_storage.add_outbox_event(
            event_id="event-2",
            event_type="test.json",
            event_source="test-service",
            event_data=json_data,
            content_type="application/json",
        )

        # Retrieve pending events
        events = await sqlite_storage.get_pending_outbox_events(limit=10)

        # Verify
        assert len(events) == 2
        # First event (binary)
        assert events[0]["event_data"] == binary_data
        assert isinstance(events[0]["event_data"], bytes)
        # Second event (JSON)
        assert events[1]["event_data"] == json_data
        assert isinstance(events[1]["event_data"], dict)

    @pytest.mark.asyncio
    async def test_binary_data_zero_size_overhead(self, sqlite_storage: SQLAlchemyStorage):
        """Test that binary data has zero size overhead (no base64 encoding)."""
        # Store workflow definition first
        await sqlite_storage.upsert_workflow_definition(
            workflow_name="test_workflow",
            source_hash="hash123",
            source_code="# test code",
        )

        # Create workflow instance
        instance_id = "test-size-overhead"
        await sqlite_storage.create_instance(
            instance_id=instance_id,
            workflow_name="test_workflow",
            source_hash="hash123",
            owner_service="test-service",
            input_data={"test": "data"},
        )

        # 100-byte binary data
        binary_data = b"x" * 100

        # Append binary data
        await sqlite_storage.append_history(
            instance_id=instance_id,
            activity_id="size_test:1",
            event_type="ActivityCompleted",
            event_data=binary_data,
        )

        # Retrieve and verify
        history = await sqlite_storage.get_history(instance_id)
        retrieved_data = history[0]["event_data"]

        # Verify exact size (no overhead)
        assert len(retrieved_data) == 100
        assert retrieved_data == binary_data

    @pytest.mark.asyncio
    async def test_large_binary_data(self, sqlite_storage: SQLAlchemyStorage):
        """Test storing large binary data (1KB+)."""
        # Store workflow definition first
        await sqlite_storage.upsert_workflow_definition(
            workflow_name="test_workflow",
            source_hash="hash123",
            source_code="# test code",
        )

        # Create workflow instance
        instance_id = "test-large-binary"
        await sqlite_storage.create_instance(
            instance_id=instance_id,
            workflow_name="test_workflow",
            source_hash="hash123",
            owner_service="test-service",
            input_data={"test": "data"},
        )

        # 10KB binary data
        binary_data = bytes(range(256)) * 40  # 10,240 bytes

        # Append large binary data
        await sqlite_storage.append_history(
            instance_id=instance_id,
            activity_id="large_binary:1",
            event_type="ActivityCompleted",
            event_data=binary_data,
        )

        # Retrieve and verify
        history = await sqlite_storage.get_history(instance_id)
        retrieved_data = history[0]["event_data"]

        assert len(retrieved_data) == 10240
        assert retrieved_data == binary_data
