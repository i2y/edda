"""
Tests for CloudEvents instance_id-based routing.

These tests verify that:
1. Events with 'eddainstanceid' extension are delivered Point-to-Point (to specific instance only)
2. Events without 'eddainstanceid' are delivered Pub/Sub (to all waiting instances)
3. Point-to-Point delivery handles non-existent instances gracefully
"""

from unittest.mock import MagicMock

import pytest
import pytest_asyncio
from sqlalchemy.ext.asyncio import create_async_engine

from edda import workflow
from edda.app import EddaApp
from edda.channels import wait_event
from edda.context import WorkflowContext
from edda.replay import ReplayEngine
from edda.storage.sqlalchemy_storage import SQLAlchemyStorage


@pytest_asyncio.fixture
async def sqlite_storage_routing():
    """Create a fresh SQLite storage for routing tests."""
    storage = SQLAlchemyStorage(create_async_engine("sqlite+aiosqlite:///:memory:", echo=False))
    await storage.initialize()

    # Create workflow definitions for testing
    await storage.upsert_workflow_definition(
        workflow_name="payment_workflow",
        source_hash="test-hash-payment",
        source_code="async def payment_workflow(ctx): pass",
    )

    yield storage
    await storage.close()


@pytest.mark.asyncio
class TestInstanceIdRouting:
    """Test instance_id-based event routing."""

    async def test_point_to_point_delivery_with_eddainstanceid(self, sqlite_storage_routing):
        """Test that eddainstanceid extension routes to specific instance only."""

        @workflow
        async def payment_workflow(ctx: WorkflowContext, order_id: str):
            event = await wait_event(ctx, event_type="payment.completed")
            return {"order_id": order_id, "payment": event.data}

        # Start two workflow instances, both waiting for 'payment.completed'
        engine = ReplayEngine(
            storage=sqlite_storage_routing,
            service_name="test-service",
            worker_id="test-worker",
        )

        instance_a = await engine.start_workflow(
            workflow_func=payment_workflow,
            workflow_name="payment_workflow",
            input_data={"order_id": "ORDER-001"},
        )

        instance_b = await engine.start_workflow(
            workflow_func=payment_workflow,
            workflow_name="payment_workflow",
            input_data={"order_id": "ORDER-002"},
        )

        # Verify both are waiting
        inst_a = await sqlite_storage_routing.get_instance(instance_a)
        inst_b = await sqlite_storage_routing.get_instance(instance_b)
        assert inst_a["status"] == "waiting_for_message"
        assert inst_b["status"] == "waiting_for_message"

        # Create mock CloudEvent with eddainstanceid pointing to instance_a
        mock_event = MagicMock()
        mock_event.__getitem__ = lambda self, key: {
            "type": "payment.completed",
            "source": "payment-gateway",
            "id": "event-001",
        }.get(key)
        mock_event.get = lambda key, default=None: {
            "time": "2025-01-01T00:00:00Z",
            "datacontenttype": "application/json",
            "subject": None,
        }.get(key, default)
        mock_event.get_data = lambda: {"amount": 100, "transaction_id": "TXN-001"}
        mock_event.get_attributes = lambda: {
            "type": "payment.completed",
            "source": "payment-gateway",
            "id": "event-001",
            "specversion": "1.0",
            "eddainstanceid": instance_a,  # Point-to-Point to instance_a
        }

        # Create EddaApp and call the delivery method directly
        app = EddaApp.__new__(EddaApp)
        app.storage = sqlite_storage_routing
        app.worker_id = "test-worker"

        # Deliver event
        await app._deliver_event_to_waiting_workflows(mock_event)

        # Verify instance_a received the message (status should be 'running')
        inst_a = await sqlite_storage_routing.get_instance(instance_a)
        assert inst_a["status"] == "running"

        # Verify instance_b did NOT receive the message (still waiting)
        inst_b = await sqlite_storage_routing.get_instance(instance_b)
        assert inst_b["status"] == "waiting_for_message"

    async def test_pubsub_delivery_without_eddainstanceid(self, sqlite_storage_routing):
        """Test that events without eddainstanceid are delivered to all waiting instances."""

        @workflow
        async def payment_workflow(ctx: WorkflowContext, order_id: str):
            event = await wait_event(ctx, event_type="payment.completed")
            return {"order_id": order_id, "payment": event.data}

        # Start two workflow instances, both waiting for 'payment.completed'
        engine = ReplayEngine(
            storage=sqlite_storage_routing,
            service_name="test-service",
            worker_id="test-worker",
        )

        instance_a = await engine.start_workflow(
            workflow_func=payment_workflow,
            workflow_name="payment_workflow",
            input_data={"order_id": "ORDER-001"},
        )

        instance_b = await engine.start_workflow(
            workflow_func=payment_workflow,
            workflow_name="payment_workflow",
            input_data={"order_id": "ORDER-002"},
        )

        # Verify both are waiting
        inst_a = await sqlite_storage_routing.get_instance(instance_a)
        inst_b = await sqlite_storage_routing.get_instance(instance_b)
        assert inst_a["status"] == "waiting_for_message"
        assert inst_b["status"] == "waiting_for_message"

        # Create mock CloudEvent WITHOUT eddainstanceid (Pub/Sub)
        mock_event = MagicMock()
        mock_event.__getitem__ = lambda self, key: {
            "type": "payment.completed",
            "source": "payment-gateway",
            "id": "event-002",
        }.get(key)
        mock_event.get = lambda key, default=None: {
            "time": "2025-01-01T00:00:00Z",
            "datacontenttype": "application/json",
            "subject": None,
        }.get(key, default)
        mock_event.get_data = lambda: {"amount": 200, "transaction_id": "TXN-002"}
        mock_event.get_attributes = lambda: {
            "type": "payment.completed",
            "source": "payment-gateway",
            "id": "event-002",
            "specversion": "1.0",
            # No eddainstanceid -> Pub/Sub to all
        }

        # Create EddaApp and call the delivery method directly
        app = EddaApp.__new__(EddaApp)
        app.storage = sqlite_storage_routing
        app.worker_id = "test-worker"

        # Deliver event
        await app._deliver_event_to_waiting_workflows(mock_event)

        # Verify BOTH instances received the message (status should be 'running')
        inst_a = await sqlite_storage_routing.get_instance(instance_a)
        inst_b = await sqlite_storage_routing.get_instance(instance_b)
        assert inst_a["status"] == "running"
        assert inst_b["status"] == "running"

    async def test_point_to_point_nonexistent_instance(self, sqlite_storage_routing):
        """Test delivery to non-existent instance_id handles gracefully."""

        @workflow
        async def payment_workflow(ctx: WorkflowContext, order_id: str):
            event = await wait_event(ctx, event_type="payment.completed")
            return {"order_id": order_id, "payment": event.data}

        # Start one workflow instance
        engine = ReplayEngine(
            storage=sqlite_storage_routing,
            service_name="test-service",
            worker_id="test-worker",
        )

        instance_a = await engine.start_workflow(
            workflow_func=payment_workflow,
            workflow_name="payment_workflow",
            input_data={"order_id": "ORDER-001"},
        )

        # Verify it's waiting
        inst_a = await sqlite_storage_routing.get_instance(instance_a)
        assert inst_a["status"] == "waiting_for_message"

        # Create mock CloudEvent with eddainstanceid pointing to NON-EXISTENT instance
        mock_event = MagicMock()
        mock_event.__getitem__ = lambda self, key: {
            "type": "payment.completed",
            "source": "payment-gateway",
            "id": "event-003",
        }.get(key)
        mock_event.get = lambda key, default=None: {
            "time": "2025-01-01T00:00:00Z",
            "datacontenttype": "application/json",
            "subject": None,
        }.get(key, default)
        mock_event.get_data = lambda: {"amount": 300, "transaction_id": "TXN-003"}
        mock_event.get_attributes = lambda: {
            "type": "payment.completed",
            "source": "payment-gateway",
            "id": "event-003",
            "specversion": "1.0",
            "eddainstanceid": "non-existent-instance-xyz",  # Non-existent
        }

        # Create EddaApp and call the delivery method directly
        app = EddaApp.__new__(EddaApp)
        app.storage = sqlite_storage_routing
        app.worker_id = "test-worker"

        # Deliver event - should NOT raise, just skip
        await app._deliver_event_to_waiting_workflows(mock_event)

        # Verify instance_a was NOT affected (still waiting)
        inst_a = await sqlite_storage_routing.get_instance(instance_a)
        assert inst_a["status"] == "waiting_for_message"

    async def test_point_to_point_instance_not_waiting(self, sqlite_storage_routing):
        """Test Point-to-Point delivery to instance that is not waiting for event."""

        @workflow
        async def payment_workflow(ctx: WorkflowContext, order_id: str):
            event = await wait_event(ctx, event_type="payment.completed")
            return {"order_id": order_id, "payment": event.data}

        # Start one workflow instance
        engine = ReplayEngine(
            storage=sqlite_storage_routing,
            service_name="test-service",
            worker_id="test-worker",
        )

        instance_a = await engine.start_workflow(
            workflow_func=payment_workflow,
            workflow_name="payment_workflow",
            input_data={"order_id": "ORDER-001"},
        )

        # Verify it's waiting for 'payment.completed'
        inst_a = await sqlite_storage_routing.get_instance(instance_a)
        assert inst_a["status"] == "waiting_for_message"

        # Create mock CloudEvent with eddainstanceid but WRONG event_type
        mock_event = MagicMock()
        mock_event.__getitem__ = lambda self, key: {
            "type": "order.shipped",  # Different event type
            "source": "order-service",
            "id": "event-004",
        }.get(key)
        mock_event.get = lambda key, default=None: {
            "time": "2025-01-01T00:00:00Z",
            "datacontenttype": "application/json",
            "subject": None,
        }.get(key, default)
        mock_event.get_data = lambda: {"tracking_number": "TRK-001"}
        mock_event.get_attributes = lambda: {
            "type": "order.shipped",
            "source": "order-service",
            "id": "event-004",
            "specversion": "1.0",
            "eddainstanceid": instance_a,  # Correct instance but wrong event type
        }

        # Create EddaApp and call the delivery method directly
        app = EddaApp.__new__(EddaApp)
        app.storage = sqlite_storage_routing
        app.worker_id = "test-worker"

        # Deliver event - should NOT deliver (no subscription for order.shipped)
        await app._deliver_event_to_waiting_workflows(mock_event)

        # Verify instance_a is still waiting
        inst_a = await sqlite_storage_routing.get_instance(instance_a)
        assert inst_a["status"] == "waiting_for_message"

    async def test_cloudevents_metadata_preserved_in_point_to_point(self, sqlite_storage_routing):
        """Test that CloudEvents metadata is preserved in Point-to-Point delivery."""

        @workflow
        async def payment_workflow(ctx: WorkflowContext, order_id: str):
            event = await wait_event(ctx, event_type="payment.completed")
            return {"order_id": order_id, "payment": event.data}

        # Start workflow
        engine = ReplayEngine(
            storage=sqlite_storage_routing,
            service_name="test-service",
            worker_id="test-worker",
        )

        instance_id = await engine.start_workflow(
            workflow_func=payment_workflow,
            workflow_name="payment_workflow",
            input_data={"order_id": "ORDER-001"},
        )

        # Create mock CloudEvent with all metadata
        mock_event = MagicMock()
        mock_event.__getitem__ = lambda self, key: {
            "type": "payment.completed",
            "source": "payment-gateway",
            "id": "event-005",
        }.get(key)
        mock_event.get = lambda key, default=None: {
            "time": "2025-01-01T12:00:00Z",
            "datacontenttype": "application/json",
            "subject": "order/ORDER-001",
        }.get(key, default)
        mock_event.get_data = lambda: {"amount": 100, "transaction_id": "TXN-005"}
        mock_event.get_attributes = lambda: {
            "type": "payment.completed",
            "source": "payment-gateway",
            "id": "event-005",
            "specversion": "1.0",
            "eddainstanceid": instance_id,
            "customext": "custom-value",  # Custom extension
        }

        # Create EddaApp and call the delivery method directly
        app = EddaApp.__new__(EddaApp)
        app.storage = sqlite_storage_routing
        app.worker_id = "test-worker"

        # Deliver event
        await app._deliver_event_to_waiting_workflows(mock_event)

        # Verify message was delivered with metadata
        history = await sqlite_storage_routing.get_history(instance_id)
        message_received = [h for h in history if h["event_type"] == "ChannelMessageReceived"]
        assert len(message_received) == 1

        # Check metadata is preserved
        event_data = message_received[0]["event_data"]
        metadata = event_data.get("metadata", {})
        assert metadata.get("ce_type") == "payment.completed"
        assert metadata.get("ce_source") == "payment-gateway"
        assert metadata.get("ce_id") == "event-005"
        assert metadata.get("ce_time") == "2025-01-01T12:00:00Z"
        assert metadata.get("ce_subject") == "order/ORDER-001"
        # Extensions should include both eddainstanceid and customext
        extensions = metadata.get("ce_extensions", {})
        assert extensions.get("eddainstanceid") == instance_id
        assert extensions.get("customext") == "custom-value"
