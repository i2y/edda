"""
Tests for Pydantic integration in event handling.

Tests cover:
- wait_event with Pydantic model parameter (replay)
- send_event_transactional with Pydantic models
- Type restoration from cached event data
"""

from datetime import datetime

import pytest
import pytest_asyncio
from pydantic import BaseModel, Field

from edda import workflow
from edda.activity import activity
from edda.channels import wait_event
from edda.context import WorkflowContext
from edda.outbox.transactional import send_event_transactional
from edda.replay import ReplayEngine
from edda.workflow import set_replay_engine


# Test Pydantic models
class PaymentCompleted(BaseModel):
    """Payment completed event model."""

    order_id: str = Field(..., pattern=r"^ORD-\d+$")
    amount: float = Field(..., ge=0.01)
    transaction_id: str
    timestamp: datetime


class InventoryReserved(BaseModel):
    """Inventory reserved event model."""

    order_id: str
    reservation_id: str
    quantity: int = Field(..., ge=1)


@pytest.mark.asyncio
class TestWaitEventPydanticReplay:
    """Test suite for wait_event with Pydantic model parameter during replay."""

    @pytest_asyncio.fixture
    async def workflow_instance(self, sqlite_storage, create_test_instance):
        """Create a workflow instance for testing."""
        instance_id = "test-pydantic-event-001"
        await create_test_instance(
            instance_id=instance_id,
            workflow_name="test_workflow",
            owner_service="test-service",
            input_data={"test": "data"},
        )
        await sqlite_storage.update_instance_status(instance_id, "running")
        return instance_id

    async def test_wait_event_converts_to_pydantic_model_during_replay(
        self, sqlite_storage, workflow_instance
    ):
        """Test that wait_event converts cached dict to Pydantic model during replay.

        Note: CloudEvents (wait_event) internally uses Message Passing (wait_message),
        so the history format uses ChannelMessageReceived with CloudEvents metadata.
        """
        # Add message data to history (using ChannelMessageReceived format with CloudEvents metadata)
        event_data = {
            "data": {
                "order_id": "ORD-123",
                "amount": 99.99,
                "transaction_id": "TXN-456",
                "timestamp": "2025-01-01T12:00:00",
            },
            "channel": "payment.completed",
            "id": "msg-001",
            "metadata": {
                "ce_type": "payment.completed",
                "ce_source": "payment-service",
                "ce_id": "evt-001",
                "ce_time": "2025-01-01T12:00:00Z",
            },
        }

        await sqlite_storage.append_history(
            workflow_instance,
            activity_id="receive_payment.completed:1",
            event_type="ChannelMessageReceived",
            event_data=event_data,
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

        # Call wait_event with Pydantic model
        received_event = await wait_event(
            ctx, event_type="payment.completed", model=PaymentCompleted
        )

        # Verify data was converted to Pydantic model
        assert isinstance(received_event.data, PaymentCompleted)
        assert received_event.data.order_id == "ORD-123"
        assert received_event.data.amount == 99.99
        assert received_event.data.transaction_id == "TXN-456"

        # Verify CloudEvents metadata is preserved
        assert received_event.type == "payment.completed"
        assert received_event.source == "payment-service"
        assert received_event.id == "evt-001"

    async def test_wait_event_returns_dict_when_no_model_during_replay(
        self, sqlite_storage, workflow_instance
    ):
        """Test that wait_event returns dict when no model parameter during replay."""
        # Add message data to history (using ChannelMessageReceived format)
        event_data = {
            "data": {"order_id": "ORD-456", "status": "completed"},
            "channel": "order.created",
            "id": "msg-002",
            "metadata": {
                "ce_type": "order.created",
                "ce_source": "order-service",
                "ce_id": "evt-002",
            },
        }

        await sqlite_storage.append_history(
            workflow_instance,
            activity_id="receive_order.created:1",
            event_type="ChannelMessageReceived",
            event_data=event_data,
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

        # Call wait_event without model parameter
        received_event = await wait_event(ctx, event_type="order.created")

        # Verify data is still a dict
        assert isinstance(received_event.data, dict)
        assert received_event.data["order_id"] == "ORD-456"
        assert received_event.data["status"] == "completed"


@pytest.mark.asyncio
class TestWaitEventWithNestedPydanticModel:
    """Test suite for wait_event with nested/complex Pydantic models."""

    @pytest_asyncio.fixture
    async def workflow_instance(self, sqlite_storage, create_test_instance):
        """Create a workflow instance for testing."""
        instance_id = "test-pydantic-event-002"
        await create_test_instance(
            instance_id=instance_id,
            workflow_name="test_workflow",
            owner_service="test-service",
            input_data={"test": "data"},
        )
        await sqlite_storage.update_instance_status(instance_id, "running")
        return instance_id

    async def test_wait_event_with_datetime_fields(self, sqlite_storage, workflow_instance):
        """Test that wait_event correctly converts datetime fields in Pydantic models."""
        # Add message data with ISO 8601 timestamp (using ChannelMessageReceived format)
        event_data = {
            "data": {
                "order_id": "ORD-789",
                "amount": 249.99,
                "transaction_id": "TXN-789",
                "timestamp": "2025-01-15T10:30:00",
            },
            "channel": "payment.completed",
            "id": "msg-003",
            "metadata": {
                "ce_type": "payment.completed",
                "ce_source": "payment-service",
                "ce_id": "evt-003",
            },
        }

        await sqlite_storage.append_history(
            workflow_instance,
            activity_id="receive_payment.completed:1",
            event_type="ChannelMessageReceived",
            event_data=event_data,
        )

        # Create replay context
        ctx = WorkflowContext(
            instance_id=workflow_instance,
            workflow_name="test_workflow",
            storage=sqlite_storage,
            worker_id="worker-1",
            is_replaying=True,
        )

        await ctx._load_history()

        # Call wait_event with Pydantic model containing datetime
        received_event = await wait_event(
            ctx, event_type="payment.completed", model=PaymentCompleted
        )

        # Verify Pydantic model with datetime field
        assert isinstance(received_event.data, PaymentCompleted)
        assert received_event.data.order_id == "ORD-789"
        assert received_event.data.amount == 249.99
        assert isinstance(received_event.data.timestamp, datetime)
        assert received_event.data.timestamp.year == 2025


@pytest.mark.asyncio
class TestSendEventTransactionalWithPydantic:
    """Test suite for send_event_transactional with Pydantic models."""

    async def test_send_event_transactional_with_pydantic(self, sqlite_storage):
        """Test send_event_transactional accepts and converts Pydantic models."""

        @activity
        async def reserve_inventory_typed(
            ctx: WorkflowContext, order_id: str, quantity: int
        ) -> dict:
            """Reserve inventory and send event with Pydantic model."""
            reservation_id = f"RES-{order_id}"

            # Send event with Pydantic model
            event = InventoryReserved(
                order_id=order_id,
                reservation_id=reservation_id,
                quantity=quantity,
            )

            event_id = await send_event_transactional(
                ctx,
                event_type="inventory.reserved",
                event_source="order-service",
                event_data=event,
            )

            return {"reservation_id": reservation_id, "event_id": event_id}

        @workflow
        async def order_workflow(ctx: WorkflowContext, order_id: str, quantity: int) -> dict:
            """Workflow that reserves inventory with Pydantic event."""
            result = await reserve_inventory_typed(ctx, order_id, quantity)
            return result

        engine = ReplayEngine(sqlite_storage, "test-service", "worker-1")
        set_replay_engine(engine)

        instance_id = await order_workflow.start(order_id="ORD-333", quantity=5)

        # Verify workflow completed
        instance = await sqlite_storage.get_instance(instance_id)
        assert instance["status"] == "completed"
        result = instance["output_data"]["result"]
        assert result["reservation_id"] == "RES-ORD-333"

        # Verify outbox event was created (as JSON dict)
        outbox_events = await sqlite_storage.get_pending_outbox_events()
        assert len(outbox_events) > 0

        # Find our event
        our_event = next(
            (e for e in outbox_events if e["event_type"] == "inventory.reserved"), None
        )
        assert our_event is not None
        assert isinstance(our_event["event_data"], dict)
        assert our_event["event_data"]["order_id"] == "ORD-333"
        assert our_event["event_data"]["quantity"] == 5

    async def test_send_event_transactional_with_dict(self, sqlite_storage):
        """Test send_event_transactional still accepts dict data."""

        @activity
        async def reserve_inventory_dict(ctx: WorkflowContext, order_id: str) -> dict:
            """Reserve inventory with dict event data."""
            event_data = {
                "order_id": order_id,
                "reservation_id": f"RES-{order_id}",
            }

            event_id = await send_event_transactional(
                ctx,
                event_type="inventory.reserved",
                event_source="order-service",
                event_data=event_data,
            )

            return {"event_id": event_id}

        @workflow
        async def order_workflow_dict(ctx: WorkflowContext, order_id: str) -> dict:
            """Workflow with dict event."""
            result = await reserve_inventory_dict(ctx, order_id)
            return result

        engine = ReplayEngine(sqlite_storage, "test-service", "worker-1")
        set_replay_engine(engine)

        instance_id = await order_workflow_dict.start(order_id="ORD-444")

        # Verify workflow completed
        instance = await sqlite_storage.get_instance(instance_id)
        assert instance["status"] == "completed"

        # Verify outbox event
        outbox_events = await sqlite_storage.get_pending_outbox_events()
        our_event = next((e for e in outbox_events if "ORD-444" in str(e["event_data"])), None)
        assert our_event is not None
        assert isinstance(our_event["event_data"], dict)
