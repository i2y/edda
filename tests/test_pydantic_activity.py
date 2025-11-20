"""
Tests for Pydantic integration in Activity functions.

Tests cover:
- Pydantic model parameters in activities
- Pydantic model return values
- Type restoration during replay
- Nested Pydantic models in activities
- Mixed Pydantic and primitive parameters
"""

from datetime import datetime

import pytest
from pydantic import BaseModel, Field

from edda import workflow
from edda.activity import activity
from edda.context import WorkflowContext
from edda.replay import ReplayEngine
from edda.workflow import set_replay_engine


# Test Pydantic models
class InventoryItem(BaseModel):
    """Inventory item model for testing."""

    item_id: str = Field(..., pattern=r"^ITEM-\d+$")
    name: str
    quantity: int = Field(..., ge=0)
    price: float = Field(..., ge=0.0)


class InventoryReservation(BaseModel):
    """Inventory reservation result model."""

    reservation_id: str
    item_id: str
    quantity_reserved: int
    status: str


class PaymentInfo(BaseModel):
    """Payment information model."""

    card_number: str = Field(..., pattern=r"^\d{16}$")
    card_holder: str
    amount: float = Field(..., ge=0.01)


class PaymentResult(BaseModel):
    """Payment result model."""

    transaction_id: str
    amount: float
    status: str
    timestamp: datetime


@pytest.mark.asyncio
class TestPydanticActivityParameters:
    """Test suite for Pydantic model parameters in Activities."""

    async def test_activity_with_pydantic_parameter(self, sqlite_storage):
        """Test Activity with a simple Pydantic model parameter."""

        @activity
        async def check_inventory(ctx: WorkflowContext, item: InventoryItem) -> dict:
            """Activity that accepts a Pydantic model."""
            return {
                "item_id": item.item_id,
                "available": item.quantity > 0,
                "price": item.price,
            }

        @workflow
        async def inventory_workflow(ctx: WorkflowContext, item: InventoryItem) -> dict:
            """Workflow that calls activity with Pydantic model."""
            result = await check_inventory(ctx, item)
            return result

        engine = ReplayEngine(sqlite_storage, "test-service", "worker-1")
        set_replay_engine(engine)

        item = InventoryItem(item_id="ITEM-001", name="Widget", quantity=10, price=9.99)
        instance_id = await inventory_workflow.start(item=item)

        instance = await sqlite_storage.get_instance(instance_id)
        assert instance["status"] == "completed"
        result = instance["output_data"]["result"]
        assert result["item_id"] == "ITEM-001"
        assert result["available"] is True
        assert result["price"] == 9.99

    async def test_activity_with_nested_pydantic_models(self, sqlite_storage):
        """Test Activity with nested Pydantic models."""

        @activity
        async def process_payment(ctx: WorkflowContext, payment_info: PaymentInfo) -> PaymentResult:
            """Activity that returns a Pydantic model."""
            return PaymentResult(
                transaction_id=f"TXN-{payment_info.card_number[-4:]}",
                amount=payment_info.amount,
                status="completed",
                timestamp=datetime(2025, 1, 1, 12, 0, 0),
            )

        @workflow
        async def payment_workflow(ctx: WorkflowContext, payment_info: PaymentInfo) -> dict:
            """Workflow that calls activity returning Pydantic model."""
            result = await process_payment(ctx, payment_info)
            # Result should be a Pydantic model
            assert isinstance(result, PaymentResult)
            return {"transaction_id": result.transaction_id, "status": result.status}

        engine = ReplayEngine(sqlite_storage, "test-service", "worker-1")
        set_replay_engine(engine)

        payment = PaymentInfo(card_number="1234567890123456", card_holder="Alice", amount=99.99)
        instance_id = await payment_workflow.start(payment_info=payment)

        instance = await sqlite_storage.get_instance(instance_id)
        assert instance["status"] == "completed"
        result = instance["output_data"]["result"]
        assert result["transaction_id"] == "TXN-3456"
        assert result["status"] == "completed"


@pytest.mark.asyncio
class TestPydanticActivityReturnValues:
    """Test suite for Pydantic model return values from Activities."""

    async def test_activity_returning_pydantic_model(self, sqlite_storage):
        """Test Activity that returns a Pydantic model."""

        @activity
        async def reserve_item(
            ctx: WorkflowContext, item_id: str, quantity: int
        ) -> InventoryReservation:
            """Activity that returns a Pydantic model."""
            return InventoryReservation(
                reservation_id=f"RES-{item_id}",
                item_id=item_id,
                quantity_reserved=quantity,
                status="reserved",
            )

        @workflow
        async def reservation_workflow(ctx: WorkflowContext, item_id: str, quantity: int) -> dict:
            """Workflow that calls activity returning Pydantic model."""
            reservation = await reserve_item(ctx, item_id, quantity)
            # reservation should be a Pydantic model
            assert isinstance(reservation, InventoryReservation)
            return {
                "reservation_id": reservation.reservation_id,
                "status": reservation.status,
            }

        engine = ReplayEngine(sqlite_storage, "test-service", "worker-1")
        set_replay_engine(engine)

        instance_id = await reservation_workflow.start(item_id="ITEM-123", quantity=5)

        instance = await sqlite_storage.get_instance(instance_id)
        assert instance["status"] == "completed"
        result = instance["output_data"]["result"]
        assert result["reservation_id"] == "RES-ITEM-123"
        assert result["status"] == "reserved"


@pytest.mark.asyncio
class TestPydanticReplayInActivities:
    """Test suite for Pydantic model restoration during Activity replay."""

    async def test_activity_replay_restores_pydantic_models(self, sqlite_storage):
        """Test that replay restores Pydantic models in activities."""
        execution_log = []

        @activity
        async def validate_item(ctx: WorkflowContext, item: InventoryItem) -> InventoryReservation:
            """Activity that logs execution for replay testing."""
            execution_log.append(f"Item: {item.item_id}, type: {type(item).__name__}")
            # Verify item is a Pydantic model instance
            assert isinstance(item, InventoryItem)
            return InventoryReservation(
                reservation_id=f"RES-{item.item_id}",
                item_id=item.item_id,
                quantity_reserved=item.quantity,
                status="validated",
            )

        @workflow
        async def validation_workflow(ctx: WorkflowContext, item: InventoryItem) -> dict:
            """Workflow for replay testing."""
            reservation = await validate_item(ctx, item)
            return {"reservation_id": reservation.reservation_id}

        engine = ReplayEngine(sqlite_storage, "test-service", "worker-1")
        set_replay_engine(engine)

        item = InventoryItem(item_id="ITEM-999", name="Test", quantity=1, price=1.0)
        await validation_workflow.start(item=item)

        # Verify execution
        assert len(execution_log) == 1
        assert "InventoryItem" in execution_log[0]


@pytest.mark.asyncio
class TestMixedParametersInActivities:
    """Test suite for Activities with mixed Pydantic and non-Pydantic parameters."""

    async def test_activity_with_mixed_parameters(self, sqlite_storage):
        """Test Activity with both Pydantic and primitive parameters."""

        @activity
        async def create_reservation(
            ctx: WorkflowContext, item: InventoryItem, customer_id: str, notes: str | None = None
        ) -> InventoryReservation:
            """Activity with mixed parameters."""
            reservation_id = f"RES-{customer_id}-{item.item_id}"
            return InventoryReservation(
                reservation_id=reservation_id,
                item_id=item.item_id,
                quantity_reserved=item.quantity,
                status="pending",
            )

        @workflow
        async def mixed_workflow(
            ctx: WorkflowContext, item: InventoryItem, customer_id: str
        ) -> dict:
            """Workflow with mixed parameters."""
            reservation = await create_reservation(ctx, item, customer_id, notes="Urgent")
            return {"reservation_id": reservation.reservation_id}

        engine = ReplayEngine(sqlite_storage, "test-service", "worker-1")
        set_replay_engine(engine)

        item = InventoryItem(item_id="ITEM-555", name="Gadget", quantity=3, price=29.99)
        instance_id = await mixed_workflow.start(item=item, customer_id="CUST-789")

        instance = await sqlite_storage.get_instance(instance_id)
        assert instance["status"] == "completed"
        result = instance["output_data"]["result"]
        assert result["reservation_id"] == "RES-CUST-789-ITEM-555"

    async def test_activity_with_dict_and_pydantic(self, sqlite_storage):
        """Test Activity with both dict and Pydantic parameters."""

        @activity
        async def process_order(ctx: WorkflowContext, item: InventoryItem, metadata: dict) -> dict:
            """Activity with dict and Pydantic parameters."""
            return {
                "item_id": item.item_id,
                "metadata_keys": list(metadata.keys()),
                "total_price": item.price * item.quantity,
            }

        @workflow
        async def order_workflow(ctx: WorkflowContext, item: InventoryItem, metadata: dict) -> dict:
            """Workflow with mixed parameters."""
            result = await process_order(ctx, item, metadata)
            return result

        engine = ReplayEngine(sqlite_storage, "test-service", "worker-1")
        set_replay_engine(engine)

        item = InventoryItem(item_id="ITEM-777", name="Tool", quantity=2, price=15.50)
        metadata = {"priority": "high", "source": "mobile"}
        instance_id = await order_workflow.start(item=item, metadata=metadata)

        instance = await sqlite_storage.get_instance(instance_id)
        assert instance["status"] == "completed"
        result = instance["output_data"]["result"]
        assert result["item_id"] == "ITEM-777"
        assert "priority" in result["metadata_keys"]
        assert result["total_price"] == 31.0
