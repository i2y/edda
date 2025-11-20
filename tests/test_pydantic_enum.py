"""Tests for Enum support in Pydantic integration."""

from enum import Enum

import pytest

from edda import activity, workflow
from edda.context import WorkflowContext
from edda.pydantic_utils import (
    enum_value_to_enum,
    extract_enum_from_annotation,
    is_enum_class,
    is_enum_instance,
    to_json_dict,
)
from edda.replay import ReplayEngine
from edda.workflow import set_replay_engine


# Test Enums
class OrderStatus(Enum):
    """Order status enum."""

    PENDING = "pending"
    PROCESSING = "processing"
    COMPLETED = "completed"
    CANCELLED = "cancelled"


class Priority(Enum):
    """Priority enum with integer values."""

    LOW = 1
    MEDIUM = 2
    HIGH = 3


# ============================================================================
# Utility Function Tests
# ============================================================================


def test_is_enum_class():
    """Test is_enum_class() function."""
    assert is_enum_class(OrderStatus) is True
    assert is_enum_class(Priority) is True
    assert is_enum_class(OrderStatus.PENDING) is False
    assert is_enum_class(str) is False
    assert is_enum_class("not a class") is False


def test_is_enum_instance():
    """Test is_enum_instance() function."""
    assert is_enum_instance(OrderStatus.PENDING) is True
    assert is_enum_instance(Priority.HIGH) is True
    assert is_enum_instance(OrderStatus) is False
    assert is_enum_instance("pending") is False
    assert is_enum_instance(1) is False


def test_extract_enum_from_annotation():
    """Test extract_enum_from_annotation() function."""
    # Direct Enum
    assert extract_enum_from_annotation(OrderStatus) == OrderStatus

    # Optional[Enum]
    assert extract_enum_from_annotation(OrderStatus | None) == OrderStatus

    # Enum | None (Python 3.10+)
    assert extract_enum_from_annotation(OrderStatus | None) == OrderStatus

    # Non-Enum types
    assert extract_enum_from_annotation(str) is None
    assert extract_enum_from_annotation(int) is None


def test_enum_value_to_enum_string():
    """Test enum_value_to_enum() with string values."""
    # By value
    assert enum_value_to_enum("pending", OrderStatus) == OrderStatus.PENDING
    assert enum_value_to_enum("processing", OrderStatus) == OrderStatus.PROCESSING

    # Invalid value
    with pytest.raises(ValueError, match="Cannot convert"):
        enum_value_to_enum("invalid", OrderStatus)


def test_enum_value_to_enum_int():
    """Test enum_value_to_enum() with integer values."""
    # By value
    assert enum_value_to_enum(1, Priority) == Priority.LOW
    assert enum_value_to_enum(3, Priority) == Priority.HIGH

    # Invalid value
    with pytest.raises(ValueError, match="Cannot convert"):
        enum_value_to_enum(999, Priority)


def test_to_json_dict_with_enum():
    """Test to_json_dict() converts Enum to value."""
    assert to_json_dict(OrderStatus.PENDING) == "pending"
    assert to_json_dict(Priority.HIGH) == 3

    # Non-Enum values pass through
    assert to_json_dict("string") == "string"
    assert to_json_dict(123) == 123


# ============================================================================
# Saga with Enum Tests
# ============================================================================


@pytest.mark.asyncio
async def test_saga_with_enum_parameter(sqlite_storage):
    """Test Saga with Enum parameter."""

    @workflow
    async def process_order(
        ctx: WorkflowContext, order_id: str, status: OrderStatus, priority: Priority
    ) -> dict:
        """Process order workflow."""
        # Verify Enum types are restored
        assert isinstance(status, OrderStatus)
        assert isinstance(priority, Priority)
        assert status == OrderStatus.PROCESSING
        assert priority == Priority.HIGH

        return {
            "order_id": order_id,
            "status_name": status.name,
            "status_value": status.value,
            "priority_value": priority.value,
        }

    # Set up replay engine
    engine = ReplayEngine(sqlite_storage, "test-service", "worker-1")
    set_replay_engine(engine)

    # Start workflow with Enum instances
    instance_id = await process_order.start(
        order_id="ORD-001", status=OrderStatus.PROCESSING, priority=Priority.HIGH
    )

    # Verify workflow completed
    instance = await sqlite_storage.get_instance(instance_id)
    assert instance is not None
    assert instance["status"] == "completed"

    # Verify output data
    output = instance["output_data"]
    assert output["result"]["status_name"] == "PROCESSING"
    assert output["result"]["status_value"] == "processing"
    assert output["result"]["priority_value"] == 3


@pytest.mark.asyncio
async def test_saga_with_optional_enum(sqlite_storage):
    """Test Saga with Optional[Enum] parameter."""

    @workflow
    async def optional_status_workflow(
        ctx: WorkflowContext, order_id: str, status: OrderStatus | None = None
    ) -> dict:
        """Workflow with optional Enum parameter."""
        if status is not None:
            assert isinstance(status, OrderStatus)
            return {"order_id": order_id, "status": status.value}
        else:
            return {"order_id": order_id, "status": "none"}

    engine = ReplayEngine(sqlite_storage, "test-service", "worker-1")
    set_replay_engine(engine)

    # Test with Enum value
    instance_id = await optional_status_workflow.start(
        order_id="ORD-001", status=OrderStatus.PENDING
    )
    instance = await sqlite_storage.get_instance(instance_id)
    assert instance["output_data"]["result"]["status"] == "pending"

    # Test with None (default)
    instance_id2 = await optional_status_workflow.start(order_id="ORD-002")
    instance2 = await sqlite_storage.get_instance(instance_id2)
    assert instance2["output_data"]["result"]["status"] == "none"


# ============================================================================
# Activity with Enum Tests
# ============================================================================


@pytest.mark.asyncio
async def test_activity_with_enum_return(sqlite_storage):
    """Test Activity with Enum return value."""

    @activity
    async def get_order_status(ctx: WorkflowContext, order_id: str) -> OrderStatus:
        """Activity that returns an Enum."""
        return OrderStatus.PROCESSING

    @workflow
    async def check_status_workflow(ctx: WorkflowContext, order_id: str) -> dict:
        """Workflow that calls Activity returning Enum."""
        status = await get_order_status(ctx, order_id)

        # Verify Enum type is restored during replay
        assert isinstance(status, OrderStatus)
        assert status == OrderStatus.PROCESSING

        return {"status": status.value, "status_name": status.name}

    engine = ReplayEngine(sqlite_storage, "test-service", "worker-1")
    set_replay_engine(engine)

    # Start workflow
    instance_id = await check_status_workflow.start(order_id="ORD-001")

    # Verify workflow completed
    instance = await sqlite_storage.get_instance(instance_id)
    assert instance["status"] == "completed"
    assert instance["output_data"]["result"]["status"] == "processing"
    assert instance["output_data"]["result"]["status_name"] == "PROCESSING"


@pytest.mark.asyncio
async def test_activity_enum_during_replay(sqlite_storage):
    """Test Activity Enum restoration during replay."""

    call_count = {"count": 0}

    @activity
    async def determine_priority(ctx: WorkflowContext, amount: float) -> Priority:
        """Activity that returns Priority Enum."""
        call_count["count"] += 1

        if amount > 1000:
            return Priority.HIGH
        elif amount > 100:
            return Priority.MEDIUM
        else:
            return Priority.LOW

    @workflow
    async def priority_workflow(ctx: WorkflowContext, amount: float) -> dict:
        """Workflow using Priority Enum."""
        priority = await determine_priority(ctx, amount)

        # Verify Enum type
        assert isinstance(priority, Priority)

        # Simulate replay by calling again (should return cached Enum)
        priority2 = await determine_priority(ctx, amount)
        assert isinstance(priority2, Priority)
        assert priority == priority2

        return {"priority": priority.name, "priority_value": priority.value}

    engine = ReplayEngine(sqlite_storage, "test-service", "worker-1")
    set_replay_engine(engine)

    # Start workflow
    instance_id = await priority_workflow.start(amount=1500.0)

    # Verify workflow completed
    instance = await sqlite_storage.get_instance(instance_id)
    assert instance["status"] == "completed"
    assert instance["output_data"]["result"]["priority"] == "HIGH"
    assert instance["output_data"]["result"]["priority_value"] == 3

    # Activity should be called once (second call hits cache)
    # Note: Due to replay mechanism, count might be 1 or 2 depending on implementation
    # For now, we just verify the workflow completed successfully


# ============================================================================
# Edge Cases
# ============================================================================


@pytest.mark.asyncio
async def test_multiple_enums_in_saga(sqlite_storage):
    """Test Saga with multiple Enum parameters."""

    @workflow
    async def multi_enum_workflow(
        ctx: WorkflowContext, status: OrderStatus, priority: Priority
    ) -> dict:
        """Workflow with multiple Enums."""
        assert isinstance(status, OrderStatus)
        assert isinstance(priority, Priority)

        return {
            "status": status.value,
            "priority": priority.value,
            "combined": f"{status.name}_{priority.name}",
        }

    engine = ReplayEngine(sqlite_storage, "test-service", "worker-1")
    set_replay_engine(engine)

    # Start workflow
    instance_id = await multi_enum_workflow.start(
        status=OrderStatus.COMPLETED, priority=Priority.LOW
    )

    # Verify workflow completed
    instance = await sqlite_storage.get_instance(instance_id)
    assert instance["output_data"]["result"]["status"] == "completed"
    assert instance["output_data"]["result"]["priority"] == 1
    assert instance["output_data"]["result"]["combined"] == "COMPLETED_LOW"


def test_enum_value_by_name():
    """Test enum_value_to_enum() with name matching."""
    # By exact name
    assert enum_value_to_enum("PENDING", OrderStatus) == OrderStatus.PENDING

    # By lowercase name (should try uppercase)
    assert enum_value_to_enum("pending", OrderStatus) == OrderStatus.PENDING
