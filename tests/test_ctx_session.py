"""
Tests for ctx.session property (Edda-managed session access).
"""

import pytest
from sqlalchemy import Column, Float, String, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import declarative_base

from edda import EddaApp, activity, workflow
from edda.context import WorkflowContext
from edda.exceptions import TerminalError
from edda.outbox.transactional import send_event_transactional

# Test ORM model
Base = declarative_base()


class Order(Base):  # type: ignore[misc, valid-type]
    """Test Order model."""

    __tablename__ = "orders"

    order_id = Column(String, primary_key=True)
    amount = Column(Float, nullable=False)
    status = Column(String, nullable=False, default="pending")


@pytest.mark.asyncio
async def test_ctx_session_inside_activity():
    """Test ctx.session access inside @activity."""
    # Create Edda app
    app = EddaApp(service_name="test-service", db_url="sqlite+aiosqlite:///:memory:")
    await app.initialize()

    # Create test table in the same database (use app's engine)
    async with app.storage.engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    @activity
    async def create_order_activity(ctx: WorkflowContext, order_id: str, amount: float):
        """Activity that uses ctx.session."""
        # Access Edda-managed session
        session = ctx.session

        # Verify session is an AsyncSession
        assert isinstance(session, AsyncSession)

        # Add order to database (same transaction as Edda)
        order = Order(order_id=order_id, amount=amount, status="created")
        session.add(order)

        # Send event (same transaction)
        await send_event_transactional(
            ctx, "order.created", "order-service", {"order_id": order_id, "amount": amount}
        )

        return {"order_id": order_id, "status": "created"}

    @workflow
    async def create_order_workflow(ctx: WorkflowContext, order_id: str, amount: float):
        """Simple workflow that creates an order."""
        result = await create_order_activity(ctx, order_id, amount, activity_id="create_order:1")
        return result

    # Execute workflow
    instance_id = await create_order_workflow.start(order_id="ORD-001", amount=100.0)

    # Verify workflow completed
    instance = await app.storage.get_instance(instance_id)
    assert instance["status"] == "completed"

    # Verify order was created (atomic with workflow)
    async with AsyncSession(app.storage.engine) as session:
        result = await session.execute(select(Order).where(Order.order_id == "ORD-001"))
        order = result.scalar_one_or_none()
        assert order is not None
        assert order.amount == 100.0
        assert order.status == "created"

    # Verify event was sent (atomic with workflow)
    outbox_events = await app.storage.get_pending_outbox_events(limit=10)
    assert len(outbox_events) == 1
    assert outbox_events[0]["event_type"] == "order.created"

    await app.shutdown()


@pytest.mark.asyncio
async def test_ctx_session_outside_transaction_raises_error():
    """Test ctx.session raises error when accessed outside transaction."""
    db_url = "sqlite+aiosqlite:///:memory:"
    app = EddaApp(service_name="test-service", db_url=db_url)
    await app.storage.initialize()

    # Create a dummy context (not in transaction)
    ctx = WorkflowContext(
        instance_id="test-001",
        workflow_name="test_workflow",
        storage=app.storage,
        worker_id="worker-1",
        is_replaying=False,
    )

    # Accessing ctx.session outside transaction should raise error
    with pytest.raises(RuntimeError, match="ctx.session must be accessed inside a transaction"):
        _ = ctx.session

    await app.shutdown()


@pytest.mark.asyncio
async def test_ctx_session_with_manual_transaction():
    """Test ctx.session with manual transaction control."""
    app = EddaApp(service_name="test-service", db_url="sqlite+aiosqlite:///:memory:")
    await app.initialize()

    # Create test table (use app's engine)
    async with app.storage.engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    @activity
    async def manual_transaction_activity(ctx: WorkflowContext, order_id: str, amount: float):
        """Activity with manual transaction control (nested transaction)."""
        # Activity already has a transaction, so this creates a nested transaction
        async with ctx.transaction():
            # ctx.session is available (nested transaction using savepoint)
            session = ctx.session

            # Add order
            order = Order(order_id=order_id, amount=amount, status="manual")
            session.add(order)

            # Send event
            await send_event_transactional(
                ctx, "order.created", "order-service", {"order_id": order_id}
            )

        return {"order_id": order_id}

    @workflow
    async def manual_transaction_workflow(ctx: WorkflowContext, order_id: str, amount: float):
        """Workflow with manual transaction."""
        result = await manual_transaction_activity(ctx, order_id, amount, activity_id="manual_tx:1")
        return result

    # Execute workflow
    instance_id = await manual_transaction_workflow.start(order_id="ORD-002", amount=200.0)

    # Verify workflow completed
    instance = await app.storage.get_instance(instance_id)
    assert instance["status"] == "completed"

    # Verify order was created
    async with AsyncSession(app.storage.engine) as session:
        result = await session.execute(select(Order).where(Order.order_id == "ORD-002"))
        order = result.scalar_one_or_none()
        assert order is not None
        assert order.amount == 200.0
        assert order.status == "manual"

    await app.shutdown()


@pytest.mark.asyncio
async def test_ctx_session_rollback_on_error():
    """Test ctx.session rolls back on error."""
    app = EddaApp(service_name="test-service", db_url="sqlite+aiosqlite:///:memory:")
    await app.initialize()

    # Create test table (use app's engine)
    async with app.storage.engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    @activity
    async def failing_activity(ctx: WorkflowContext, order_id: str, amount: float):
        """Activity that fails after DB operation."""
        session = ctx.session

        # Add order
        order = Order(order_id=order_id, amount=amount, status="failing")
        session.add(order)

        # Simulate error (TerminalError is never retried)
        raise TerminalError("Intentional failure")

    @workflow
    async def failing_workflow(ctx: WorkflowContext, order_id: str, amount: float):
        """Workflow that fails."""
        result = await failing_activity(ctx, order_id, amount, activity_id="failing:1")
        return result

    # Execute workflow (should fail)
    with pytest.raises(TerminalError, match="Intentional failure"):
        await failing_workflow.start(order_id="ORD-003", amount=300.0)

    # Verify order was NOT created (transaction rolled back)
    async with AsyncSession(app.storage.engine) as session:
        result = await session.execute(select(Order).where(Order.order_id == "ORD-003"))
        order = result.scalar_one_or_none()
        assert order is None  # Should not exist due to rollback

    await app.shutdown()


@pytest.mark.asyncio
async def test_ctx_session_multiple_operations():
    """Test ctx.session with multiple database operations."""
    app = EddaApp(service_name="test-service", db_url="sqlite+aiosqlite:///:memory:")
    await app.initialize()

    # Create test table (use app's engine)
    async with app.storage.engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    @activity
    async def multi_operation_activity(ctx: WorkflowContext, order_ids: list[str]):
        """Activity with multiple DB operations."""
        session = ctx.session

        # Add multiple orders
        for i, order_id in enumerate(order_ids):
            order = Order(order_id=order_id, amount=float(i + 1) * 100.0, status="batch")
            session.add(order)

        return {"count": len(order_ids)}

    @workflow
    async def multi_operation_workflow(ctx: WorkflowContext, order_ids: list[str]):
        """Workflow with multiple operations."""
        result = await multi_operation_activity(ctx, order_ids, activity_id="multi_op:1")
        return result

    # Execute workflow
    instance_id = await multi_operation_workflow.start(order_ids=["ORD-004", "ORD-005", "ORD-006"])

    # Verify workflow completed
    instance = await app.storage.get_instance(instance_id)
    assert instance["status"] == "completed"

    # Verify all orders were created atomically
    async with AsyncSession(app.storage.engine) as session:
        result = await session.execute(select(Order).where(Order.status == "batch"))
        orders = result.scalars().all()
        assert len(orders) == 3
        order_ids_in_db = {o.order_id for o in orders}
        assert order_ids_in_db == {"ORD-004", "ORD-005", "ORD-006"}

    await app.shutdown()
