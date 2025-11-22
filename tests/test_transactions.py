"""
Tests for transaction management functionality.

This module tests the transactional outbox pattern implementation,
ensuring atomicity between business logic, history recording, and event sending.
"""

import pytest
from sqlalchemy.ext.asyncio import create_async_engine

from edda.activity import activity
from edda.context import WorkflowContext
from edda.exceptions import TerminalError
from edda.outbox.transactional import send_event_transactional
from edda.storage.sqlalchemy_storage import SQLAlchemyStorage


@pytest.fixture
async def storage():
    """Create an in-memory SQLite storage for testing."""
    storage = SQLAlchemyStorage(create_async_engine("sqlite+aiosqlite:///:memory:", echo=False))
    await storage.initialize()
    yield storage
    await storage.close()


@pytest.fixture
async def context(storage):
    """Create a workflow context for testing."""
    instance_id = "test-instance-1"
    workflow_name = "test_workflow"
    worker_id = "test-worker-1"
    source_hash = "test-hash"

    # Create workflow definition first (required for foreign key)
    await storage.upsert_workflow_definition(
        workflow_name=workflow_name,
        source_hash=source_hash,
        source_code="def test(): pass",
    )

    # Create a workflow instance
    await storage.create_instance(
        instance_id=instance_id,
        workflow_name=workflow_name,
        source_hash=source_hash,
        owner_service="test-service",
        input_data={"test": "data"},
    )

    ctx = WorkflowContext(
        instance_id=instance_id,
        workflow_name=workflow_name,
        storage=storage,
        worker_id=worker_id,
        is_replaying=False,
    )

    return ctx


@pytest.mark.asyncio
async def test_transaction_commit(storage):
    """Test that transaction commits successfully."""
    assert not storage.in_transaction()

    # Create workflow definition first
    await storage.upsert_workflow_definition(
        workflow_name="test_workflow",
        source_hash="hash1",
        source_code="def test(): pass",
    )

    await storage.begin_transaction()
    assert storage.in_transaction()

    # Perform some operations
    await storage.create_instance(
        instance_id="txn-test-1",
        workflow_name="test_workflow",
        source_hash="hash1",
        owner_service="service1",
        input_data={"key": "value"},
    )

    await storage.commit_transaction()
    assert not storage.in_transaction()

    # Verify the operation was committed
    instance = await storage.get_instance("txn-test-1")
    assert instance is not None
    assert instance["workflow_name"] == "test_workflow"


@pytest.mark.asyncio
async def test_transaction_rollback(storage):
    """Test that transaction rolls back on error."""
    assert not storage.in_transaction()

    # Create workflow definition first
    await storage.upsert_workflow_definition(
        workflow_name="test_workflow",
        source_hash="hash2",
        source_code="def test(): pass",
    )

    await storage.begin_transaction()
    assert storage.in_transaction()

    # Perform some operations
    await storage.create_instance(
        instance_id="txn-test-2",
        workflow_name="test_workflow",
        source_hash="hash2",
        owner_service="service2",
        input_data={"key": "value"},
    )

    await storage.rollback_transaction()
    assert not storage.in_transaction()

    # Verify the operation was rolled back
    instance = await storage.get_instance("txn-test-2")
    assert instance is None


@pytest.mark.asyncio
async def test_nested_transactions_savepoint(storage):
    """Test nested transactions using savepoints."""
    assert not storage.in_transaction()

    # Create workflow definitions first
    await storage.upsert_workflow_definition(
        workflow_name="test_workflow",
        source_hash="hash1",
        source_code="def test(): pass",
    )
    await storage.upsert_workflow_definition(
        workflow_name="test_workflow",
        source_hash="hash2",
        source_code="def test2(): pass",
    )

    # Begin outer transaction
    await storage.begin_transaction()
    assert storage.in_transaction()

    await storage.create_instance(
        instance_id="nested-1",
        workflow_name="test_workflow",
        source_hash="hash1",
        owner_service="service1",
        input_data={"outer": True},
    )

    # Begin inner transaction (savepoint)
    await storage.begin_transaction()
    assert storage.in_transaction()

    await storage.create_instance(
        instance_id="nested-2",
        workflow_name="test_workflow",
        source_hash="hash2",
        owner_service="service2",
        input_data={"inner": True},
    )

    # Rollback inner transaction only
    await storage.rollback_transaction()
    assert storage.in_transaction()  # Still in outer transaction

    # Commit outer transaction
    await storage.commit_transaction()
    assert not storage.in_transaction()

    # Verify outer committed, inner rolled back
    outer_instance = await storage.get_instance("nested-1")
    inner_instance = await storage.get_instance("nested-2")
    assert outer_instance is not None
    assert inner_instance is None


@pytest.mark.asyncio
async def test_context_transaction(context):
    """Test WorkflowContext.transaction() context manager."""
    assert not context.storage.in_transaction()

    async with context.transaction():
        assert context.storage.in_transaction()

        await context.storage.append_history(
            instance_id=context.instance_id,
            activity_id="test_event:1",
            event_type="TestEvent",
            event_data={"test": "data"},
        )

    assert not context.storage.in_transaction()

    # Verify the history was committed
    history = await context.storage.get_history(context.instance_id)
    assert len(history) == 1
    assert history[0]["event_type"] == "TestEvent"


@pytest.mark.asyncio
async def test_context_transaction_rollback_on_error(context):
    """Test that context.transaction() rolls back on exception."""
    assert not context.storage.in_transaction()

    try:
        async with context.transaction():
            assert context.storage.in_transaction()

            await context.storage.append_history(
                instance_id=context.instance_id,
                activity_id="test_event:1",
                event_type="TestEvent",
                event_data={"test": "data"},
            )

            # Simulate an error (TerminalError is never retried)
            raise TerminalError("Test error")
    except TerminalError:
        pass

    assert not context.storage.in_transaction()

    # Verify the history was NOT committed
    history = await context.storage.get_history(context.instance_id)
    assert len(history) == 0


@pytest.mark.asyncio
async def test_activity_automatic_transaction(context):
    """Test that @activity wraps execution in a transaction by default."""

    @activity
    async def test_activity(ctx: WorkflowContext, value: int) -> dict:
        # Should be in a transaction
        assert ctx.in_transaction()
        return {"result": value * 2}

    assert not context.storage.in_transaction()

    result = await test_activity(context, 21)

    assert not context.storage.in_transaction()
    assert result == {"result": 42}

    # Verify history was recorded
    history = await context.storage.get_history(context.instance_id)
    assert len(history) == 1
    assert history[0]["event_type"] == "ActivityCompleted"


@pytest.mark.asyncio
async def test_activity_transaction_rollback(context):
    """Test that activity transaction rolls back on error."""

    @activity
    async def failing_activity(ctx: WorkflowContext, value: int) -> dict:
        assert ctx.in_transaction()

        # This should be rolled back
        await send_event_transactional(
            ctx,
            event_type="test.event",
            event_source="test",
            event_data={"value": value},
        )

        # Simulate an error (TerminalError is never retried)
        raise TerminalError("Activity failed")

    assert not context.storage.in_transaction()

    with pytest.raises(TerminalError, match="Activity failed"):
        await failing_activity(context, 123)

    assert not context.storage.in_transaction()

    # Verify NO outbox events were created (rolled back)
    outbox_events = await context.storage.get_pending_outbox_events()
    assert len(outbox_events) == 0


@pytest.mark.asyncio
async def test_send_event_transactional_warning(context, caplog):
    """Test that send_event_transactional() warns when not in transaction."""
    import logging

    caplog.set_level(logging.WARNING)

    assert not context.storage.in_transaction()

    # Should warn because not in transaction
    await send_event_transactional(
        context,
        event_type="test.event",
        event_source="test",
        event_data={"test": "data"},
    )

    # Check that warning was logged
    assert any(
        "send_event_transactional() called outside of a transaction" in record.message
        for record in caplog.records
    )


@pytest.mark.asyncio
async def test_atomic_history_and_outbox(context):
    """Test that history and outbox writes are atomic."""

    @activity
    async def test_activity(ctx: WorkflowContext, value: int) -> dict:
        # Both operations should be in the same transaction
        assert ctx.in_transaction()

        await send_event_transactional(
            ctx,
            event_type="test.event",
            event_source="test",
            event_data={"value": value},
        )

        return {"result": value}

    await test_activity(context, 42)

    # Verify both history and outbox were committed together
    history = await context.storage.get_history(context.instance_id)
    outbox = await context.storage.get_pending_outbox_events()

    assert len(history) == 1  # Activity completed event
    assert len(outbox) == 1  # Outbox event
    assert outbox[0]["event_type"] == "test.event"


@pytest.mark.asyncio
async def test_atomic_rollback(context):
    """Test that history and outbox rollback together on error."""

    @activity
    async def failing_activity(ctx: WorkflowContext, value: int) -> dict:
        assert ctx.in_transaction()

        await send_event_transactional(
            ctx,
            event_type="test.event",
            event_source="test",
            event_data={"value": value},
        )

        # Simulate error after sending event (TerminalError is never retried)
        raise TerminalError("Something went wrong")

    with pytest.raises(TerminalError, match="Something went wrong"):
        await failing_activity(context, 42)

    # Verify rollback behavior
    # With the new implementation:
    # - Failure history is recorded OUTSIDE the transaction for observability
    # - Outbox events are rolled back (they were inside the transaction)
    history = await context.storage.get_history(context.instance_id)
    outbox = await context.storage.get_pending_outbox_events()

    # History should contain the failure record (for observability)
    assert len(history) == 1
    assert history[0]["event_type"] == "ActivityFailed"

    # Outbox should be empty (rolled back with transaction)
    assert len(outbox) == 0
