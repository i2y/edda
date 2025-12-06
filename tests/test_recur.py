"""
Tests for the recur() functionality (Erlang-style tail recursion pattern).

Tests cover:
- RecurException class
- recur() method in WorkflowContext
- ReplayEngine handling of RecurException
- History archiving
- Continued workflow chain tracking
- Pydantic model and Enum serialization in recur kwargs
"""

from enum import Enum

import pytest
import pytest_asyncio
from pydantic import BaseModel
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from edda.activity import activity
from edda.context import WorkflowContext
from edda.replay import ReplayEngine
from edda.storage.sqlalchemy_storage import WorkflowHistoryArchive, WorkflowInstance
from edda.workflow import RecurException


class TestRecurException:
    """Test suite for RecurException class."""

    def test_recur_exception_init(self):
        """Test RecurException initialization with kwargs."""
        kwargs = {"count": 100, "name": "test"}
        exc = RecurException(kwargs=kwargs)

        assert exc.kwargs == kwargs
        assert str(exc) == "Workflow recur requested"

    def test_recur_exception_empty_kwargs(self):
        """Test RecurException with empty kwargs."""
        exc = RecurException(kwargs={})

        assert exc.kwargs == {}
        assert str(exc) == "Workflow recur requested"


@pytest.mark.asyncio
class TestRecurMethod:
    """Test suite for WorkflowContext.recur() method."""

    @pytest_asyncio.fixture
    async def context(self, sqlite_storage):
        """Create a WorkflowContext for testing."""
        # Create workflow definition
        await sqlite_storage.upsert_workflow_definition(
            workflow_name="test_recur_workflow",
            source_hash="test-hash",
            source_code="async def test_recur_workflow(ctx): pass",
        )

        # Create instance
        await sqlite_storage.create_instance(
            instance_id="test-recur-123",
            workflow_name="test_recur_workflow",
            source_hash="test-hash",
            owner_service="test-service",
            input_data={"initial": "data"},
        )

        return WorkflowContext(
            instance_id="test-recur-123",
            workflow_name="test_recur_workflow",
            storage=sqlite_storage,
            worker_id="worker-test",
        )

    async def test_recur_raises_exception(self, context):
        """Test that recur() raises RecurException with correct kwargs."""
        with pytest.raises(RecurException) as exc_info:
            await context.recur(count=100, name="test")

        assert exc_info.value.kwargs == {"count": 100, "name": "test"}

    async def test_recur_with_pydantic_model(self, context):
        """Test that recur() converts Pydantic models to JSON."""

        class OrderState(BaseModel):
            order_id: str
            processed_count: int

        state = OrderState(order_id="ORD-123", processed_count=50)

        with pytest.raises(RecurException) as exc_info:
            await context.recur(state=state)

        # Pydantic model should be converted to dict
        assert exc_info.value.kwargs == {"state": {"order_id": "ORD-123", "processed_count": 50}}

    async def test_recur_with_enum(self, context):
        """Test that recur() converts Enums to their values."""

        class Status(str, Enum):
            PENDING = "pending"
            COMPLETED = "completed"

        with pytest.raises(RecurException) as exc_info:
            await context.recur(status=Status.COMPLETED, count=10)

        # Enum should be converted to its value
        assert exc_info.value.kwargs == {"status": "completed", "count": 10}

    async def test_recur_with_nested_pydantic(self, context):
        """Test recur() with nested Pydantic models."""

        class Item(BaseModel):
            product_id: str
            quantity: int

        class OrderState(BaseModel):
            order_id: str
            items: list[Item]

        state = OrderState(
            order_id="ORD-456",
            items=[Item(product_id="PROD-1", quantity=2), Item(product_id="PROD-2", quantity=1)],
        )

        with pytest.raises(RecurException) as exc_info:
            await context.recur(state=state)

        expected = {
            "state": {
                "order_id": "ORD-456",
                "items": [
                    {"product_id": "PROD-1", "quantity": 2},
                    {"product_id": "PROD-2", "quantity": 1},
                ],
            }
        }
        assert exc_info.value.kwargs == expected


@pytest.mark.asyncio
class TestRecurInReplayEngine:
    """Test suite for ReplayEngine handling of RecurException."""

    @pytest.fixture
    def replay_engine(self, sqlite_storage):
        """Create a ReplayEngine instance for testing."""
        return ReplayEngine(
            storage=sqlite_storage,
            service_name="test-service",
            worker_id="worker-test-001",
        )

    async def test_recur_creates_new_instance(self, replay_engine, sqlite_storage):
        """Test that recur() creates a new workflow instance."""

        @activity
        async def process_item(ctx: WorkflowContext, item_id: str) -> dict:
            return {"item_id": item_id, "processed": True}

        async def recur_workflow(ctx: WorkflowContext, iteration: int = 0) -> dict:
            result = await process_item(ctx, f"item-{iteration}")

            # Recur after first iteration for testing
            if iteration == 0:
                await ctx.recur(iteration=iteration + 1)

            return result

        # Start workflow - it should recur and create a new instance
        new_instance_id = await replay_engine.start_workflow(
            workflow_name="recur_workflow",
            workflow_func=recur_workflow,
            input_data={"iteration": 0},
        )

        # The returned ID should be the new instance (after recur)
        assert new_instance_id is not None
        assert new_instance_id.startswith("recur_workflow-")

        # Get the new instance
        new_instance = await sqlite_storage.get_instance(new_instance_id)
        assert new_instance is not None
        # New instance should be completed (it ran iteration=1 and returned)
        assert new_instance["status"] == "completed"
        assert new_instance["input_data"] == {"iteration": 1}

    async def test_recur_marks_old_as_recurred(self, replay_engine, sqlite_storage):
        """Test that the original workflow is marked as 'recurred'."""

        @activity
        async def do_work(ctx: WorkflowContext) -> dict:
            return {"done": True}

        async def recur_workflow(ctx: WorkflowContext, count: int = 0) -> dict:
            await do_work(ctx)

            if count == 0:
                await ctx.recur(count=count + 1)

            return {"final_count": count}

        # Track the original instance ID
        original_id = None

        # Monkey-patch to capture original ID
        original_start = replay_engine.start_workflow

        async def capturing_start(*args, **kwargs):
            nonlocal original_id
            result = await original_start(*args, **kwargs)
            return result

        # Start workflow
        _ = await replay_engine.start_workflow(
            workflow_name="recur_workflow",
            workflow_func=recur_workflow,
            input_data={"count": 0},
        )

        # Find the original instance (status = 'recurred')
        async with AsyncSession(sqlite_storage.engine) as session:
            result = await session.execute(
                select(WorkflowInstance).where(WorkflowInstance.status == "recurred")
            )
            recurred_instance = result.scalar_one_or_none()

            assert recurred_instance is not None
            assert recurred_instance.input_data == '{"count": 0}'

    async def test_recur_sets_continued_from(self, replay_engine, sqlite_storage):
        """Test that new instance has continued_from set to old instance."""

        async def simple_recur(ctx: WorkflowContext, n: int = 0) -> dict:
            if n == 0:
                await ctx.recur(n=1)
            return {"n": n}

        new_instance_id = await replay_engine.start_workflow(
            workflow_name="simple_recur",
            workflow_func=simple_recur,
            input_data={"n": 0},
        )

        # Get new instance and check continued_from
        async with AsyncSession(sqlite_storage.engine) as session:
            # Find the new instance
            result = await session.execute(
                select(WorkflowInstance).where(WorkflowInstance.instance_id == new_instance_id)
            )
            new_instance = result.scalar_one()

            assert new_instance.continued_from is not None

            # Find the old instance
            result = await session.execute(
                select(WorkflowInstance).where(
                    WorkflowInstance.instance_id == new_instance.continued_from
                )
            )
            old_instance = result.scalar_one()

            assert old_instance.status == "recurred"

    async def test_recur_archives_history(self, replay_engine, sqlite_storage):
        """Test that history is archived when workflow recurs."""

        @activity
        async def activity_one(ctx: WorkflowContext) -> dict:
            return {"step": 1}

        @activity
        async def activity_two(ctx: WorkflowContext) -> dict:
            return {"step": 2}

        async def workflow_with_activities(ctx: WorkflowContext, iteration: int = 0) -> dict:
            await activity_one(ctx)
            await activity_two(ctx)

            if iteration == 0:
                await ctx.recur(iteration=1)

            return {"iteration": iteration}

        await replay_engine.start_workflow(
            workflow_name="workflow_with_activities",
            workflow_func=workflow_with_activities,
            input_data={"iteration": 0},
        )

        # Check that history was archived
        async with AsyncSession(sqlite_storage.engine) as session:
            # Find archived entries
            result = await session.execute(select(WorkflowHistoryArchive))
            archived = result.scalars().all()

            # Should have 2 archived entries from the first iteration
            assert len(archived) >= 2

            # Check that archived entries have the expected activity IDs
            activity_ids = [entry.activity_id for entry in archived]
            assert "activity_one:1" in activity_ids
            assert "activity_two:1" in activity_ids

    async def test_recur_clears_compensations(self, replay_engine, sqlite_storage):
        """Test that compensations are cleared when workflow recurs."""
        from edda.compensation import register_compensation

        # Track if compensation was called
        compensation_called = []

        async def undo_action(value: str) -> None:
            compensation_called.append(value)

        @activity
        async def action_with_compensation(ctx: WorkflowContext, value: str) -> dict:
            # Register a compensation
            await register_compensation(ctx, undo_action, value=value)
            return {"action": "done", "value": value}

        async def workflow_with_compensation(ctx: WorkflowContext, count: int = 0) -> dict:
            await action_with_compensation(ctx, f"value-{count}")

            if count == 0:
                await ctx.recur(count=1)

            return {"count": count}

        await replay_engine.start_workflow(
            workflow_name="workflow_with_compensation",
            workflow_func=workflow_with_compensation,
            input_data={"count": 0},
        )

        # Check that compensations were cleared for the recurred instance
        async with AsyncSession(sqlite_storage.engine) as session:
            result = await session.execute(
                select(WorkflowInstance).where(WorkflowInstance.status == "recurred")
            )
            recurred_instance = result.scalar_one()

            # Get compensations for recurred instance
            compensations = await sqlite_storage.get_compensations(recurred_instance.instance_id)
            assert len(compensations) == 0

            # Compensation should not have been called (recur, not failure)
            assert len(compensation_called) == 0

    async def test_multiple_recurs(self, replay_engine, sqlite_storage):
        """Test multiple recur operations creating a chain."""

        async def chain_workflow(ctx: WorkflowContext, step: int = 0) -> dict:
            if step < 2:
                await ctx.recur(step=step + 1)
            return {"final_step": step}

        _ = await replay_engine.start_workflow(
            workflow_name="chain_workflow",
            workflow_func=chain_workflow,
            input_data={"step": 0},
        )

        # Should have 3 instances in total (step 0, 1, 2)
        async with AsyncSession(sqlite_storage.engine) as session:
            result = await session.execute(
                select(WorkflowInstance).where(WorkflowInstance.workflow_name == "chain_workflow")
            )
            all_instances = result.scalars().all()

            assert len(all_instances) == 3

            # Count statuses
            recurred_count = sum(1 for i in all_instances if i.status == "recurred")
            completed_count = sum(1 for i in all_instances if i.status == "completed")

            assert recurred_count == 2  # step 0 and 1
            assert completed_count == 1  # step 2 (final)

            # Final instance should have input step=2
            final_instance = next(i for i in all_instances if i.status == "completed")
            assert '"step": 2' in final_instance.input_data

    async def test_recur_new_instance_has_fresh_history(self, replay_engine, sqlite_storage):
        """Test that new instance starts with fresh history."""

        @activity
        async def count_activity(ctx: WorkflowContext, n: int) -> dict:
            return {"n": n}

        async def counting_workflow(ctx: WorkflowContext, iteration: int = 0) -> dict:
            # Call activity multiple times
            for i in range(3):
                await count_activity(ctx, i, activity_id=f"count:{i}")

            if iteration == 0:
                await ctx.recur(iteration=1)

            return {"iteration": iteration}

        final_id = await replay_engine.start_workflow(
            workflow_name="counting_workflow",
            workflow_func=counting_workflow,
            input_data={"iteration": 0},
        )

        # Get history for the final instance
        final_history = await sqlite_storage.get_history(final_id)

        # Should have 3 fresh entries (not 6)
        assert len(final_history) == 3

        # All should be from iteration 1
        for entry in final_history:
            assert entry["activity_id"].startswith("count:")


@pytest.mark.asyncio
class TestArchiveHistory:
    """Test suite for archive_history storage method."""

    async def test_archive_history_moves_entries(self, sqlite_storage):
        """Test that archive_history moves entries to archive table."""
        # Create workflow definition and instance
        await sqlite_storage.upsert_workflow_definition(
            workflow_name="archive_test",
            source_hash="hash-123",
            source_code="async def archive_test(ctx): pass",
        )

        instance_id = "archive-test-001"
        await sqlite_storage.create_instance(
            instance_id=instance_id,
            workflow_name="archive_test",
            source_hash="hash-123",
            owner_service="test",
            input_data={},
        )

        # Add some history entries
        await sqlite_storage.append_history(
            instance_id=instance_id,
            activity_id="activity:1",
            event_type="ActivityCompleted",
            event_data={"result": "first"},
        )
        await sqlite_storage.append_history(
            instance_id=instance_id,
            activity_id="activity:2",
            event_type="ActivityCompleted",
            event_data={"result": "second"},
        )

        # Archive history
        archived_count = await sqlite_storage.archive_history(instance_id)

        assert archived_count == 2

        # Check that original history is empty
        history = await sqlite_storage.get_history(instance_id)
        assert len(history) == 0

        # Check that entries are in archive
        async with AsyncSession(sqlite_storage.engine) as session:
            result = await session.execute(
                select(WorkflowHistoryArchive).where(
                    WorkflowHistoryArchive.instance_id == instance_id
                )
            )
            archived = result.scalars().all()

            assert len(archived) == 2

    async def test_archive_history_preserves_data(self, sqlite_storage):
        """Test that archived entries preserve all original data."""
        await sqlite_storage.upsert_workflow_definition(
            workflow_name="preserve_test",
            source_hash="hash-456",
            source_code="async def preserve_test(ctx): pass",
        )

        instance_id = "preserve-test-001"
        await sqlite_storage.create_instance(
            instance_id=instance_id,
            workflow_name="preserve_test",
            source_hash="hash-456",
            owner_service="test",
            input_data={},
        )

        original_data = {"result": {"nested": "data", "list": [1, 2, 3]}}

        await sqlite_storage.append_history(
            instance_id=instance_id,
            activity_id="complex:1",
            event_type="ActivityCompleted",
            event_data=original_data,
        )

        await sqlite_storage.archive_history(instance_id)

        # Check archived data matches original
        async with AsyncSession(sqlite_storage.engine) as session:
            result = await session.execute(
                select(WorkflowHistoryArchive).where(
                    WorkflowHistoryArchive.instance_id == instance_id
                )
            )
            archived = result.scalar_one()

            import json

            archived_data = json.loads(archived.event_data)
            assert archived_data == original_data

    async def test_archive_history_handles_binary_data(self, sqlite_storage):
        """Test that binary data is properly handled during archival."""
        await sqlite_storage.upsert_workflow_definition(
            workflow_name="binary_test",
            source_hash="hash-789",
            source_code="async def binary_test(ctx): pass",
        )

        instance_id = "binary-test-001"
        await sqlite_storage.create_instance(
            instance_id=instance_id,
            workflow_name="binary_test",
            source_hash="hash-789",
            owner_service="test",
            input_data={},
        )

        # Add binary history entry
        binary_data = b"\x00\x01\x02\x03\xff"
        await sqlite_storage.append_history(
            instance_id=instance_id,
            activity_id="binary:1",
            event_type="ActivityCompleted",
            event_data=binary_data,
        )

        archived_count = await sqlite_storage.archive_history(instance_id)
        assert archived_count == 1

        # Check archived binary data
        async with AsyncSession(sqlite_storage.engine) as session:
            result = await session.execute(
                select(WorkflowHistoryArchive).where(
                    WorkflowHistoryArchive.instance_id == instance_id
                )
            )
            archived = result.scalar_one()

            import base64
            import json

            archived_json = json.loads(archived.event_data)
            assert archived_json["_binary"] is True
            decoded = base64.b64decode(archived_json["data"])
            assert decoded == binary_data

    async def test_archive_history_empty_history(self, sqlite_storage):
        """Test archiving when there's no history."""
        await sqlite_storage.upsert_workflow_definition(
            workflow_name="empty_test",
            source_hash="hash-empty",
            source_code="async def empty_test(ctx): pass",
        )

        instance_id = "empty-test-001"
        await sqlite_storage.create_instance(
            instance_id=instance_id,
            workflow_name="empty_test",
            source_hash="hash-empty",
            owner_service="test",
            input_data={},
        )

        archived_count = await sqlite_storage.archive_history(instance_id)
        assert archived_count == 0
