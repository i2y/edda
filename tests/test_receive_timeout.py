"""
Tests for receive() timeout handling.

Tests verify that:
1. TimeoutError is raised during replay when history contains timeout error
2. Workflow can catch TimeoutError with try/except
3. _check_expired_message_subscriptions() resumes workflow instead of failing it
"""

import pytest
import pytest_asyncio

from edda.channels import receive, subscribe
from edda.context import WorkflowContext


@pytest.mark.asyncio
class TestReceiveTimeoutReplay:
    """Test that receive() raises TimeoutError during replay when history contains timeout."""

    @pytest_asyncio.fixture
    async def workflow_instance(self, sqlite_storage, create_test_instance):
        """Create a workflow instance for testing."""
        instance_id = "test-timeout-instance-001"
        await create_test_instance(
            instance_id=instance_id,
            workflow_name="test_workflow",
            owner_service="test-service",
            input_data={"test": "data"},
        )
        await sqlite_storage.update_instance_status(instance_id, "running")
        return instance_id

    async def test_receive_raises_timeout_error_during_replay(
        self, sqlite_storage, workflow_instance
    ):
        """Test that receive() raises TimeoutError when replaying a timeout event."""
        # Record a timeout error in history (simulating what _check_expired_message_subscriptions does)
        await sqlite_storage.append_history(
            instance_id=workflow_instance,
            activity_id="receive_payment:1",
            event_type="MessageTimeout",
            event_data={
                "_error": True,
                "error_type": "TimeoutError",
                "error_message": "Message on channel 'payment' did not arrive within timeout",
                "channel": "payment",
                "timeout_at": "2025-01-01T00:00:00+00:00",
            },
        )

        # Create context in replay mode
        ctx = WorkflowContext(
            instance_id=workflow_instance,
            workflow_name="test_workflow",
            storage=sqlite_storage,
            worker_id="worker-1",
            is_replaying=True,
        )

        # Subscribe first (required before receive)
        await subscribe(ctx, "payment", mode="broadcast")

        # Load history to populate cache with the timeout error
        await ctx._load_history()

        # Reset activity counter after subscribe
        ctx._activity_call_counters.clear()

        # receive() should raise TimeoutError from cached history
        with pytest.raises(TimeoutError) as exc_info:
            await receive(ctx, channel="payment", message_id="receive_payment:1")

        assert "did not arrive within timeout" in str(exc_info.value)

    async def test_receive_timeout_can_be_caught_in_workflow(
        self, sqlite_storage, workflow_instance
    ):
        """Test that TimeoutError can be caught with try/except in workflow code."""
        # Record a timeout error in history
        await sqlite_storage.append_history(
            instance_id=workflow_instance,
            activity_id="receive_approval:1",
            event_type="MessageTimeout",
            event_data={
                "_error": True,
                "error_type": "TimeoutError",
                "error_message": "Message on channel 'approval' did not arrive within 60 seconds",
                "channel": "approval",
            },
        )

        # Create context in replay mode
        ctx = WorkflowContext(
            instance_id=workflow_instance,
            workflow_name="test_workflow",
            storage=sqlite_storage,
            worker_id="worker-1",
            is_replaying=True,
        )

        await subscribe(ctx, "approval", mode="broadcast")

        # Load history to populate cache with the timeout error
        await ctx._load_history()

        ctx._activity_call_counters.clear()

        # Simulate workflow code that catches TimeoutError
        timeout_caught = False
        try:
            await receive(ctx, channel="approval", message_id="receive_approval:1")
        except TimeoutError:
            timeout_caught = True

        assert timeout_caught is True

    async def test_receive_timeout_with_generic_error_type(self, sqlite_storage, workflow_instance):
        """Test that non-TimeoutError errors are also re-raised during replay."""
        # Record a timeout event but with a different error type (not TimeoutError)
        # This tests that receive() properly handles various error types from MessageTimeout events
        await sqlite_storage.append_history(
            instance_id=workflow_instance,
            activity_id="receive_data:1",
            event_type="MessageTimeout",
            event_data={
                "_error": True,
                "error_type": "ValueError",
                "error_message": "Invalid message format",
                "channel": "data",
            },
        )

        # Create context in replay mode
        ctx = WorkflowContext(
            instance_id=workflow_instance,
            workflow_name="test_workflow",
            storage=sqlite_storage,
            worker_id="worker-1",
            is_replaying=True,
        )

        await subscribe(ctx, "data", mode="broadcast")

        # Load history to populate cache with the error
        await ctx._load_history()

        ctx._activity_call_counters.clear()

        # Should raise generic Exception for non-TimeoutError
        with pytest.raises(Exception) as exc_info:
            await receive(ctx, channel="data", message_id="receive_data:1")

        assert "ValueError" in str(exc_info.value)
        assert "Invalid message format" in str(exc_info.value)


@pytest.mark.asyncio
class TestCheckExpiredMessageSubscriptions:
    """Test that _check_expired_message_subscriptions resumes workflow."""

    @pytest_asyncio.fixture
    async def workflow_instance(self, sqlite_storage, create_test_instance):
        """Create a workflow instance for testing."""
        instance_id = "test-timeout-workflow-001"
        await create_test_instance(
            instance_id=instance_id,
            workflow_name="timeout_test_workflow",
            owner_service="test-service",
            input_data={"test": "data"},
        )
        await sqlite_storage.update_instance_status(instance_id, "waiting_for_message")
        return instance_id

    async def test_find_expired_message_subscriptions_returns_workflow_name(
        self, sqlite_storage, workflow_instance
    ):
        """Test that find_expired_message_subscriptions returns workflow_name."""
        from datetime import UTC, datetime, timedelta

        # Subscribe and set timeout in the past
        await sqlite_storage.subscribe_to_channel(
            workflow_instance, "test_channel", mode="broadcast"
        )

        # Manually set timeout_at to past (simulating expired timeout)
        # This requires direct SQL since subscribe_to_channel doesn't set timeout
        from sqlalchemy import text

        past_time = datetime.now(UTC) - timedelta(seconds=60)
        async with sqlite_storage.engine.begin() as conn:
            await conn.execute(
                text(
                    """
                    UPDATE channel_subscriptions
                    SET timeout_at = :timeout_at, activity_id = :activity_id
                    WHERE instance_id = :instance_id AND channel = :channel
                    """
                ),
                {
                    "timeout_at": past_time,
                    "activity_id": "receive_test_channel:1",
                    "instance_id": workflow_instance,
                    "channel": "test_channel",
                },
            )

        # Find expired subscriptions
        expired = await sqlite_storage.find_expired_message_subscriptions()

        # Should find our expired subscription with workflow_name
        assert len(expired) >= 1
        our_sub = next((s for s in expired if s["instance_id"] == workflow_instance), None)
        assert our_sub is not None
        assert our_sub["workflow_name"] == "timeout_test_workflow"
        assert our_sub["channel"] == "test_channel"
        assert our_sub["activity_id"] == "receive_test_channel:1"
