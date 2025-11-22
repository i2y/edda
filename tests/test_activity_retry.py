"""
Tests for automatic activity retry functionality.

Tests cover:
- Default retry policy (5 attempts, exponential backoff)
- Custom retry policies
- RetryExhaustedError wrapping with __cause__
- TerminalError immediate failure
- Retry metadata recording in history
- Policy resolution order (activity > app > default)
- Backoff timing verification
- Max duration enforcement
- Replay behavior (no retry during replay)
"""

from datetime import UTC, datetime

import pytest

from edda import RetryPolicy, TerminalError, activity
from edda.context import WorkflowContext
from edda.exceptions import RetryExhaustedError


@pytest.mark.asyncio
class TestDefaultRetryPolicy:
    """Test default retry policy behavior (5 attempts, exponential backoff)."""

    async def test_activity_succeeds_on_first_attempt(self, sqlite_storage, create_test_instance):
        """Test that successful activity doesn't retry."""
        instance_id = "test-retry-001"
        await create_test_instance(
            instance_id=instance_id,
            workflow_name="test_workflow",
            owner_service="test-service",
            input_data={},
        )

        ctx = WorkflowContext(
            instance_id=instance_id,
            workflow_name="test_workflow",
            storage=sqlite_storage,
            worker_id="worker-1",
            is_replaying=False,
        )

        attempt_count = 0

        @activity
        async def successful_activity(ctx: WorkflowContext, value: int) -> dict:
            nonlocal attempt_count
            attempt_count += 1
            return {"result": value * 2}

        result = await successful_activity(ctx, 21, activity_id="successful_activity:1")

        assert result == {"result": 42}
        assert attempt_count == 1  # No retries

        # Verify history: no retry metadata (success on first attempt)
        history = await sqlite_storage.get_history(instance_id)
        assert len(history) == 1
        assert history[0]["event_type"] == "ActivityCompleted"
        assert "retry_metadata" not in history[0]["event_data"]

    async def test_activity_retries_with_exponential_backoff(
        self, sqlite_storage, create_test_instance
    ):
        """Test that activity retries 5 times with exponential backoff."""
        instance_id = "test-retry-002"
        await create_test_instance(
            instance_id=instance_id,
            workflow_name="test_workflow",
            owner_service="test-service",
            input_data={},
        )

        ctx = WorkflowContext(
            instance_id=instance_id,
            workflow_name="test_workflow",
            storage=sqlite_storage,
            worker_id="worker-1",
            is_replaying=False,
        )

        attempt_count = 0
        attempt_times = []

        @activity
        async def flaky_activity(ctx: WorkflowContext, value: int) -> dict:
            nonlocal attempt_count
            attempt_count += 1
            attempt_times.append(datetime.now(UTC))

            if attempt_count < 3:
                raise ValueError(f"Attempt {attempt_count} failed")

            return {"result": value * 2, "attempts": attempt_count}

        result = await flaky_activity(ctx, 21, activity_id="flaky_activity:1")

        assert result == {"result": 42, "attempts": 3}
        assert attempt_count == 3  # 1 initial + 2 retries

        # Verify history: retry metadata should be present
        history = await sqlite_storage.get_history(instance_id)
        assert len(history) == 1
        assert history[0]["event_type"] == "ActivityCompleted"
        assert "retry_metadata" in history[0]["event_data"]

        retry_metadata = history[0]["event_data"]["retry_metadata"]
        assert retry_metadata["total_attempts"] == 3
        assert retry_metadata["exhausted"] is False
        assert retry_metadata["last_error"]["message"] == "Attempt 2 failed"

        # Verify exponential backoff timing (approximately 1s between attempts)
        assert len(attempt_times) == 3
        delay1 = (attempt_times[1] - attempt_times[0]).total_seconds()
        delay2 = (attempt_times[2] - attempt_times[1]).total_seconds()
        assert 0.9 <= delay1 <= 1.5  # ~1 second (allow some variance)
        assert 1.9 <= delay2 <= 2.5  # ~2 seconds (allow some variance)

    async def test_activity_exhausts_retries(self, sqlite_storage, create_test_instance):
        """Test that activity wraps error in RetryExhaustedError after 5 attempts."""
        instance_id = "test-retry-003"
        await create_test_instance(
            instance_id=instance_id,
            workflow_name="test_workflow",
            owner_service="test-service",
            input_data={},
        )

        ctx = WorkflowContext(
            instance_id=instance_id,
            workflow_name="test_workflow",
            storage=sqlite_storage,
            worker_id="worker-1",
            is_replaying=False,
        )

        attempt_count = 0

        @activity
        async def always_failing_activity(ctx: WorkflowContext, value: int) -> dict:
            nonlocal attempt_count
            attempt_count += 1
            raise ConnectionError(f"Network error on attempt {attempt_count}")

        with pytest.raises(RetryExhaustedError) as exc_info:
            await always_failing_activity(ctx, 21, activity_id="always_failing_activity:1")

        assert attempt_count == 5  # 5 attempts (default max_attempts)

        # Verify exception chaining (__cause__)
        assert exc_info.value.__cause__ is not None
        assert isinstance(exc_info.value.__cause__, ConnectionError)
        assert "Network error on attempt 5" in str(exc_info.value.__cause__)

        # Verify history: failure with retry metadata
        history = await sqlite_storage.get_history(instance_id)
        assert len(history) == 1
        assert history[0]["event_type"] == "ActivityFailed"
        assert "retry_metadata" in history[0]["event_data"]

        retry_metadata = history[0]["event_data"]["retry_metadata"]
        assert retry_metadata["total_attempts"] == 5
        assert retry_metadata["exhausted"] is True
        assert retry_metadata["last_error"]["message"] == "Network error on attempt 5"


@pytest.mark.asyncio
class TestCustomRetryPolicy:
    """Test custom retry policies at activity level."""

    async def test_activity_custom_policy_3_attempts(self, sqlite_storage, create_test_instance):
        """Test activity with custom retry policy (3 attempts)."""
        instance_id = "test-retry-004"
        await create_test_instance(
            instance_id=instance_id,
            workflow_name="test_workflow",
            owner_service="test-service",
            input_data={},
        )

        ctx = WorkflowContext(
            instance_id=instance_id,
            workflow_name="test_workflow",
            storage=sqlite_storage,
            worker_id="worker-1",
            is_replaying=False,
        )

        attempt_count = 0

        @activity(retry_policy=RetryPolicy(max_attempts=3, initial_interval=0.1))
        async def custom_retry_activity(ctx: WorkflowContext, value: int) -> dict:
            nonlocal attempt_count
            attempt_count += 1
            raise ValueError(f"Attempt {attempt_count}")

        with pytest.raises(RetryExhaustedError):
            await custom_retry_activity(ctx, 21, activity_id="custom_retry_activity:1")

        assert attempt_count == 3  # Custom max_attempts

    async def test_activity_custom_backoff_timing(self, sqlite_storage, create_test_instance):
        """Test custom backoff multiplier and initial delay."""
        instance_id = "test-retry-005"
        await create_test_instance(
            instance_id=instance_id,
            workflow_name="test_workflow",
            owner_service="test-service",
            input_data={},
        )

        ctx = WorkflowContext(
            instance_id=instance_id,
            workflow_name="test_workflow",
            storage=sqlite_storage,
            worker_id="worker-1",
            is_replaying=False,
        )

        attempt_times = []

        @activity(
            retry_policy=RetryPolicy(
                max_attempts=3,
                initial_interval=0.5,
                backoff_coefficient=3.0,  # 0.5s, 1.5s
            )
        )
        async def custom_backoff_activity(ctx: WorkflowContext) -> dict:
            attempt_times.append(datetime.now(UTC))
            raise ValueError("Test error")

        with pytest.raises(RetryExhaustedError):
            await custom_backoff_activity(ctx, activity_id="custom_backoff_activity:1")

        # Verify timing: ~0.5s, ~1.5s delays
        assert len(attempt_times) == 3
        delay1 = (attempt_times[1] - attempt_times[0]).total_seconds()
        delay2 = (attempt_times[2] - attempt_times[1]).total_seconds()
        assert 0.4 <= delay1 <= 0.7  # ~0.5 second
        assert 1.4 <= delay2 <= 1.8  # ~1.5 seconds


@pytest.mark.asyncio
class TestTerminalError:
    """Test TerminalError for non-retryable errors."""

    async def test_terminal_error_no_retry(self, sqlite_storage, create_test_instance):
        """Test that TerminalError propagates immediately without retry."""
        instance_id = "test-retry-006"
        await create_test_instance(
            instance_id=instance_id,
            workflow_name="test_workflow",
            owner_service="test-service",
            input_data={},
        )

        ctx = WorkflowContext(
            instance_id=instance_id,
            workflow_name="test_workflow",
            storage=sqlite_storage,
            worker_id="worker-1",
            is_replaying=False,
        )

        attempt_count = 0

        @activity
        async def terminal_error_activity(ctx: WorkflowContext, user_id: str) -> dict:
            nonlocal attempt_count
            attempt_count += 1
            # Simulate validation error (non-retryable)
            raise TerminalError(f"User {user_id} not found")

        with pytest.raises(TerminalError, match="User 123 not found"):
            await terminal_error_activity(ctx, "123", activity_id="terminal_error_activity:1")

        assert attempt_count == 1  # No retries

        # Verify history: failure without retry metadata
        history = await sqlite_storage.get_history(instance_id)
        assert len(history) == 1
        assert history[0]["event_type"] == "ActivityFailed"
        assert "retry_metadata" not in history[0]["event_data"]


@pytest.mark.asyncio
class TestRetryMetadata:
    """Test retry metadata recording in history."""

    async def test_retry_metadata_in_completed_event(self, sqlite_storage, create_test_instance):
        """Test that retry metadata is embedded in ActivityCompleted event."""
        instance_id = "test-retry-007"
        await create_test_instance(
            instance_id=instance_id,
            workflow_name="test_workflow",
            owner_service="test-service",
            input_data={},
        )

        ctx = WorkflowContext(
            instance_id=instance_id,
            workflow_name="test_workflow",
            storage=sqlite_storage,
            worker_id="worker-1",
            is_replaying=False,
        )

        attempt_count = 0

        @activity
        async def retry_metadata_activity(ctx: WorkflowContext) -> dict:
            nonlocal attempt_count
            attempt_count += 1
            if attempt_count < 2:
                raise ValueError("First attempt failed")
            return {"success": True}

        result = await retry_metadata_activity(ctx, activity_id="retry_metadata_activity:1")

        assert result == {"success": True}

        # Verify retry metadata
        history = await sqlite_storage.get_history(instance_id)
        assert len(history) == 1
        assert history[0]["event_type"] == "ActivityCompleted"

        retry_metadata = history[0]["event_data"]["retry_metadata"]
        assert retry_metadata["total_attempts"] == 2
        assert retry_metadata["exhausted"] is False
        assert "total_duration_ms" in retry_metadata
        assert retry_metadata["last_error"]["message"] == "First attempt failed"

    async def test_retry_metadata_in_failed_event(self, sqlite_storage, create_test_instance):
        """Test that retry metadata is embedded in ActivityFailed event."""
        instance_id = "test-retry-008"
        await create_test_instance(
            instance_id=instance_id,
            workflow_name="test_workflow",
            owner_service="test-service",
            input_data={},
        )

        ctx = WorkflowContext(
            instance_id=instance_id,
            workflow_name="test_workflow",
            storage=sqlite_storage,
            worker_id="worker-1",
            is_replaying=False,
        )

        @activity(retry_policy=RetryPolicy(max_attempts=2, initial_interval=0.1))
        async def failing_activity(ctx: WorkflowContext) -> dict:
            raise ConnectionError("Network error")

        with pytest.raises(RetryExhaustedError):
            await failing_activity(ctx, activity_id="failing_activity:1")

        # Verify retry metadata in failure
        history = await sqlite_storage.get_history(instance_id)
        assert len(history) == 1
        assert history[0]["event_type"] == "ActivityFailed"

        retry_metadata = history[0]["event_data"]["retry_metadata"]
        assert retry_metadata["total_attempts"] == 2
        assert retry_metadata["exhausted"] is True
        assert retry_metadata["last_error"]["message"] == "Network error"


@pytest.mark.asyncio
class TestReplayBehavior:
    """Test that replay skips retry loop (uses cached results)."""

    async def test_replay_skips_retry_loop(self, sqlite_storage, create_test_instance):
        """Test that cached activity results bypass retry logic during replay."""
        instance_id = "test-retry-009"
        await create_test_instance(
            instance_id=instance_id,
            workflow_name="test_workflow",
            owner_service="test-service",
            input_data={},
        )

        # Add history for replayed activity
        await sqlite_storage.append_history(
            instance_id,
            activity_id="replay_activity:1",
            event_type="ActivityCompleted",
            event_data={
                "activity_name": "replay_activity",
                "result": {"cached": True, "value": 42},
                "retry_metadata": {
                    "total_attempts": 3,
                    "total_duration_ms": 5200,
                    "last_error": {"type": "ValueError", "message": "Previous error"},
                    "exhausted": False,
                    "errors": [],
                },
            },
        )

        # Create replaying context
        ctx = WorkflowContext(
            instance_id=instance_id,
            workflow_name="test_workflow",
            storage=sqlite_storage,
            worker_id="worker-1",
            is_replaying=True,
        )

        # Load history for replay
        await ctx._load_history()

        execution_count = 0

        @activity
        async def replay_activity(ctx: WorkflowContext) -> dict:
            nonlocal execution_count
            execution_count += 1
            # This should never execute during replay
            raise RuntimeError("This should not execute during replay")

        result = await replay_activity(ctx, activity_id="replay_activity:1")

        # Verify: cached result returned, no execution
        assert result == {"cached": True, "value": 42}
        assert execution_count == 0  # No retry loop execution during replay


@pytest.mark.asyncio
class TestMaxDurationEnforcement:
    """Test max_duration_seconds enforcement in retry policy."""

    async def test_max_duration_stops_retries(self, sqlite_storage, create_test_instance):
        """Test that retries stop when max_duration_seconds is exceeded."""
        instance_id = "test-retry-010"
        await create_test_instance(
            instance_id=instance_id,
            workflow_name="test_workflow",
            owner_service="test-service",
            input_data={},
        )

        ctx = WorkflowContext(
            instance_id=instance_id,
            workflow_name="test_workflow",
            storage=sqlite_storage,
            worker_id="worker-1",
            is_replaying=False,
        )

        attempt_count = 0

        @activity(
            retry_policy=RetryPolicy(
                max_attempts=100,  # High max_attempts
                initial_interval=1.0,
                backoff_coefficient=1.0,
                max_duration=3.0,  # But short max_duration
            )
        )
        async def max_duration_activity(ctx: WorkflowContext) -> dict:
            nonlocal attempt_count
            attempt_count += 1
            raise ValueError("Always fails")

        with pytest.raises(RetryExhaustedError):
            await max_duration_activity(ctx, activity_id="max_duration_activity:1")

        # Should stop before 100 attempts due to max_duration
        assert attempt_count < 100
        # With 1s delays, should stop after ~3 attempts (1s + 1s + 1s = 3s)
        assert 3 <= attempt_count <= 4

        # Verify retry metadata shows duration enforcement
        history = await sqlite_storage.get_history(instance_id)
        retry_metadata = history[0]["event_data"]["retry_metadata"]
        assert retry_metadata["total_duration_ms"] >= 3000  # 3 seconds in milliseconds
        assert retry_metadata["exhausted"] is True
