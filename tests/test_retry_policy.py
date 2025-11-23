"""
Tests for RetryPolicy configuration and edge cases.

Tests cover:
- RetryPolicy validation and defaults
- Policy resolution order (activity > app > default)
- Edge cases (0 attempts, negative delays, etc.)
- RetryMetadata structure and tracking
- Backoff calculation edge cases
"""

import pytest

from edda import EddaApp, RetryPolicy, activity
from edda.context import WorkflowContext
from edda.exceptions import RetryExhaustedError
from edda.retry import DEFAULT_RETRY_POLICY, RetryMetadata


class TestRetryPolicyDefaults:
    """Test default RetryPolicy values."""

    def test_default_retry_policy_values(self):
        """Test that DEFAULT_RETRY_POLICY has correct values."""
        policy = DEFAULT_RETRY_POLICY

        assert policy.max_attempts == 5
        assert policy.initial_interval == 1.0
        assert policy.backoff_coefficient == 2.0
        assert policy.max_interval == 60.0
        assert policy.max_duration == 300.0  # 5 minutes

    def test_custom_retry_policy_creation(self):
        """Test creating custom RetryPolicy with specific values."""
        policy = RetryPolicy(
            max_attempts=10,
            initial_interval=2.0,
            backoff_coefficient=3.0,
            max_interval=120.0,
            max_duration=600.0,
        )

        assert policy.max_attempts == 10
        assert policy.initial_interval == 2.0
        assert policy.backoff_coefficient == 3.0
        assert policy.max_interval == 120.0
        assert policy.max_duration == 600.0

    def test_retry_policy_partial_defaults(self):
        """Test creating RetryPolicy with partial overrides."""
        policy = RetryPolicy(
            max_attempts=3,
            initial_interval=0.5,
            # Other values use defaults
        )

        assert policy.max_attempts == 3
        assert policy.initial_interval == 0.5
        assert policy.backoff_coefficient == 2.0  # Default
        assert policy.max_interval == 60.0  # Default
        assert policy.max_duration == 300.0  # Default


@pytest.mark.asyncio
class TestRetryPolicyResolution:
    """Test retry policy resolution order (activity > app > default)."""

    async def test_activity_level_policy_overrides_app_level(
        self, sqlite_storage, create_test_instance
    ):
        """Test that activity-level policy takes precedence over app-level."""
        instance_id = "test-policy-001"
        await create_test_instance(
            instance_id=instance_id,
            workflow_name="test_workflow",
            owner_service="test-service",
            input_data={},
        )

        # App-level policy (10 attempts)
        app_policy = RetryPolicy(max_attempts=10, initial_interval=0.1)

        ctx = WorkflowContext(
            instance_id=instance_id,
            workflow_name="test_workflow",
            storage=sqlite_storage,
            worker_id="worker-1",
            is_replaying=False,
        )
        # Simulate app-level policy
        ctx._app_retry_policy = app_policy

        attempt_count = 0

        # Activity-level policy overrides (3 attempts)
        @activity(retry_policy=RetryPolicy(max_attempts=3, initial_interval=0.1))
        async def activity_policy_override(ctx: WorkflowContext) -> dict:
            nonlocal attempt_count
            attempt_count += 1
            raise ValueError("Always fails")

        with pytest.raises(RetryExhaustedError):
            await activity_policy_override(ctx, activity_id="activity_policy_override:1")

        # Should use activity-level policy (3 attempts), not app-level (10)
        assert attempt_count == 3

    async def test_app_level_policy_overrides_default(self, sqlite_storage, create_test_instance):
        """Test that app-level policy takes precedence over framework default."""
        instance_id = "test-policy-002"
        await create_test_instance(
            instance_id=instance_id,
            workflow_name="test_workflow",
            owner_service="test-service",
            input_data={},
        )

        # App-level policy (2 attempts)
        app_policy = RetryPolicy(max_attempts=2, initial_interval=0.1)

        ctx = WorkflowContext(
            instance_id=instance_id,
            workflow_name="test_workflow",
            storage=sqlite_storage,
            worker_id="worker-1",
            is_replaying=False,
        )
        # Simulate app-level policy
        ctx._app_retry_policy = app_policy

        attempt_count = 0

        # Activity without custom policy (uses app-level)
        @activity
        async def uses_app_policy(ctx: WorkflowContext) -> dict:
            nonlocal attempt_count
            attempt_count += 1
            raise ValueError("Always fails")

        with pytest.raises(RetryExhaustedError):
            await uses_app_policy(ctx, activity_id="uses_app_policy:1")

        # Should use app-level policy (2 attempts), not default (5)
        assert attempt_count == 2

    async def test_default_policy_when_no_overrides(self, sqlite_storage, create_test_instance):
        """Test that framework default is used when no app/activity policy."""
        instance_id = "test-policy-003"
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
        # No app-level policy
        ctx._app_retry_policy = None

        attempt_count = 0

        # Activity without custom policy (uses default)
        @activity
        async def uses_default_policy(ctx: WorkflowContext) -> dict:
            nonlocal attempt_count
            attempt_count += 1
            raise ValueError("Always fails")

        with pytest.raises(RetryExhaustedError):
            await uses_default_policy(ctx, activity_id="uses_default_policy:1")

        # Should use default policy (5 attempts)
        assert attempt_count == 5


@pytest.mark.asyncio
class TestRetryPolicyEdgeCases:
    """Test edge cases and validation for RetryPolicy."""

    async def test_max_attempts_1_no_retry(self, sqlite_storage, create_test_instance):
        """Test that max_attempts=1 means no retries (only initial attempt)."""
        instance_id = "test-policy-004"
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

        @activity(retry_policy=RetryPolicy(max_attempts=1, initial_interval=0.1))
        async def no_retry_activity(ctx: WorkflowContext) -> dict:
            nonlocal attempt_count
            attempt_count += 1
            raise ValueError("Fails on first attempt")

        with pytest.raises(RetryExhaustedError):
            await no_retry_activity(ctx, activity_id="no_retry_activity:1")

        assert attempt_count == 1  # Only initial attempt, no retries

    async def test_max_delay_caps_backoff(self, sqlite_storage, create_test_instance):
        """Test that max_delay_seconds caps exponential backoff."""
        instance_id = "test-policy-005"
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

        from datetime import UTC, datetime

        attempt_times = []

        @activity(
            retry_policy=RetryPolicy(
                max_attempts=4,
                initial_interval=1.0,
                backoff_coefficient=10.0,  # Would be 1s, 10s, 100s without cap
                max_interval=2.0,  # Cap at 2s
            )
        )
        async def capped_backoff_activity(ctx: WorkflowContext) -> dict:
            attempt_times.append(datetime.now(UTC))
            raise ValueError("Always fails")

        with pytest.raises(RetryExhaustedError):
            await capped_backoff_activity(ctx, activity_id="capped_backoff_activity:1")

        # Verify backoff is capped at max_delay_seconds
        assert len(attempt_times) == 4
        delay1 = (attempt_times[1] - attempt_times[0]).total_seconds()
        delay2 = (attempt_times[2] - attempt_times[1]).total_seconds()
        delay3 = (attempt_times[3] - attempt_times[2]).total_seconds()

        # First delay: 1s (initial_delay_seconds)
        assert 0.9 <= delay1 <= 1.2

        # Second delay: capped at 2s (max_delay_seconds)
        assert 1.9 <= delay2 <= 2.2

        # Third delay: capped at 2s (max_delay_seconds)
        assert 1.9 <= delay3 <= 2.2


class TestRetryMetadataStructure:
    """Test RetryMetadata dataclass and tracking."""

    def test_retry_metadata_creation(self):
        """Test creating RetryMetadata with all fields."""
        metadata = RetryMetadata(
            total_attempts=3,
            total_duration_ms=7500,
            last_error={"type": "ValueError", "message": "Test error"},
            exhausted=False,
        )

        assert metadata.total_attempts == 3
        assert metadata.total_duration_ms == 7500
        assert metadata.last_error["message"] == "Test error"
        assert metadata.exhausted is False

    def test_retry_metadata_to_dict(self):
        """Test RetryMetadata.to_dict() serialization."""
        metadata = RetryMetadata(
            total_attempts=5,
            total_duration_ms=12300,
            last_error={"type": "ConnectionError", "message": "Network timeout"},
            exhausted=True,
        )

        result = metadata.to_dict()

        assert result["total_attempts"] == 5
        assert result["total_duration_ms"] == 12300
        assert result["last_error"]["message"] == "Network timeout"
        assert result["exhausted"] is True

    async def test_retry_metadata_tracks_duration(self, sqlite_storage, create_test_instance):
        """Test that retry metadata accurately tracks total duration."""
        instance_id = "test-policy-006"
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

        @activity(retry_policy=RetryPolicy(max_attempts=3, initial_interval=0.5))
        async def duration_tracking_activity(ctx: WorkflowContext) -> dict:
            raise ValueError("Test error")

        with pytest.raises(RetryExhaustedError):
            await duration_tracking_activity(ctx, activity_id="duration_tracking_activity:1")

        # Verify metadata duration is reasonable (3 attempts, 2 delays of ~0.5s and ~1s)
        history = await sqlite_storage.get_history(instance_id)
        retry_metadata = history[0]["event_data"]["retry_metadata"]

        # Total duration should be approximately 1.5 seconds (0.5s + 1s delays) = 1500ms
        assert 1300 <= retry_metadata["total_duration_ms"] <= 2000


@pytest.mark.asyncio
class TestEddaAppRetryPolicyIntegration:
    """Test EddaApp default_retry_policy integration."""

    async def test_edda_app_accepts_default_retry_policy(self):
        """Test that EddaApp accepts default_retry_policy parameter."""
        custom_policy = RetryPolicy(
            max_attempts=7,
            initial_interval=0.3,
            backoff_coefficient=1.5,
        )

        app = EddaApp(
            db_url="sqlite+aiosqlite:///:memory:",
            service_name="test-service",
            default_retry_policy=custom_policy,
        )

        # Verify app stores the policy
        assert app.default_retry_policy is not None
        assert app.default_retry_policy.max_attempts == 7
        assert app.default_retry_policy.initial_interval == 0.3
        assert app.default_retry_policy.backoff_coefficient == 1.5

    async def test_edda_app_uses_framework_default_when_none(self):
        """Test that EddaApp uses framework default when no policy provided."""
        app = EddaApp(
            db_url="sqlite+aiosqlite:///:memory:",
            service_name="test-service",
            # No default_retry_policy
        )

        # Should use framework default (None means use DEFAULT_RETRY_POLICY)
        assert app.default_retry_policy is None


@pytest.mark.asyncio
class TestBackoffCalculationEdgeCases:
    """Test backoff calculation edge cases."""

    async def test_zero_initial_delay_immediate_retry(self, sqlite_storage, create_test_instance):
        """Test that initial_interval=0 means immediate retry."""
        instance_id = "test-policy-007"
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

        from datetime import UTC, datetime

        attempt_times = []

        @activity(retry_policy=RetryPolicy(max_attempts=3, initial_interval=0.0))
        async def immediate_retry_activity(ctx: WorkflowContext) -> dict:
            attempt_times.append(datetime.now(UTC))
            raise ValueError("Test error")

        with pytest.raises(RetryExhaustedError):
            await immediate_retry_activity(ctx, activity_id="immediate_retry_activity:1")

        # Verify retries are nearly immediate
        assert len(attempt_times) == 3
        delay1 = (attempt_times[1] - attempt_times[0]).total_seconds()
        delay2 = (attempt_times[2] - attempt_times[1]).total_seconds()

        # Should be very small (< 0.1s overhead)
        assert delay1 < 0.1
        assert delay2 < 0.1

    async def test_backoff_multiplier_1_constant_delay(self, sqlite_storage, create_test_instance):
        """Test that backoff_coefficient=1.0 means constant delay."""
        instance_id = "test-policy-008"
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

        from datetime import UTC, datetime

        attempt_times = []

        @activity(
            retry_policy=RetryPolicy(
                max_attempts=4,
                initial_interval=0.5,
                backoff_coefficient=1.0,  # Constant delay
            )
        )
        async def constant_delay_activity(ctx: WorkflowContext) -> dict:
            attempt_times.append(datetime.now(UTC))
            raise ValueError("Test error")

        with pytest.raises(RetryExhaustedError):
            await constant_delay_activity(ctx, activity_id="constant_delay_activity:1")

        # Verify all delays are constant (~0.5s)
        assert len(attempt_times) == 4
        delay1 = (attempt_times[1] - attempt_times[0]).total_seconds()
        delay2 = (attempt_times[2] - attempt_times[1]).total_seconds()
        delay3 = (attempt_times[3] - attempt_times[2]).total_seconds()

        # All delays should be approximately 0.5s
        assert 0.4 <= delay1 <= 0.7
        assert 0.4 <= delay2 <= 0.7
        assert 0.4 <= delay3 <= 0.7
