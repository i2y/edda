"""
Retry policy module for Edda framework.

This module provides retry configuration and metadata tracking for activities.
Inspired by Restate's retry mechanism and Temporal's retry policies.
"""

import time
from dataclasses import dataclass, field
from typing import Any


@dataclass
class RetryPolicy:
    """
    Retry policy configuration for activities.

    Inspired by Restate's retry mechanism with Edda-specific optimizations.

    Attributes:
        initial_interval: First retry delay in seconds
        backoff_coefficient: Exponential backoff multiplier
        max_interval: Maximum retry delay in seconds (caps exponential growth)
        max_attempts: Maximum retry attempts (None = infinite, use with caution)
        max_duration: Maximum total retry duration in seconds (None = infinite)
        retryable_error_types: Tuple of exception types to retry
        non_retryable_error_types: Tuple of exception types to never retry

    Example:
        # Default policy (5 attempts, exponential backoff)
        policy = RetryPolicy()

        # Custom policy
        policy = RetryPolicy(
            initial_interval=0.5,
            backoff_coefficient=1.5,
            max_attempts=10,
            max_duration=120.0,
        )

        # Infinite retry (Restate-style, use with caution)
        policy = RetryPolicy(max_attempts=None, max_duration=None)
    """

    # Backoff parameters
    initial_interval: float = 1.0  # seconds
    backoff_coefficient: float = 2.0  # exponential multiplier
    max_interval: float = 60.0  # seconds (cap exponential growth)

    # Retry limits
    max_attempts: int | None = 5  # None = infinite (Restate-style)
    max_duration: float | None = 300.0  # seconds (5 minutes), None = infinite

    # Exception filtering
    retryable_error_types: tuple[type[Exception], ...] = (Exception,)
    non_retryable_error_types: tuple[type[Exception], ...] = ()

    def is_retryable(self, error: Exception) -> bool:
        """
        Determine if an error is retryable.

        Priority:
        1. TerminalError -> always non-retryable
        2. non_retryable_error_types -> non-retryable
        3. retryable_error_types -> retryable
        4. Default: non-retryable (safe default)

        Args:
            error: Exception to check

        Returns:
            True if error should be retried, False otherwise
        """
        # Import here to avoid circular dependency
        from edda.exceptions import TerminalError

        # TerminalError always stops retry
        if isinstance(error, TerminalError):
            return False

        # Check explicit non-retryable types
        if self.non_retryable_error_types and isinstance(error, self.non_retryable_error_types):
            return False

        # Check explicit retryable types (default: non-retryable)
        return bool(self.retryable_error_types and isinstance(error, self.retryable_error_types))

    def calculate_delay(self, attempt: int) -> float:
        """
        Calculate backoff delay for given attempt number.

        Formula: delay = initial_interval * (backoff_coefficient ^ (attempt - 1))
        Capped at max_interval to prevent excessive delays.

        Args:
            attempt: Current attempt number (1-indexed)

        Returns:
            Delay in seconds (exponential backoff, capped at max_interval)

        Example:
            # Default policy: initial=1.0, coefficient=2.0, max=60.0
            # Attempt 1: 1.0s
            # Attempt 2: 2.0s
            # Attempt 3: 4.0s
            # Attempt 4: 8.0s
            # Attempt 5: 16.0s
            # Attempt 6: 32.0s
            # Attempt 7: 60.0s (capped)
            # Attempt 8: 60.0s (capped)
        """
        delay = self.initial_interval * (self.backoff_coefficient ** (attempt - 1))
        return min(delay, self.max_interval)


@dataclass
class RetryMetadata:
    """
    Track retry attempts for observability.

    This metadata is stored in workflow history for debugging and monitoring.

    Attributes:
        total_attempts: Total number of attempts made
        total_duration_ms: Total time spent retrying (milliseconds)
        exhausted: Whether max retries were reached
        errors: List of error information for each attempt
        last_error: Information about the last error encountered
    """

    total_attempts: int = 0
    total_duration_ms: int = 0
    exhausted: bool = False
    errors: list[dict[str, Any]] = field(default_factory=list)
    last_error: dict[str, Any] | None = None

    def add_attempt(self, attempt: int, error: Exception) -> None:
        """
        Record a failed attempt.

        Args:
            attempt: Attempt number (1-indexed)
            error: Exception that caused the failure
        """
        self.total_attempts = attempt
        error_info = {
            "attempt": attempt,
            "error_type": type(error).__name__,
            "message": str(error),
            "timestamp_ms": int(time.time() * 1000),
        }
        self.errors.append(error_info)
        self.last_error = {
            "error_type": type(error).__name__,
            "message": str(error),
        }

    def to_dict(self) -> dict[str, Any]:
        """
        Convert to JSON-serializable dict for storage.

        Returns:
            Dictionary representation of retry metadata
        """
        return {
            "total_attempts": self.total_attempts,
            "total_duration_ms": self.total_duration_ms,
            "exhausted": self.exhausted,
            "errors": self.errors,
            "last_error": self.last_error,
        }


# Default retry policy
DEFAULT_RETRY_POLICY = RetryPolicy(
    initial_interval=1.0,  # Start with 1 second delay
    backoff_coefficient=2.0,  # Standard exponential backoff
    max_interval=60.0,  # Cap at 60 seconds
    max_attempts=5,  # Balance between resilience and fail-fast
    max_duration=300.0,  # 5 minutes total (prevents runaway retry)
)


# Preset policies for common scenarios
AGGRESSIVE_RETRY = RetryPolicy(
    initial_interval=0.1,  # Fast retries for low-latency services
    backoff_coefficient=1.5,  # Slower exponential growth
    max_interval=10.0,  # Short max delay
    max_attempts=10,  # More attempts
    max_duration=60.0,  # 1 minute total
)

CONSERVATIVE_RETRY = RetryPolicy(
    initial_interval=5.0,  # Wait longer between attempts
    backoff_coefficient=2.0,  # Standard exponential
    max_interval=300.0,  # Up to 5 minutes between retries
    max_attempts=3,  # Fewer attempts (fail faster)
    max_duration=900.0,  # 15 minutes total
)

INFINITE_RETRY = RetryPolicy(
    initial_interval=1.0,
    backoff_coefficient=2.0,
    max_interval=60.0,
    max_attempts=None,  # Infinite attempts (Restate-style)
    max_duration=None,  # Infinite duration (use with caution)
)
