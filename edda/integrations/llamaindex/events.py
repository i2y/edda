"""Durable events for LlamaIndex Workflow integration.

These events signal to the DurableWorkflow that a durable operation
(sleep or wait for external event) should be performed.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

# Lazy import to avoid requiring llama-index at import time
if TYPE_CHECKING:
    pass


def _import_event_class() -> type[Any]:
    """Import Event class with helpful error message."""
    try:
        from llama_index.core.workflow import Event  # type: ignore[import-not-found]

        return Event  # type: ignore[no-any-return]
    except ImportError as e:
        msg = (
            "llama-index-core is not installed. Install with:\n"
            "  pip install llama-index-core\n"
            "or\n"
            "  pip install 'edda-framework[llamaindex]'"
        )
        raise ImportError(msg) from e


class DurableSleepEvent:
    """
    Event that signals a durable sleep operation.

    When a step returns this event, the DurableWorkflow will:
    1. Record the step completion
    2. Call Edda's sleep() function (durable timer)
    3. Resume with the specified resume_data after the sleep completes

    Example:
        @step
        async def rate_limited_step(self, ctx: Context, ev: SomeEvent) -> DurableSleepEvent:
            # Hit rate limit, need to wait
            return DurableSleepEvent(
                seconds=60,
                resume_data={"retry_count": ev.retry_count + 1},
            )
    """

    def __init__(
        self,
        seconds: float,
        resume_data: dict[str, Any] | None = None,
    ) -> None:
        """
        Initialize a durable sleep event.

        Args:
            seconds: Number of seconds to sleep
            resume_data: Data to include when resuming after sleep
        """
        self.seconds = seconds
        self.resume_data = resume_data or {}

    def to_dict(self) -> dict[str, Any]:
        """Serialize to dictionary."""
        return {
            "_type": "DurableSleepEvent",
            "seconds": self.seconds,
            "resume_data": self.resume_data,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> DurableSleepEvent:
        """Deserialize from dictionary."""
        return cls(
            seconds=data["seconds"],
            resume_data=data.get("resume_data", {}),
        )


class DurableWaitEvent:
    """
    Event that signals waiting for an external event.

    When a step returns this event, the DurableWorkflow will:
    1. Record the step completion
    2. Call Edda's wait_event() function (durable event subscription)
    3. Resume with the received event data after the event arrives

    Example:
        @step
        async def wait_for_approval(self, ctx: Context, ev: OrderEvent) -> DurableWaitEvent:
            return DurableWaitEvent(
                event_type=f"approval.{ev.order_id}",
                timeout_seconds=3600,  # 1 hour timeout
            )
    """

    def __init__(
        self,
        event_type: str,
        timeout_seconds: float | None = None,
    ) -> None:
        """
        Initialize a durable wait event.

        Args:
            event_type: The event type to wait for (e.g., "payment.completed")
            timeout_seconds: Optional timeout in seconds
        """
        self.event_type = event_type
        self.timeout_seconds = timeout_seconds

    def to_dict(self) -> dict[str, Any]:
        """Serialize to dictionary."""
        return {
            "_type": "DurableWaitEvent",
            "event_type": self.event_type,
            "timeout_seconds": self.timeout_seconds,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> DurableWaitEvent:
        """Deserialize from dictionary."""
        return cls(
            event_type=data["event_type"],
            timeout_seconds=data.get("timeout_seconds"),
        )


class ResumeEvent:
    """
    Event used to resume workflow after a durable operation.

    This is an internal event type used by DurableWorkflow to resume
    execution after a DurableSleepEvent or DurableWaitEvent completes.
    """

    def __init__(self, data: dict[str, Any] | None = None) -> None:
        """
        Initialize a resume event.

        Args:
            data: Data from the completed operation (sleep resume_data or received event)
        """
        self.data = data or {}

    def to_dict(self) -> dict[str, Any]:
        """Serialize to dictionary."""
        return {
            "_type": "ResumeEvent",
            "data": self.data,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> ResumeEvent:
        """Deserialize from dictionary."""
        return cls(data=data.get("data", {}))
