"""Marker nodes for durable graph operations.

These are special marker classes that tell DurableGraph to perform
workflow-level operations (wait_event, sleep) outside of activities.

This design keeps activities pure (atomic, retryable units of work)
while allowing graphs to wait for external events or sleep.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Generic, TypeVar

NextT = TypeVar("NextT")


@dataclass
class WaitForEvent(Generic[NextT]):
    """
    Marker that tells DurableGraph to wait for an external event.

    When a node returns this marker, DurableGraph will:
    1. Complete the current node's activity
    2. Call wait_event() at the workflow level (outside activities)
    3. Store the received event data in ctx.last_event
    4. Continue execution with next_node

    Example:
        @dataclass
        class WaitForPaymentNode(BaseNode[OrderState, None, str]):
            async def run(self, ctx: DurableGraphContext) -> WaitForEvent[ProcessPaymentNode]:
                # Do some processing...
                ctx.state.waiting_for = "payment"

                # Return marker to wait for event
                return WaitForEvent(
                    event_type=f"payment.{ctx.state.order_id}",
                    next_node=ProcessPaymentNode(),
                    timeout_seconds=3600,
                )

        @dataclass
        class ProcessPaymentNode(BaseNode[OrderState, None, str]):
            async def run(self, ctx: DurableGraphContext) -> End[str]:
                # Access the event that was received
                event = ctx.last_event
                if event.data.get("status") == "success":
                    return End("payment_received")
                return End("payment_failed")
    """

    event_type: str
    next_node: NextT
    timeout_seconds: int | None = None


@dataclass
class Sleep(Generic[NextT]):
    """
    Marker that tells DurableGraph to sleep before continuing.

    When a node returns this marker, DurableGraph will:
    1. Complete the current node's activity
    2. Call sleep() at the workflow level (outside activities)
    3. Continue execution with next_node

    Example:
        @dataclass
        class RateLimitNode(BaseNode[ApiState, None, str]):
            async def run(self, ctx: DurableGraphContext) -> Sleep[RetryApiNode]:
                # Hit rate limit, need to wait
                return Sleep(
                    seconds=60,
                    next_node=RetryApiNode(),
                )
    """

    seconds: int
    next_node: NextT


@dataclass
class ReceivedEvent:
    """
    Event data received from wait_event.

    This is stored in DurableGraphContext.last_event after WaitForEvent completes.
    """

    event_type: str
    data: dict[str, Any] = field(default_factory=dict)
    metadata: dict[str, Any] = field(default_factory=dict)
