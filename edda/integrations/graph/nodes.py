"""Marker nodes for durable graph operations.

These are special marker classes that tell DurableGraph to perform
workflow-level operations (wait_event, sleep) outside of activities.

This design keeps activities pure (atomic, retryable units of work)
while allowing graphs to wait for external events or sleep.

These classes inherit from pydantic-graph's BaseNode so they can be:
1. Included in return type annotations without type: ignore
2. Registered in Graph for proper type validation
3. Detected by graph visualization tools
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Generic, TypeVar

NextT = TypeVar("NextT")

# Try to import BaseNode for inheritance
# If pydantic-graph is not installed, use a fallback
try:
    from pydantic_graph import BaseNode

    _HAS_PYDANTIC_GRAPH = True
except ImportError:
    _HAS_PYDANTIC_GRAPH = False

    # Fallback base class when pydantic-graph is not installed
    class BaseNode:  # type: ignore[no-redef]
        pass


if TYPE_CHECKING:
    from pydantic_graph import BaseNode as _BaseNode

    # For type checking, we need the actual BaseNode
    _MarkerBase = _BaseNode[Any, Any, Any]
else:
    _MarkerBase = BaseNode


@dataclass
class WaitForEvent(_MarkerBase, Generic[NextT]):  # type: ignore[misc,valid-type]
    """
    Marker node that tells DurableGraph to wait for an external event.

    When a node returns this marker, DurableGraph will:
    1. Complete the current node's activity
    2. Call wait_event() at the workflow level (outside activities)
    3. Store the received event data in ctx.last_event
    4. Continue execution with next_node

    IMPORTANT: Register this class in your Graph for type checking:
        graph = Graph(nodes=[MyNode1, MyNode2, WaitForEvent])

    Example:
        @dataclass
        class WaitForPaymentNode(BaseNode[OrderState, None, str]):
            async def run(
                self, ctx: DurableGraphContext
            ) -> WaitForEvent[ProcessPaymentNode] | End[str]:
                ctx.state.waiting_for = "payment"
                return WaitForEvent(
                    event_type=f"payment.{ctx.state.order_id}",
                    next_node=ProcessPaymentNode(),
                    timeout_seconds=3600,
                )

        @dataclass
        class ProcessPaymentNode(BaseNode[OrderState, None, str]):
            async def run(self, ctx: DurableGraphContext) -> End[str]:
                event = ctx.last_event
                if event.data.get("status") == "success":
                    return End("payment_received")
                return End("payment_failed")

        # Register WaitForEvent in the Graph
        graph = Graph(nodes=[WaitForPaymentNode, ProcessPaymentNode, WaitForEvent])
    """

    event_type: str
    next_node: NextT
    timeout_seconds: int | None = None

    async def run(self, _ctx: Any) -> Any:
        """Never called - DurableGraph intercepts this marker."""
        raise RuntimeError(
            "WaitForEvent marker should not be executed directly. "
            "Use DurableGraph.run() instead of Graph.run()."
        )


@dataclass
class Sleep(_MarkerBase, Generic[NextT]):  # type: ignore[misc,valid-type]
    """
    Marker node that tells DurableGraph to sleep before continuing.

    When a node returns this marker, DurableGraph will:
    1. Complete the current node's activity
    2. Call sleep() at the workflow level (outside activities)
    3. Continue execution with next_node

    IMPORTANT: Register this class in your Graph for type checking:
        graph = Graph(nodes=[MyNode1, MyNode2, Sleep])

    Example:
        @dataclass
        class RateLimitNode(BaseNode[ApiState, None, str]):
            async def run(
                self, ctx: DurableGraphContext
            ) -> Sleep[RetryApiNode] | End[str]:
                if rate_limited:
                    return Sleep(seconds=60, next_node=RetryApiNode())
                return End("success")

        # Register Sleep in the Graph
        graph = Graph(nodes=[RateLimitNode, RetryApiNode, Sleep])
    """

    seconds: int
    next_node: NextT

    async def run(self, _ctx: Any) -> Any:
        """Never called - DurableGraph intercepts this marker."""
        raise RuntimeError(
            "Sleep marker should not be executed directly. "
            "Use DurableGraph.run() instead of Graph.run()."
        )


@dataclass
class ReceivedEvent:
    """
    Event data received from wait_event.

    This is stored in DurableGraphContext.last_event after WaitForEvent completes.
    """

    event_type: str
    data: dict[str, Any] = field(default_factory=dict)
    metadata: dict[str, Any] = field(default_factory=dict)
