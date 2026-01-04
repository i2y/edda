"""DurableGraphContext - bridges pydantic-graph and Edda contexts."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Generic, TypeVar

if TYPE_CHECKING:
    from edda.context import WorkflowContext

    from .nodes import ReceivedEvent

StateT = TypeVar("StateT")
DepsT = TypeVar("DepsT")


@dataclass
class DurableGraphContext(Generic[StateT, DepsT]):
    """
    Context that bridges pydantic-graph and Edda.

    Provides access to:
    - pydantic-graph's state and deps via properties
    - last_event: The most recent event received via WaitForEvent

    This context is passed to node's run() method when executing
    via DurableGraph.

    For durable wait operations (wait_event, sleep), use the WaitForEvent
    and Sleep marker nodes instead of calling methods directly:

        from edda.integrations.graph import WaitForEvent, Sleep

        @dataclass
        class MyNode(BaseNode[MyState, None, str]):
            async def run(self, ctx: DurableGraphContext) -> WaitForEvent[NextNode]:
                # Return a marker to wait for an event
                return WaitForEvent(
                    event_type="payment.completed",
                    next_node=NextNode(),
                    timeout_seconds=3600,
                )

        @dataclass
        class NextNode(BaseNode[MyState, None, str]):
            async def run(self, ctx: DurableGraphContext) -> End[str]:
                # Access the received event
                event = ctx.last_event
                return End(event.data.get("status", "unknown"))

    Attributes:
        state: The graph state object (mutable, shared across nodes)
        deps: The dependencies object (immutable)
        last_event: The most recent event received via WaitForEvent (or None)
        workflow_ctx: The Edda WorkflowContext
    """

    _state: StateT
    _deps: DepsT
    workflow_ctx: WorkflowContext
    last_event: ReceivedEvent | None = field(default=None)

    @property
    def state(self) -> StateT:
        """Get the graph state object."""
        return self._state

    @property
    def deps(self) -> DepsT:
        """Get the dependencies object."""
        return self._deps

    @property
    def instance_id(self) -> str:
        """Get the workflow instance ID."""
        return self.workflow_ctx.instance_id

    @property
    def is_replaying(self) -> bool:
        """Check if the workflow is currently replaying."""
        return self.workflow_ctx.is_replaying
