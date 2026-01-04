"""DurableGraph - makes pydantic-graph execution durable via Edda."""

from __future__ import annotations

import dataclasses
import importlib
from typing import TYPE_CHECKING, Any, Generic, TypeVar

from edda.activity import activity
from edda.pydantic_utils import to_json_dict

from .context import DurableGraphContext
from .exceptions import GraphExecutionError
from .nodes import ReceivedEvent, Sleep, WaitForEvent

if TYPE_CHECKING:
    from edda.context import WorkflowContext

StateT = TypeVar("StateT")
DepsT = TypeVar("DepsT")
RunEndT = TypeVar("RunEndT")


def _import_pydantic_graph() -> Any:
    """Import pydantic_graph with helpful error message."""
    try:
        import pydantic_graph

        return pydantic_graph
    except ImportError as e:
        msg = (
            "pydantic-graph is not installed. Install with:\n"
            "  pip install pydantic-graph\n"
            "or\n"
            "  pip install 'edda-framework[graph]'"
        )
        raise ImportError(msg) from e


def _get_class_path(cls: type) -> str:
    """Get fully qualified class path for serialization."""
    return f"{cls.__module__}:{cls.__qualname__}"


def _import_class(path: str) -> type:
    """Import a class from its fully qualified path."""
    module_path, class_name = path.rsplit(":", 1)
    module = importlib.import_module(module_path)
    return getattr(module, class_name)  # type: ignore[no-any-return]


def _serialize_node(node: Any) -> dict[str, Any]:
    """Serialize a node to a dict."""
    if dataclasses.is_dataclass(node) and not isinstance(node, type):
        return {
            "_class_path": _get_class_path(node.__class__),
            "_data": dataclasses.asdict(node),
        }
    return {
        "_class_path": _get_class_path(node.__class__),
        "_data": {},
    }


def _deserialize_node(data: dict[str, Any]) -> Any:
    """Deserialize a node from a dict."""
    cls = _import_class(data["_class_path"])
    return cls(**data.get("_data", {}))


def _serialize_state(state: Any) -> dict[str, Any]:
    """Serialize state to a dict."""
    if state is None:
        return {"_none": True}
    if dataclasses.is_dataclass(state) and not isinstance(state, type):
        return {
            "_class_path": _get_class_path(state.__class__),
            "_data": dataclasses.asdict(state),
        }
    if hasattr(state, "model_dump"):
        return {
            "_class_path": _get_class_path(state.__class__),
            "_data": state.model_dump(),
        }
    return {"_raw": str(state)}


def _serialize_deps(deps: Any) -> dict[str, Any] | None:
    """Serialize deps to a dict."""
    if deps is None:
        return None
    if dataclasses.is_dataclass(deps) and not isinstance(deps, type):
        return {
            "_class_path": _get_class_path(deps.__class__),
            "_data": dataclasses.asdict(deps),
        }
    if hasattr(deps, "model_dump"):
        return {
            "_class_path": _get_class_path(deps.__class__),
            "_data": deps.model_dump(),
        }
    # For simple types (int, str, etc.), return as-is wrapped
    return {"_value": deps}


def _deserialize_deps(data: dict[str, Any] | None) -> Any:
    """Deserialize deps from a dict."""
    if data is None:
        return None
    if "_value" in data:
        return data["_value"]
    cls = _import_class(data["_class_path"])
    if dataclasses.is_dataclass(cls):
        return cls(**data["_data"])
    if hasattr(cls, "model_validate"):
        return cls.model_validate(data["_data"])
    return cls(**data["_data"])


def _deserialize_state(data: dict[str, Any]) -> Any:
    """Deserialize state from a dict."""
    if data.get("_none"):
        return None
    if "_raw" in data:
        raise ValueError(f"Cannot deserialize state from raw: {data['_raw']}")
    cls = _import_class(data["_class_path"])
    if dataclasses.is_dataclass(cls):
        return cls(**data["_data"])
    if hasattr(cls, "model_validate"):
        return cls.model_validate(data["_data"])
    return cls(**data["_data"])


def _restore_state(source: Any, target: Any) -> None:
    """Copy state from source to target object."""
    if dataclasses.is_dataclass(source) and not isinstance(source, type):
        for field in dataclasses.fields(source):
            setattr(target, field.name, getattr(source, field.name))
    elif hasattr(source, "__dict__"):
        for key, value in source.__dict__.items():
            if not key.startswith("_"):
                setattr(target, key, value)


@activity
async def _run_graph_node(
    ctx: WorkflowContext,
    node_data: dict[str, Any],
    state_data: dict[str, Any],
    deps_data: dict[str, Any] | None,
    last_event_data: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """
    Execute a single graph node as a durable activity.

    This activity is the core of DurableGraph - it runs one node and returns
    the serialized result (next node, End, WaitForEvent, or Sleep) along
    with the updated state.
    """
    pg = _import_pydantic_graph()

    # Deserialize node, state, and deps
    node = _deserialize_node(node_data)
    state = _deserialize_state(state_data)
    deps = _deserialize_deps(deps_data)

    # Reconstruct last_event if provided
    last_event: ReceivedEvent | None = None
    if last_event_data:
        last_event = ReceivedEvent(
            event_type=last_event_data.get("event_type", ""),
            data=last_event_data.get("data", {}),
            metadata=last_event_data.get("metadata", {}),
        )

    # Create durable context
    durable_ctx = DurableGraphContext(
        _state=state,
        _deps=deps,
        workflow_ctx=ctx,
        last_event=last_event,
    )

    try:
        # Execute the node
        result = await node.run(durable_ctx)

        # Serialize result based on type
        if isinstance(result, pg.End):
            return {
                "_type": "End",
                "_data": to_json_dict(result.data),
                "_state": _serialize_state(state),
            }
        elif isinstance(result, WaitForEvent):
            return {
                "_type": "WaitForEvent",
                "_event_type": result.event_type,
                "_timeout_seconds": result.timeout_seconds,
                "_next_node": _serialize_node(result.next_node),
                "_state": _serialize_state(state),
            }
        elif isinstance(result, Sleep):
            return {
                "_type": "Sleep",
                "_seconds": result.seconds,
                "_next_node": _serialize_node(result.next_node),
                "_state": _serialize_state(state),
            }
        else:
            # Regular node transition
            return {
                "_type": "Node",
                "_node": _serialize_node(result),
                "_state": _serialize_state(state),
            }

    except Exception as e:
        raise GraphExecutionError(
            f"Node {node.__class__.__name__} failed: {e}",
            node.__class__.__name__,
        ) from e


class DurableGraph(Generic[StateT, DepsT, RunEndT]):
    """
    Wrapper that makes pydantic-graph execution durable.

    DurableGraph wraps a pydantic-graph Graph and executes it with Edda's
    durability guarantees:

    - Each node execution is recorded as an Edda Activity
    - On replay, completed nodes return cached results (no re-execution)
    - Crash recovery: workflows resume from the last completed node
    - WaitForEvent/Sleep markers enable durable wait operations

    Example:
        from dataclasses import dataclass
        from pydantic_graph import BaseNode, Graph, End
        from edda import workflow, WorkflowContext
        from edda.integrations.graph import (
            DurableGraph,
            DurableGraphContext,
            WaitForEvent,
        )

        @dataclass
        class OrderState:
            order_id: str | None = None

        @dataclass
        class ProcessOrder(BaseNode[OrderState, None, str]):
            order_id: str

            async def run(self, ctx: DurableGraphContext) -> WaitForEvent[WaitPayment]:
                ctx.state.order_id = self.order_id
                return WaitForEvent(
                    event_type="payment.completed",
                    next_node=WaitPayment(),
                )

        @dataclass
        class WaitPayment(BaseNode[OrderState, None, str]):
            async def run(self, ctx: DurableGraphContext) -> End[str]:
                # Access the received event
                event = ctx.last_event
                if event and event.data.get("status") == "success":
                    return End("completed")
                return End("failed")

        graph = Graph(nodes=[ProcessOrder, WaitPayment])
        durable = DurableGraph(graph)

        @workflow
        async def order_workflow(ctx: WorkflowContext, order_id: str) -> str:
            return await durable.run(
                ctx,
                start_node=ProcessOrder(order_id=order_id),
                state=OrderState(),
            )
    """

    def __init__(self, graph: Any) -> None:
        """
        Initialize DurableGraph wrapper.

        Args:
            graph: A pydantic-graph Graph instance

        Raises:
            TypeError: If graph is not a pydantic-graph Graph instance
        """
        pg = _import_pydantic_graph()
        if not isinstance(graph, pg.Graph):
            raise TypeError(f"Expected pydantic_graph.Graph, got {type(graph).__name__}")
        self._graph = graph

    @property
    def graph(self) -> Any:
        """Get the underlying pydantic-graph Graph instance."""
        return self._graph

    async def run(
        self,
        ctx: WorkflowContext,
        start_node: Any,
        *,
        state: StateT,
        deps: DepsT = None,  # type: ignore[assignment]
    ) -> RunEndT:
        """
        Execute the graph durably with Edda crash recovery.

        Args:
            ctx: Edda WorkflowContext
            start_node: The initial node to start execution from
            state: Initial graph state (will be mutated during execution)
            deps: Optional dependencies accessible via ctx.deps

        Returns:
            The final result (End.data value)

        Raises:
            GraphExecutionError: If graph execution fails
        """
        from edda.channels import sleep as edda_sleep
        from edda.channels import wait_event as edda_wait_event

        current_node = start_node
        last_event_data: dict[str, Any] | None = None

        # Execute nodes until End is reached
        while True:
            # Serialize inputs
            node_data = _serialize_node(current_node)
            state_data = _serialize_state(state)
            deps_data = _serialize_deps(deps)

            # Run node as activity (handles replay/caching automatically)
            # The @activity decorator transforms the function signature
            result = await _run_graph_node(  # type: ignore[misc,call-arg]
                ctx,  # type: ignore[arg-type]
                node_data,
                state_data,
                deps_data,
                last_event_data,
            )

            # Restore state from result
            restored_state = _deserialize_state(result["_state"])
            _restore_state(restored_state, state)

            # Handle result based on type
            if result["_type"] == "End":
                return result["_data"]  # type: ignore[no-any-return]

            elif result["_type"] == "WaitForEvent":
                # Wait for event at workflow level (outside activity)
                event = await edda_wait_event(
                    ctx,
                    result["_event_type"],
                    timeout_seconds=result.get("_timeout_seconds"),
                )
                # Store event data for next node
                # Note: edda.channels.ReceivedEvent uses 'type' not 'event_type'
                last_event_data = {
                    "event_type": getattr(event, "type", result["_event_type"]),
                    "data": event.data if isinstance(event.data, dict) else {},
                    "metadata": getattr(event, "extensions", {}),
                }
                # Move to next node
                current_node = _deserialize_node(result["_next_node"])

            elif result["_type"] == "Sleep":
                # Sleep at workflow level (outside activity)
                await edda_sleep(ctx, result["_seconds"])
                # Clear last_event since this wasn't an event wait
                last_event_data = None
                # Move to next node
                current_node = _deserialize_node(result["_next_node"])

            else:
                # Regular node transition
                last_event_data = None  # Clear last_event for regular transitions
                current_node = _deserialize_node(result["_node"])
