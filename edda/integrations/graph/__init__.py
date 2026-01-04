"""
Durable Graph Integration for Edda.

This module provides integration between pydantic-graph and Edda's durable
execution framework, making pydantic-graph execution crash-recoverable and
supporting durable wait operations.

Example:
    from dataclasses import dataclass
    from pydantic_graph import BaseNode, Graph, End
    from edda import workflow, WorkflowContext
    from edda.integrations.graph import DurableGraph, DurableGraphContext

    @dataclass
    class MyState:
        counter: int = 0

    @dataclass
    class IncrementNode(BaseNode[MyState, None, int]):
        async def run(self, ctx: DurableGraphContext) -> "CheckNode":
            ctx.state.counter += 1
            return CheckNode()

    @dataclass
    class CheckNode(BaseNode[MyState, None, int]):
        async def run(self, ctx: DurableGraphContext) -> IncrementNode | End[int]:
            if ctx.state.counter >= 5:
                return End(ctx.state.counter)
            return IncrementNode()

    graph = Graph(nodes=[IncrementNode, CheckNode])
    durable = DurableGraph(graph)

    @workflow
    async def counter_workflow(ctx: WorkflowContext) -> int:
        return await durable.run(
            ctx,
            start_node=IncrementNode(),
            state=MyState(),
        )

Installation:
    pip install 'edda-framework[graph]'
"""

from .context import DurableGraphContext
from .exceptions import GraphExecutionError
from .graph import DurableGraph
from .nodes import ReceivedEvent, Sleep, WaitForEvent

__all__ = [
    "DurableGraph",
    "DurableGraphContext",
    "GraphExecutionError",
    "ReceivedEvent",
    "Sleep",
    "WaitForEvent",
]
