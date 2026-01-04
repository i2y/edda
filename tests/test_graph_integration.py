"""Tests for durable graph integration."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass

import pytest

from edda import WorkflowContext, workflow
from edda.replay import ReplayEngine
from edda.workflow import set_replay_engine

# Skip all tests if pydantic-graph is not installed
pytest.importorskip("pydantic_graph")

from pydantic_graph import BaseNode, End, Graph  # noqa: E402

from edda.integrations.graph import (  # noqa: E402
    DurableGraph,
    DurableGraphContext,
)

# =============================================================================
# Test Fixtures
# =============================================================================


@pytest.fixture
def replay_engine(sqlite_storage):
    """Create and configure ReplayEngine."""
    engine = ReplayEngine(
        storage=sqlite_storage,
        service_name="test-graph-service",
        worker_id="worker-graph-test",
    )
    set_replay_engine(engine)
    return engine


# =============================================================================
# Simple Graph Tests
# =============================================================================


@dataclass
class CounterState:
    """State for counter graph."""

    value: int = 0


@dataclass
class IncrementNode(BaseNode[CounterState, None, int]):
    """Node that increments the counter."""

    amount: int = 1

    async def run(self, ctx: DurableGraphContext) -> CheckNode:
        ctx.state.value += self.amount
        return CheckNode()


@dataclass
class CheckNode(BaseNode[CounterState, None, int]):
    """Node that checks if we should continue."""

    target: int = 5

    async def run(self, ctx: DurableGraphContext) -> IncrementNode | End[int]:
        if ctx.state.value >= self.target:
            return End(ctx.state.value)
        return IncrementNode()


@dataclass
class BigIncrementNode2(BaseNode[CounterState, None, int]):
    """Node that increments by 2."""

    amount: int = 2

    async def run(self, ctx: DurableGraphContext) -> BigCheckNode2:
        ctx.state.value += self.amount
        return BigCheckNode2()


@dataclass
class BigCheckNode2(BaseNode[CounterState, None, int]):
    """Check node for big increment test."""

    async def run(self, ctx: DurableGraphContext) -> BigIncrementNode2 | End[int]:
        if ctx.state.value >= 5:
            return End(ctx.state.value)
        return BigIncrementNode2()


class TestDurableGraphBasic:
    """Basic tests for DurableGraph."""

    async def test_simple_graph_execution(self, replay_engine, sqlite_storage):
        """Test that a simple graph executes correctly."""
        graph = Graph(nodes=[IncrementNode, CheckNode])
        durable = DurableGraph(graph)

        @workflow
        async def counter_workflow(ctx: WorkflowContext) -> int:
            return await durable.run(
                ctx,
                start_node=IncrementNode(),
                state=CounterState(),
            )

        instance_id = await counter_workflow.start()

        # Wait for completion
        await asyncio.sleep(0.1)

        instance = await sqlite_storage.get_instance(instance_id)
        assert instance is not None
        assert instance["status"] == "completed"
        assert instance["output_data"]["result"] == 5

    async def test_graph_with_custom_start_value(self, replay_engine, sqlite_storage):
        """Test graph execution with custom initial state."""
        graph = Graph(nodes=[IncrementNode, CheckNode])
        durable = DurableGraph(graph)

        @workflow
        async def counter_workflow_v2(ctx: WorkflowContext, start_value: int) -> int:
            return await durable.run(
                ctx,
                start_node=IncrementNode(),
                state=CounterState(value=start_value),
            )

        instance_id = await counter_workflow_v2.start(start_value=3)

        await asyncio.sleep(0.1)

        instance = await sqlite_storage.get_instance(instance_id)
        assert instance is not None
        assert instance["status"] == "completed"
        # Starting from 3, needs 2 more increments to reach 5
        assert instance["output_data"]["result"] == 5

    async def test_graph_with_larger_increment(self, replay_engine, sqlite_storage):
        """Test graph with custom increment amount."""
        # Use module-level BigIncrementNode2/BigCheckNode2 classes
        graph = Graph(nodes=[BigIncrementNode2, BigCheckNode2])
        durable = DurableGraph(graph)

        @workflow
        async def counter_workflow_v3(ctx: WorkflowContext) -> int:
            return await durable.run(
                ctx,
                start_node=BigIncrementNode2(),
                state=CounterState(),
                deps=None,
            )

        instance_id = await counter_workflow_v3.start()

        await asyncio.sleep(0.1)

        instance = await sqlite_storage.get_instance(instance_id)
        assert instance is not None
        assert instance["status"] == "completed"
        # 0 + 2 + 2 + 2 = 6 (>= 5)
        assert instance["output_data"]["result"] == 6


# =============================================================================
# State and Deps Tests
# =============================================================================


@dataclass
class ComputeState:
    """State for compute graph."""

    results: list[int] | None = None

    def __post_init__(self):
        if self.results is None:
            self.results = []


@dataclass
class ComputeDeps:
    """Dependencies for compute graph."""

    multiplier: int = 2


@dataclass
class ComputeNode(BaseNode[ComputeState, ComputeDeps, list[int]]):
    """Node that computes and stores a value."""

    input_value: int

    async def run(self, ctx: DurableGraphContext) -> FinalizeNode:
        result = self.input_value * ctx.deps.multiplier
        ctx.state.results.append(result)
        return FinalizeNode()


@dataclass
class FinalizeNode(BaseNode[ComputeState, ComputeDeps, list[int]]):
    """Node that finalizes computation."""

    async def run(self, ctx: DurableGraphContext) -> End[list[int]]:
        return End(ctx.state.results)


class TestDurableGraphWithDeps:
    """Test DurableGraph with dependencies."""

    async def test_graph_with_deps(self, replay_engine, sqlite_storage):
        """Test that deps are accessible in nodes."""
        graph = Graph(nodes=[ComputeNode, FinalizeNode])
        durable = DurableGraph(graph)

        @workflow
        async def compute_workflow(ctx: WorkflowContext, value: int) -> list[int]:
            return await durable.run(
                ctx,
                start_node=ComputeNode(input_value=value),
                state=ComputeState(),
                deps=ComputeDeps(multiplier=3),
            )

        instance_id = await compute_workflow.start(value=7)

        await asyncio.sleep(0.1)

        instance = await sqlite_storage.get_instance(instance_id)
        assert instance is not None
        assert instance["status"] == "completed"
        # 7 * 3 = 21
        assert instance["output_data"]["result"] == [21]


# =============================================================================
# Error Handling Tests
# =============================================================================


@dataclass
class ErrorState:
    """State for error test graph."""

    pass


@dataclass
class FailingNode(BaseNode[ErrorState, None, str]):
    """Node that raises an error."""

    async def run(self, ctx: DurableGraphContext) -> End[str]:
        raise ValueError("Intentional error")


class TestDurableGraphErrors:
    """Test error handling in DurableGraph."""

    async def test_node_error_propagates(self, replay_engine, sqlite_storage):
        """Test that node errors propagate up."""
        graph = Graph(nodes=[FailingNode])
        durable = DurableGraph(graph)

        @workflow
        async def error_workflow(ctx: WorkflowContext) -> str:
            return await durable.run(
                ctx,
                start_node=FailingNode(),
                state=ErrorState(),
            )

        with pytest.raises(Exception) as exc_info:
            await error_workflow.start()
        # The error should contain "Intentional error" somewhere in the chain
        assert "Intentional error" in str(exc_info.value) or (
            exc_info.value.__cause__ and "Intentional error" in str(exc_info.value.__cause__)
        )

    def test_invalid_graph_type(self):
        """Test that non-Graph objects raise TypeError."""
        with pytest.raises(TypeError, match="Expected pydantic_graph.Graph"):
            DurableGraph("not a graph")


# =============================================================================
# Replay Tests
# =============================================================================


@dataclass
class ReplayState:
    """State for replay test graph."""

    execution_count: int = 0
    node_executions: list[str] | None = None

    def __post_init__(self):
        if self.node_executions is None:
            self.node_executions = []


@dataclass
class FirstNode(BaseNode[ReplayState, None, dict]):
    """First node in replay test."""

    async def run(self, ctx: DurableGraphContext) -> SecondNode:
        ctx.state.execution_count += 1
        ctx.state.node_executions.append("first")
        return SecondNode()


@dataclass
class SecondNode(BaseNode[ReplayState, None, dict]):
    """Second node in replay test."""

    async def run(self, ctx: DurableGraphContext) -> End[dict]:
        ctx.state.execution_count += 1
        ctx.state.node_executions.append("second")
        return End(
            {
                "count": ctx.state.execution_count,
                "nodes": ctx.state.node_executions,
            }
        )


class TestDurableGraphReplay:
    """Test replay behavior of DurableGraph."""

    async def test_activity_history_recorded(self, replay_engine, sqlite_storage):
        """Test that node executions are recorded in history."""
        graph = Graph(nodes=[FirstNode, SecondNode])
        durable = DurableGraph(graph)

        @workflow
        async def replay_test_workflow(ctx: WorkflowContext) -> dict:
            return await durable.run(
                ctx,
                start_node=FirstNode(),
                state=ReplayState(),
            )

        instance_id = await replay_test_workflow.start()

        await asyncio.sleep(0.1)

        # Check history was recorded
        history = await sqlite_storage.get_history(instance_id)
        activity_events = [e for e in history if e["event_type"] == "ActivityCompleted"]

        # Should have 2 activity completions (one per node)
        assert len(activity_events) == 2

        # All activities use the same function name (_run_graph_node)
        activity_names = [e["event_data"]["activity_name"] for e in activity_events]
        assert all(name == "_run_graph_node" for name in activity_names)


# =============================================================================
# Context Access Tests
# =============================================================================


@dataclass
class ContextTestState:
    """State for context test graph."""

    instance_id: str | None = None
    is_replaying: bool | None = None


@dataclass
class ContextCheckNode(BaseNode[ContextTestState, None, dict]):
    """Node that checks context properties."""

    async def run(self, ctx: DurableGraphContext) -> End[dict]:
        ctx.state.instance_id = ctx.instance_id
        ctx.state.is_replaying = ctx.is_replaying
        return End(
            {
                "instance_id": ctx.instance_id,
                "is_replaying": ctx.is_replaying,
            }
        )


class TestDurableGraphContext:
    """Test DurableGraphContext functionality."""

    async def test_context_properties_accessible(self, replay_engine, sqlite_storage):
        """Test that context properties are accessible in nodes."""
        graph = Graph(nodes=[ContextCheckNode])
        durable = DurableGraph(graph)

        @workflow
        async def context_test_workflow(ctx: WorkflowContext) -> dict:
            return await durable.run(
                ctx,
                start_node=ContextCheckNode(),
                state=ContextTestState(),
            )

        instance_id = await context_test_workflow.start()

        await asyncio.sleep(0.1)

        instance = await sqlite_storage.get_instance(instance_id)
        assert instance is not None
        assert instance["status"] == "completed"

        result = instance["output_data"]["result"]
        assert result["instance_id"] == instance_id
        assert result["is_replaying"] is False
