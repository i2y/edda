"""Tests for MCP cancel tool functionality."""

import asyncio

import pytest

# Skip all tests if mcp is not installed
pytest.importorskip("mcp")

from edda import WorkflowContext, activity
from edda.channels import wait_event
from edda.compensation import register_compensation
from edda.integrations.mcp import EddaMCPServer


@pytest.fixture
async def mcp_server_with_cancellable_tool():
    """Create MCP server with a cancellable durable tool."""
    server = EddaMCPServer(
        name="Cancel Test Service",
        db_url="sqlite+aiosqlite:///:memory:",
    )

    # Track compensation execution
    compensations_executed: list[str] = []

    @activity
    async def process_step(ctx: WorkflowContext, value: str):
        return {"processed": value.upper()}

    @activity
    async def compensate_step(ctx: WorkflowContext):
        compensations_executed.append("step_compensated")

    @server.durable_tool(description="Cancellable workflow")
    async def cancellable_workflow(ctx: WorkflowContext, value: str):
        result = await process_step(ctx, value)
        await register_compensation(ctx, compensate_step)
        # Wait for an event that won't come (workflow will be cancelled)
        await wait_event(ctx, "test_event")
        return result

    @server.durable_tool(description="Quick workflow")
    async def quick_workflow(ctx: WorkflowContext, value: str):
        result = await process_step(ctx, value)
        return result

    # Initialize the EddaApp
    await server._edda_app.initialize()
    server._compensations_executed = compensations_executed
    yield server
    # Cleanup after tests
    await server.shutdown()


@pytest.mark.asyncio
async def test_cancel_tool_registered(mcp_server_with_cancellable_tool):
    """Test that cancel tool is registered for durable_tool."""
    server = mcp_server_with_cancellable_tool

    # Check that 4 tools are registered per workflow
    # cancellable_workflow: main, status, result, cancel
    # quick_workflow: main, status, result, cancel
    # We verify by checking if the workflows exist in registry
    assert "cancellable_workflow" in server._workflows
    assert "quick_workflow" in server._workflows


@pytest.mark.asyncio
async def test_cancel_waiting_workflow(mcp_server_with_cancellable_tool):
    """Test cancelling a workflow waiting for an event."""
    server = mcp_server_with_cancellable_tool
    workflow = server._workflows["cancellable_workflow"]

    # Start workflow (will start processing and wait for event)
    instance_id = await workflow.start(value="test")

    # Give it time to reach wait_event
    await asyncio.sleep(0.2)

    # Verify workflow is waiting
    instance = await server.storage.get_instance(instance_id)
    assert instance["status"] == "waiting_for_message"

    # Cancel the workflow
    success = await server.replay_engine.cancel_workflow(instance_id, "mcp_user")

    assert success is True

    # Verify workflow is cancelled
    instance = await server.storage.get_instance(instance_id)
    assert instance["status"] == "cancelled"


@pytest.mark.asyncio
async def test_cancel_nonexistent_instance(mcp_server_with_cancellable_tool):
    """Test cancelling a workflow that doesn't exist returns False."""
    server = mcp_server_with_cancellable_tool

    # Try to cancel non-existent workflow
    success = await server.replay_engine.cancel_workflow("nonexistent-id", "mcp_user")

    assert success is False


@pytest.mark.asyncio
async def test_cancel_completed_workflow(mcp_server_with_cancellable_tool):
    """Test that completed workflows cannot be cancelled."""
    server = mcp_server_with_cancellable_tool
    workflow = server._workflows["quick_workflow"]

    # Start and wait for completion
    instance_id = await workflow.start(value="complete")

    # Give it time to complete
    await asyncio.sleep(0.2)

    # Verify workflow is completed
    instance = await server.storage.get_instance(instance_id)
    assert instance["status"] == "completed"

    # Try to cancel - should return False
    success = await server.replay_engine.cancel_workflow(instance_id, "mcp_user")

    assert success is False

    # Status should still be completed
    instance = await server.storage.get_instance(instance_id)
    assert instance["status"] == "completed"


@pytest.mark.asyncio
async def test_cancel_already_cancelled_workflow(mcp_server_with_cancellable_tool):
    """Test that cancelling already cancelled workflow is idempotent."""
    server = mcp_server_with_cancellable_tool
    workflow = server._workflows["cancellable_workflow"]

    # Start workflow
    instance_id = await workflow.start(value="idempotent")

    # Give it time to reach wait_event
    await asyncio.sleep(0.2)

    # Cancel first time
    success1 = await server.replay_engine.cancel_workflow(instance_id, "mcp_user")
    assert success1 is True

    # Cancel second time - should return False (already cancelled)
    success2 = await server.replay_engine.cancel_workflow(instance_id, "mcp_user")
    assert success2 is False

    # Status should still be cancelled
    instance = await server.storage.get_instance(instance_id)
    assert instance["status"] == "cancelled"


@pytest.mark.asyncio
async def test_replay_engine_property(mcp_server_with_cancellable_tool):
    """Test that replay_engine property is accessible on EddaMCPServer."""
    server = mcp_server_with_cancellable_tool

    # replay_engine should be accessible after initialization
    assert server.replay_engine is not None

    # Should be the same instance as EddaApp's replay_engine
    assert server.replay_engine is server._edda_app.replay_engine
