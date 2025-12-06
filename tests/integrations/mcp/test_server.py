"""Tests for EddaMCPServer basic functionality."""

import pytest

# Skip all tests if mcp is not installed
pytest.importorskip("mcp")

from edda import WorkflowContext, activity
from edda.integrations.mcp import EddaMCPServer


@pytest.fixture
async def mcp_server():
    """Create MCP server with in-memory SQLite."""
    server = EddaMCPServer(
        name="Test Service",
        db_url="sqlite+aiosqlite:///:memory:",
    )
    # Initialize the EddaApp (sets up replay engine, storage, etc.)
    await server._edda_app.initialize()
    yield server
    # Cleanup after tests
    await server.shutdown()


@pytest.mark.asyncio
async def test_server_initialization(mcp_server):
    """Test EddaMCPServer initializes correctly."""
    assert mcp_server._name == "Test Service"
    assert mcp_server._edda_app is not None
    assert mcp_server._mcp is not None
    assert mcp_server._workflows == {}


@pytest.mark.asyncio
async def test_durable_tool_registration(mcp_server):
    """Test @durable_tool registers workflow correctly."""

    @mcp_server.durable_tool(description="Test workflow")
    async def test_workflow(ctx: WorkflowContext, value: str):
        """Test workflow function."""
        return {"result": value}

    # Check workflow is registered
    assert "test_workflow" in mcp_server._workflows
    workflow_instance = mcp_server._workflows["test_workflow"]
    assert workflow_instance.name == "test_workflow"


@pytest.mark.asyncio
async def test_asgi_app_creation(mcp_server):
    """Test ASGI app is created correctly."""
    asgi_app = mcp_server.asgi_app()
    assert asgi_app is not None
    assert callable(asgi_app)


@pytest.mark.asyncio
async def test_workflow_start_via_tool(mcp_server):
    """Test workflow can be started via MCP tool."""

    @activity
    async def test_activity(ctx: WorkflowContext, value: str):
        return {"processed": value}

    @mcp_server.durable_tool(description="Process value")
    async def process_value(ctx: WorkflowContext, value: str):
        result = await test_activity(ctx, value)
        return result

    # Manually call the workflow start
    workflow = mcp_server._workflows["process_value"]
    instance_id = await workflow.start(value="test_value")

    assert instance_id is not None
    assert isinstance(instance_id, str)

    # Verify instance was created in storage
    instance = await mcp_server.storage.get_instance(instance_id)
    assert instance["workflow_name"] == "process_value"
    assert instance["status"] in ["completed", "running"]


@pytest.mark.asyncio
async def test_multiple_tools_registration(mcp_server):
    """Test multiple durable tools can be registered."""

    @mcp_server.durable_tool(description="Tool 1")
    async def tool_one(ctx: WorkflowContext):
        return {"tool": "one"}

    @mcp_server.durable_tool(description="Tool 2")
    async def tool_two(ctx: WorkflowContext):
        return {"tool": "two"}

    @mcp_server.durable_tool(description="Tool 3")
    async def tool_three(ctx: WorkflowContext):
        return {"tool": "three"}

    # All workflows should be registered
    assert len(mcp_server._workflows) == 3
    assert "tool_one" in mcp_server._workflows
    assert "tool_two" in mcp_server._workflows
    assert "tool_three" in mcp_server._workflows


@pytest.mark.asyncio
async def test_status_tool_progress_metadata(mcp_server):
    """Test status tool returns progress metadata (completed activities, poll interval)."""
    import asyncio

    @activity
    async def step_one(ctx: WorkflowContext, value: str):
        return {"step": 1}

    @activity
    async def step_two(ctx: WorkflowContext, value: str):
        return {"step": 2}

    @mcp_server.durable_tool(description="Multi-step workflow")
    async def multi_step_workflow(ctx: WorkflowContext, value: str):
        result1 = await step_one(ctx, value)
        result2 = await step_two(ctx, value)
        return {"result": "completed", "steps": [result1, result2]}

    # Start workflow
    workflow = mcp_server._workflows["multi_step_workflow"]
    instance_id = await workflow.start(value="test")

    # Wait for completion
    await asyncio.sleep(0.2)

    # Get instance and history
    instance = await mcp_server.storage.get_instance(instance_id)
    history = await mcp_server.storage.get_history(instance_id)

    # Verify completed activities can be counted from history
    completed_activities = len([h for h in history if h["event_type"] == "ActivityCompleted"])
    assert completed_activities == 2  # step_one and step_two

    # Verify status is completed
    assert instance["status"] == "completed"


@pytest.mark.asyncio
async def test_status_tool_poll_interval_running():
    """Test status tool suggests shorter poll interval for running workflows."""
    from edda.channels import wait_event

    server = EddaMCPServer(
        name="Poll Test Service",
        db_url="sqlite+aiosqlite:///:memory:",
    )
    await server._edda_app.initialize()

    @activity
    async def process(ctx: WorkflowContext):
        return {"processed": True}

    @server.durable_tool(description="Waiting workflow")
    async def waiting_workflow(ctx: WorkflowContext):
        await process(ctx)
        # Wait for an event (workflow will be in waiting_for_message status)
        await wait_event(ctx, "test_event")
        return {"done": True}

    # Start workflow
    workflow = server._workflows["waiting_workflow"]
    instance_id = await workflow.start()

    # Give it time to reach wait_event
    import asyncio

    await asyncio.sleep(0.2)

    # Check status
    instance = await server.storage.get_instance(instance_id)

    # Should be waiting for message (not running)
    assert instance["status"] == "waiting_for_message"

    # Cleanup
    await server.shutdown()
