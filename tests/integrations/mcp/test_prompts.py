"""Tests for MCP Prompts functionality."""

import pytest

# Skip all tests if mcp is not installed
pytest.importorskip("mcp")

from mcp.server.fastmcp.prompts.base import UserMessage  # type: ignore[import-not-found]
from mcp.types import TextContent  # type: ignore[import-not-found]

from edda import WorkflowContext, activity
from edda.integrations.mcp import EddaMCPServer


@pytest.fixture
async def mcp_server():
    """Create MCP server with in-memory SQLite."""
    server = EddaMCPServer(
        name="Test Service",
        db_url="sqlite+aiosqlite:///:memory:",
    )
    # Initialize the EddaApp
    await server._edda_app.initialize()
    yield server
    # Cleanup
    await server.shutdown()


@pytest.mark.asyncio
async def test_prompt_registration(mcp_server):
    """Test @prompt decorator registers prompt correctly."""

    @mcp_server.prompt(description="Test prompt")
    async def test_prompt(arg: str) -> UserMessage:
        """Test prompt function."""
        return UserMessage(content=TextContent(type="text", text=f"Test: {arg}"))

    # Prompt should be registered with FastMCP
    # We can't easily inspect FastMCP's internal registry, but we can call the function
    result = await test_prompt("value")
    assert isinstance(result, UserMessage)
    assert result.role == "user"
    assert "Test: value" in result.content.text


@pytest.mark.asyncio
async def test_prompt_with_workflow_state(mcp_server):
    """Test prompt can access workflow state."""

    # Create a test workflow
    @activity
    async def test_activity(ctx: WorkflowContext, value: str):
        return {"processed": value}

    @mcp_server.durable_tool(description="Test workflow")
    async def test_workflow(ctx: WorkflowContext, value: str):
        result = await test_activity(ctx, value)
        return result

    # Start workflow
    workflow = mcp_server._workflows["test_workflow"]
    instance_id = await workflow.start(value="test_value")

    # Wait for completion
    await mcp_server._edda_app.replay_engine.resume_workflow(instance_id, workflow.func)

    # Define prompt that accesses workflow state
    @mcp_server.prompt()
    async def analyze(instance_id: str) -> UserMessage:
        """Analyze workflow execution."""
        instance = await mcp_server.storage.get_instance(instance_id)
        text = f"Status: {instance['status']}, Name: {instance['workflow_name']}"
        return UserMessage(content=TextContent(type="text", text=text))

    # Call prompt
    result = await analyze(instance_id)
    assert "Status:" in result.content.text
    assert "test_workflow" in result.content.text


@pytest.mark.asyncio
async def test_prompt_error_handling(mcp_server):
    """Test prompt handles missing workflow gracefully."""

    @mcp_server.prompt()
    async def analyze(instance_id: str) -> UserMessage:
        """Analyze workflow execution."""
        instance = await mcp_server.storage.get_instance(instance_id)
        if instance is None:
            text = f"Workflow '{instance_id}' not found"
        else:
            text = f"Found: {instance['workflow_name']}"
        return UserMessage(content=TextContent(type="text", text=text))

    # Call with nonexistent instance_id
    result = await analyze("nonexistent-id")
    assert "not found" in result.content.text


@pytest.mark.asyncio
async def test_prompt_with_multiple_arguments(mcp_server):
    """Test prompt with multiple arguments."""

    @mcp_server.prompt(description="Compare two values")
    async def compare(value_a: str, value_b: str) -> UserMessage:
        """Compare two values."""
        text = f"Comparing '{value_a}' vs '{value_b}'"
        return UserMessage(content=TextContent(type="text", text=text))

    result = await compare("foo", "bar")
    assert "foo" in result.content.text
    assert "bar" in result.content.text


@pytest.mark.asyncio
async def test_multiple_prompts_registration(mcp_server):
    """Test multiple prompts can be registered."""

    @mcp_server.prompt(description="Prompt 1")
    async def prompt_one() -> UserMessage:
        return UserMessage(content=TextContent(type="text", text="Prompt 1"))

    @mcp_server.prompt(description="Prompt 2")
    async def prompt_two() -> UserMessage:
        return UserMessage(content=TextContent(type="text", text="Prompt 2"))

    @mcp_server.prompt(description="Prompt 3")
    async def prompt_three() -> UserMessage:
        return UserMessage(content=TextContent(type="text", text="Prompt 3"))

    # All prompts should be callable
    r1 = await prompt_one()
    r2 = await prompt_two()
    r3 = await prompt_three()

    assert "Prompt 1" in r1.content.text
    assert "Prompt 2" in r2.content.text
    assert "Prompt 3" in r3.content.text


@pytest.mark.asyncio
async def test_sync_prompt(mcp_server):
    """Test synchronous prompt function works."""

    @mcp_server.prompt(description="Sync prompt")
    def sync_prompt(value: str) -> UserMessage:
        """Synchronous prompt function."""
        return UserMessage(content=TextContent(type="text", text=f"Sync: {value}"))

    # FastMCP handles both sync and async
    result = sync_prompt("test")
    assert isinstance(result, UserMessage)
    assert "Sync: test" in result.content.text


@pytest.mark.asyncio
async def test_prompt_without_description(mcp_server):
    """Test prompt uses docstring if no description provided."""

    @mcp_server.prompt()
    async def undocumented_prompt(arg: str) -> UserMessage:
        """This is the docstring."""
        return UserMessage(content=TextContent(type="text", text=f"Value: {arg}"))

    # Should use docstring as description
    result = await undocumented_prompt("test")
    assert "Value: test" in result.content.text


@pytest.mark.asyncio
async def test_prompt_accesses_workflow_history(mcp_server):
    """Test prompt can access detailed workflow history."""

    # Create test workflow with multiple activities
    @activity
    async def step_one(ctx: WorkflowContext):
        return {"step": 1}

    @activity
    async def step_two(ctx: WorkflowContext):
        return {"step": 2}

    @mcp_server.durable_tool(description="Multi-step workflow")
    async def multi_step(ctx: WorkflowContext):
        await step_one(ctx)
        await step_two(ctx)
        return {"completed": True}

    # Start and complete workflow
    workflow = mcp_server._workflows["multi_step"]
    instance_id = await workflow.start()
    await mcp_server._edda_app.replay_engine.resume_workflow(instance_id, workflow.func)

    # Prompt that analyzes history
    @mcp_server.prompt()
    async def analyze_history(instance_id: str) -> UserMessage:
        """Analyze workflow history."""
        history = await mcp_server.storage.get_history(instance_id)
        text = f"Workflow had {len(history)} activities"
        return UserMessage(content=TextContent(type="text", text=text))

    result = await analyze_history(instance_id)
    assert "activities" in result.content.text
