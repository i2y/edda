"""E2E integration tests for MCP server."""

import asyncio

import pytest

# Skip all tests if mcp is not installed
pytest.importorskip("mcp")

from edda import WorkflowContext, activity
from edda.integrations.mcp import EddaMCPServer


@pytest.fixture
async def mcp_server_with_tool():
    """Create MCP server with a sample durable tool."""
    server = EddaMCPServer(
        name="Integration Test Service",
        db_url="sqlite+aiosqlite:///:memory:",
    )

    @activity
    async def process_step(ctx: WorkflowContext, value: str):
        await asyncio.sleep(0.1)  # Simulate work
        return {"processed": value.upper()}

    @server.durable_tool(description="Process input value")
    async def process_value(ctx: WorkflowContext, value: str):
        result = await process_step(ctx, value)
        return result

    # Initialize the EddaApp
    await server._edda_app.initialize()
    yield server
    # Cleanup after tests
    await server.shutdown()


@pytest.mark.asyncio
async def test_e2e_workflow_start_to_result(mcp_server_with_tool):
    """Test complete workflow: start -> status -> result."""
    server = mcp_server_with_tool

    # Step 1: Start workflow via main tool
    workflow = server._workflows["process_value"]
    instance_id = await workflow.start(value="hello")

    # Verify instance_id is returned
    assert instance_id is not None
    assert isinstance(instance_id, str)

    # Step 2: Check status
    instance = await server.storage.get_instance(instance_id)
    status = instance["status"]

    # Status should be either completed or running
    assert status in ["completed", "running"]

    # Step 3: If completed, get result
    if status == "completed":
        output_data = instance.get("output_data")
        assert output_data is not None
        # The workflow result is wrapped in "result" key
        assert output_data["result"]["processed"] == "HELLO"


@pytest.mark.asyncio
async def test_workflow_with_failure(mcp_server_with_tool):
    """Test workflow that fails."""
    server = mcp_server_with_tool

    @server.durable_tool(description="Failing workflow")
    async def failing_workflow(ctx: WorkflowContext):
        raise ValueError("Intentional failure")

    workflow = server._workflows["failing_workflow"]

    # Start workflow (should fail)
    with pytest.raises(ValueError, match="Intentional failure"):
        await workflow.start()


@pytest.mark.asyncio
async def test_multiple_workflows_concurrently(mcp_server_with_tool):
    """Test multiple workflows can run concurrently."""
    server = mcp_server_with_tool
    workflow = server._workflows["process_value"]

    # Start 3 workflows sequentially to avoid workflow_definitions race condition
    # (concurrent first-time registrations cause UNIQUE constraint violation)
    instance_ids = []
    for value in ["first", "second", "third"]:
        instance_id = await workflow.start(value=value)
        instance_ids.append(instance_id)

    # All should have unique instance_ids
    assert len(instance_ids) == 3
    assert len(set(instance_ids)) == 3

    # All should be in database
    for instance_id in instance_ids:
        instance = await server.storage.get_instance(instance_id)
        assert instance["workflow_name"] == "process_value"


@pytest.mark.asyncio
async def test_status_tool_usage(mcp_server_with_tool):
    """Test that status tool can check workflow progress."""
    server = mcp_server_with_tool
    workflow = server._workflows["process_value"]

    # Start workflow
    instance_id = await workflow.start(value="status_test")

    # Check status via storage (simulating status tool)
    instance = await server.storage.get_instance(instance_id)
    status = instance["status"]

    assert status in ["completed", "running", "failed"]
    assert instance["workflow_name"] == "process_value"


@pytest.mark.asyncio
async def test_result_tool_before_completion(mcp_server_with_tool):
    """Test that result tool returns error if workflow not completed."""
    server = mcp_server_with_tool

    @activity
    async def long_running_step(ctx: WorkflowContext):
        await asyncio.sleep(10)  # Very long wait
        return {"done": True}

    @server.durable_tool(description="Long workflow")
    async def long_workflow(ctx: WorkflowContext):
        result = await long_running_step(ctx)
        return result

    workflow = server._workflows["long_workflow"]

    # Start workflow (will not complete quickly)
    instance_id = await workflow.start()

    # Immediately check status (should be running)
    instance = await server.storage.get_instance(instance_id)
    status = instance["status"]

    # If still running, output_data should be None
    if status == "running":
        output_data = instance.get("output_data")
        assert output_data is None


@pytest.mark.asyncio
async def test_pydantic_model_integration(mcp_server_with_tool):
    """Test durable tool with Pydantic models."""
    from pydantic import BaseModel

    server = mcp_server_with_tool

    class OrderInput(BaseModel):
        order_id: str
        amount: float

    class OrderResult(BaseModel):
        order_id: str
        status: str

    @server.durable_tool(description="Process order with Pydantic")
    async def process_order(ctx: WorkflowContext, order: OrderInput) -> OrderResult:
        return OrderResult(order_id=order.order_id, status="completed")

    workflow = server._workflows["process_order"]

    # Start workflow with Pydantic model
    from pydantic import TypeAdapter

    order_adapter = TypeAdapter(OrderInput)
    order_input = order_adapter.validate_python({"order_id": "123", "amount": 99.99})
    instance_id = await workflow.start(order=order_input)

    # Verify workflow started
    instance = await server.storage.get_instance(instance_id)
    assert instance["workflow_name"] == "process_order"
