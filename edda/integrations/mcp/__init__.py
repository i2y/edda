"""
Edda MCP (Model Context Protocol) Integration.

Provides MCP server functionality for Edda durable workflows,
enabling long-running workflow tools via the MCP protocol.

Example:
    ```python
    from edda.integrations.mcp import EddaMCPServer
    from edda import WorkflowContext, activity

    server = EddaMCPServer(
        name="Order Service",
        db_url="postgresql://user:pass@localhost/orders",
    )

    @activity
    async def reserve_inventory(ctx, items):
        return {"reserved": True}

    @server.durable_tool(description="Process order workflow")
    async def process_order(ctx: WorkflowContext, order_id: str):
        await reserve_inventory(ctx, [order_id], activity_id="reserve:1")
        return {"status": "completed"}

    # Deploy with uvicorn
    if __name__ == "__main__":
        import uvicorn
        uvicorn.run(server.asgi_app(), host="0.0.0.0", port=8000)
    ```

The server automatically generates three MCP tools for each @durable_tool:
- `tool_name`: Start the workflow, returns instance_id
- `tool_name_status`: Check workflow status
- `tool_name_result`: Get workflow result (if completed)
"""

from edda.integrations.mcp.server import EddaMCPServer

__all__ = ["EddaMCPServer"]
