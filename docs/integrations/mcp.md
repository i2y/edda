# MCP (Model Context Protocol) Integration

Edda provides seamless integration with the [Model Context Protocol (MCP)](https://modelcontextprotocol.io/), allowing AI assistants like Claude to interact with your durable workflows as long-running tools.

## Overview

MCP is a standardized protocol for AI tool integration. Edda's MCP integration automatically converts your durable workflows into MCP-compliant tools that:

- **Start workflows** and return instance IDs immediately
- **Check workflow status** to monitor progress with completed activity count and suggested poll interval
- **Retrieve results** when workflows complete
- **Cancel workflows** if running or waiting, with automatic compensation execution

This enables AI assistants to work with long-running processes that may take minutes, hours, or even days to complete, with full control over the workflow lifecycle.

## Installation

Install Edda with MCP support:

```bash
pip install edda-framework[mcp]

# Or using uv
uv add edda-framework --extra mcp
```

## Quick Start

### 1. Create an MCP Server

```python
from edda.integrations.mcp import EddaMCPServer
from edda import WorkflowContext, activity

# Create MCP server
server = EddaMCPServer(
    name="Order Service",
    db_url="postgresql://user:pass@localhost/orders",
)

@activity
async def reserve_inventory(ctx: WorkflowContext, items: list[str]):
    # Your business logic here
    return {"reserved": True}

@activity
async def process_payment(ctx: WorkflowContext, amount: float):
    # Payment processing logic
    return {"transaction_id": "txn_123"}

@server.durable_tool(description="Process customer order workflow")
async def process_order(ctx: WorkflowContext, order_id: str, items: list[str]):
    """
    Long-running order processing workflow.

    This workflow reserves inventory, processes payment, and ships the order.
    """
    # Reserve inventory
    await reserve_inventory(ctx, items)

    # Process payment
    await process_payment(ctx, 99.99)

    return {"status": "completed", "order_id": order_id}
```

### 2. Deploy the Server

```python
# Deploy with uvicorn (production)
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(server.asgi_app(), host="0.0.0.0", port=8000)
```

```bash
# Run the server
uvicorn your_app:server.asgi_app --host 0.0.0.0 --port 8000
```

### 3. Use from MCP Clients (e.g., Claude Desktop)

Add to your MCP client configuration (e.g., Claude Desktop: `~/Library/Application Support/Claude/claude_desktop_config.json` on macOS):

```json
{
  "mcpServers": {
    "order-service": {
      "command": "uvicorn",
      "args": ["your_app:server.asgi_app", "--host", "127.0.0.1", "--port", "8000"],
      "env": {
        "DATABASE_URL": "postgresql://user:pass@localhost/orders"
      }
    }
  }
}
```

## Auto-Generated Tools

Each `@durable_tool` automatically generates **four MCP tools**:

### 1. Main Tool: Start Workflow

```
Tool Name: process_order
Description: Process customer order workflow

Input: {"order_id": "ORD-123", "items": ["item1", "item2"]}
Output: {
  "content": [{
    "type": "text",
    "text": "Workflow 'process_order' started successfully.\nInstance ID: abc123...\n\nUse 'process_order_status' tool to check progress."
  }],
  "isError": false
}
```

### 2. Status Tool: Check Progress

```
Tool Name: process_order_status
Description: Check status of process_order workflow

Input: {"instance_id": "abc123..."}
Output: {
  "content": [{
    "type": "text",
    "text": "Workflow Status: running\nCurrent Activity: payment:1\nCompleted Activities: 1\nSuggested Poll Interval: 5000ms\nInstance ID: abc123..."
  }],
  "isError": false
}
```

The status tool provides progress metadata for efficient polling:

- **Completed Activities**: Number of activities that have finished
- **Suggested Poll Interval**: Recommended wait time before checking again (5000ms for running, 10000ms for waiting)

### 3. Result Tool: Get Final Result

```
Tool Name: process_order_result
Description: Get result of process_order workflow (if completed)

Input: {"instance_id": "abc123..."}
Output: {
  "content": [{
    "type": "text",
    "text": "Workflow Result:\n{'status': 'completed', 'order_id': 'ORD-123'}"
  }],
  "isError": false
}
```

### 4. Cancel Tool: Stop Workflow

```
Tool Name: process_order_cancel
Description: Cancel process_order workflow (if running or waiting)

Input: {"instance_id": "abc123..."}
Output: {
  "content": [{
    "type": "text",
    "text": "Workflow 'process_order' cancelled successfully.\nInstance ID: abc123...\nCompensations executed.\n\nThe workflow has been stopped and any side effects have been rolled back."
  }],
  "isError": false
}
```

The cancel tool:

- Only works on workflows with status `running`, `waiting_for_event`, or `waiting_for_timer`
- Automatically executes SAGA compensation transactions to roll back side effects
- Returns an error for already completed, failed, or cancelled workflows

## MCP Prompts

Edda supports [MCP Prompts](https://modelcontextprotocol.io/docs/concepts/prompts), allowing AI assistants to access workflow state for context-aware prompts.

### Defining a Prompt

Use the `@server.prompt()` decorator to define prompts that can access workflow data:

```python
from edda.integrations.mcp import EddaMCPServer
from mcp.server.fastmcp.prompts.base import UserMessage
from mcp.types import TextContent

server = EddaMCPServer(
    name="Workflow Analysis Service",
    db_url="sqlite:///workflow.db",
)

@server.prompt(description="Analyze a completed workflow execution")
async def analyze_workflow(instance_id: str) -> UserMessage:
    """Generate an analysis prompt for a workflow."""
    # Access workflow state via server.storage
    instance = await server.storage.get_instance(instance_id)
    history = await server.storage.get_history(instance_id)

    text = f"""**Workflow Analysis Request**

**Workflow Details:**
- Instance ID: {instance_id}
- Status: {instance['status']}
- Workflow: {instance['workflow_name']}
- Activities Completed: {len(history)}

**Execution History:**
{chr(10).join(f"- {h['activity_id']}: {h['event_type']}" for h in history)}

Please analyze this workflow execution and provide:
1. Summary of what happened
2. Any potential issues or improvements
3. Performance observations"""

    return UserMessage(content=TextContent(type="text", text=text))
```

### Accessing Workflow State

Prompts can access workflow data through `server.storage`:

- `server.storage.get_instance(instance_id)` - Get workflow status, input, output
- `server.storage.get_history(instance_id)` - Get execution history (activities, events)

### Example: Debug Prompt

```python
@server.prompt(description="Debug a failed workflow")
async def debug_workflow(instance_id: str) -> UserMessage:
    """Generate a debugging prompt for failed workflows."""
    instance = await server.storage.get_instance(instance_id)
    history = await server.storage.get_history(instance_id)

    # Find failed activities
    failed = [h for h in history if h.get("event_type") == "ActivityFailed"]

    text = f"""**Workflow Debug Request**

Status: {instance['status']}
Error: {instance.get('error', 'None')}

Failed Activities: {len(failed)}
{chr(10).join(f"- {f['activity_id']}: {f.get('event_data', {}).get('error', 'Unknown')}" for f in failed)}

Please help debug this workflow failure."""

    return UserMessage(content=TextContent(type="text", text=text))
```

### Using Prompts from MCP Clients

MCP clients (like Claude Desktop) can list and use prompts:

1. **List available prompts**: Client discovers prompts with descriptions
2. **Get prompt**: Client requests prompt with arguments (e.g., `instance_id`)
3. **Use generated content**: AI assistant receives the context-rich prompt

See [examples/mcp/prompts_example.py](https://github.com/i2y/edda/blob/main/examples/mcp/prompts_example.py) for a complete working example.

## Advanced Configuration

### Authentication

Protect your MCP server with token-based authentication:

```python
def verify_token(token: str) -> bool:
    # Your token verification logic
    return token == "secret-token-123"

server = EddaMCPServer(
    name="Order Service",
    db_url="postgresql://user:pass@localhost/orders",
    token_verifier=verify_token,
)
```

Clients must include the token in the Authorization header:

```http
Authorization: Bearer secret-token-123
```

### Transactional Outbox Pattern

Enable event-driven architecture with outbox pattern:

```python
server = EddaMCPServer(
    name="Order Service",
    db_url="postgresql://user:pass@localhost/orders",
    outbox_enabled=True,
    broker_url="nats://localhost:4222",
)
```

## MCP Protocol Compliance

Edda's MCP integration follows the [MCP Tools specification](https://modelcontextprotocol.io/docs/concepts/tools):

- **JSON-RPC 2.0**: All communication uses JSON-RPC 2.0 protocol
- **Content Arrays**: Responses include `content` array with text/image/resource items
- **Error Handling**: Errors are reported with `isError: true` flag
- **Stateless HTTP**: Uses MCP's streamable HTTP transport for production deployments

## Architecture

```
┌─────────────────┐
│    MCP Client   │
└────────┬────────┘
         │ JSON-RPC 2.0
         │ (HTTP Transport)
         ▼
┌─────────────────┐
│  EddaMCPServer  │
│   ┌─────────┐   │
│   │ FastMCP │   │  ← Official MCP SDK
│   └─────────┘   │
│   ┌─────────┐   │
│   │ EddaApp │   │  ← Durable Execution
│   └─────────┘   │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│    Database     │
└─────────────────┘
```


## Related Documentation

- [Edda Workflows and Activities](../core-features/workflows-activities.md)
- [Transactional Outbox Pattern](../core-features/transactional-outbox.md)
- [MCP Protocol Specification](https://modelcontextprotocol.io/)

## Examples

See the [examples/mcp/](https://github.com/i2y/edda/tree/main/examples/mcp/) directory for complete working examples.
