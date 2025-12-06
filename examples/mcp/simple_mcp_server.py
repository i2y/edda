"""
Simple MCP Server Example

This example demonstrates a basic MCP server with a single durable workflow.
AI assistants like Claude can use this to perform long-running greetings.
"""

import asyncio
import sys
from pathlib import Path

from edda import WorkflowContext, activity
from edda.integrations.mcp import EddaMCPServer

# Create database directory in user's home directory
db_dir = Path.home() / ".edda"
db_dir.mkdir(exist_ok=True)
db_path = db_dir / "mcp_simple.db"

# Create MCP server
server = EddaMCPServer(
    name="Greeting Service",
    db_url=f"sqlite+aiosqlite:///{db_path}",
)


@activity
async def format_greeting(_ctx: WorkflowContext, name: str, style: str) -> dict:
    """Format a greeting based on style."""
    await asyncio.sleep(0.5)  # Simulate work

    greetings = {
        "formal": f"Good day, {name}. It is a pleasure to meet you.",
        "casual": f"Hey {name}! Nice to meet you!",
        "friendly": f"Hello {name}! How are you doing today?",
    }

    return {"greeting": greetings.get(style, f"Hello, {name}!")}


@server.durable_tool(description="Generate a personalized greeting (takes a few seconds)")
async def greet_user(ctx: WorkflowContext, name: str, style: str = "friendly"):
    """
    Create a personalized greeting workflow.

    Args:
        name: Person's name
        style: Greeting style - 'formal', 'casual', or 'friendly' (default)

    Returns:
        dict with 'greeting' field containing the personalized message
    """
    # Execute the greeting activity
    result = await format_greeting(ctx, name, style)  # Auto: "format_greeting:1"

    return result


# Deploy the server
async def main():
    """Initialize and run the MCP server."""
    # Write to stderr to keep stdout clean for JSON-RPC messages (stdio transport compatibility)
    sys.stderr.write("Starting Simple MCP Server (stdio transport)...\n")
    sys.stderr.write("Server name: Greeting Service\n")
    sys.stderr.write(f"Database: {db_path}\n")
    sys.stderr.write("\nAvailable MCP tools:\n")
    sys.stderr.write("  - greet_user: Start greeting workflow\n")
    sys.stderr.write("  - greet_user_status: Check workflow status\n")
    sys.stderr.write("  - greet_user_result: Get greeting result\n")
    sys.stderr.write("\nPress Ctrl+C to stop\n")
    sys.stderr.flush()

    # Initialize EddaApp (setup replay engine, storage, etc.)
    await server.initialize()

    # Run with stdio transport (for MCP clients, e.g., Claude Desktop)
    # stdout is used for JSON-RPC messages, stderr for diagnostics
    await server.run_stdio()


if __name__ == "__main__":
    asyncio.run(main())
