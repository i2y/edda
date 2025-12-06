"""
Remote MCP Server Example

This example demonstrates how to run an Edda MCP server as a remote server
that can be accessed over HTTP/HTTPS from Claude Desktop or other MCP clients.

Features:
- Bearer token authentication
- Listens on all network interfaces (0.0.0.0)
- Production-ready authentication pattern
- Can be tested locally or deployed remotely
"""

import asyncio
import os
import sys
from pathlib import Path

from edda import WorkflowContext, activity
from edda.integrations.mcp import EddaMCPServer

# Create database directory in user's home directory
db_dir = Path.home() / ".edda"
db_dir.mkdir(exist_ok=True)
db_path = db_dir / "mcp_remote.db"


# Authentication function
def verify_token(token: str) -> bool:
    """
    Verify Bearer token for authentication.

    In production, you should:
    - Use environment variables for tokens
    - Validate against a database or secret manager
    - Implement token rotation
    - Use JWT tokens with expiration
    """
    # Get expected token from environment variable
    expected_token = os.environ.get("MCP_AUTH_TOKEN", "demo-secret-token-123")
    return token == expected_token


# Create MCP server with authentication
server = EddaMCPServer(
    name="Remote Greeting Service",
    db_url=f"sqlite+aiosqlite:///{db_path}",
    token_verifier=verify_token,  # Enable authentication
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


@server.durable_tool(description="Generate a personalized greeting (remote server)")
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
if __name__ == "__main__":
    import uvicorn

    # Get configuration from environment variables
    auth_token = os.environ.get("MCP_AUTH_TOKEN", "demo-secret-token-123")
    host = os.environ.get("MCP_HOST", "0.0.0.0")  # Listen on all interfaces
    port = int(os.environ.get("MCP_PORT", "8000"))

    # Write to stderr to keep stdout clean for JSON-RPC messages (HTTP transport compatibility)
    sys.stderr.write("=" * 60 + "\n")
    sys.stderr.write("Starting Remote MCP Server...\n")
    sys.stderr.write("=" * 60 + "\n")
    sys.stderr.write("Server name: Remote Greeting Service\n")
    sys.stderr.write(f"Database: {db_path}\n")
    sys.stderr.write(f"Listening on: http://{host}:{port}\n")
    sys.stderr.write("Authentication: Bearer token required\n")
    sys.stderr.write(f"Auth token: {auth_token}\n")
    sys.stderr.write("\n")
    sys.stderr.write("Available MCP tools:\n")
    sys.stderr.write("  - greet_user: Start greeting workflow\n")
    sys.stderr.write("  - greet_user_status: Check workflow status\n")
    sys.stderr.write("  - greet_user_result: Get greeting result\n")
    sys.stderr.write("\n")
    sys.stderr.write("To connect from MCP clients (e.g., Claude Desktop), use:\n")
    sys.stderr.write('  "command": "npx",\n')
    sys.stderr.write('  "args": ["mcp-remote", "http://localhost:8000",\n')
    sys.stderr.write('           "--header", "Authorization: Bearer ' + auth_token + '"]\n')
    sys.stderr.write("\n")
    sys.stderr.write("Press Ctrl+C to stop\n")
    sys.stderr.write("=" * 60 + "\n")
    sys.stderr.flush()

    uvicorn.run(
        server.asgi_app(),
        host=host,
        port=port,
        log_config=None,
    )
