# MCP Integration Examples

This directory contains example MCP (Model Context Protocol) servers that demonstrate how to expose Edda durable workflows as AI tools.

## MCP Transport Modes

MCP supports two transport modes:

1. **stdio transport** - For MCP client subprocess integration (stdin/stdout for JSON-RPC)
2. **HTTP transport** - For remote server deployment (HTTP endpoints for JSON-RPC)

## Examples

### 1. Simple MCP Server (`simple_mcp_server.py`) - stdio transport

A minimal example showing the basics of MCP integration with MCP clients.

**Features**:
- Single durable workflow (`greet_user`)
- Simple activity execution
- SQLite database
- stdio transport for MCP client subprocess

**Use with MCP Clients**:
Add to your MCP client configuration.

**Run standalone** (for testing):
```bash
python simple_mcp_server.py
```

**Provides these tools**:
- `greet_user`: Start greeting workflow
- `greet_user_status`: Check workflow status
- `greet_user_result`: Get greeting result

### 2. Order Processing Server (`order_processing_mcp.py`) - stdio transport

A realistic example showing multi-step order processing with MCP clients.

**Features**:
- Multi-step workflow (validation → inventory → payment → shipping)
- Multiple activities
- Simulated long-running operations (~5.5 seconds total)
- stdio transport for MCP client subprocess

**Use with MCP Clients**:
Add to your MCP client configuration.

**Run standalone** (for testing):
```bash
python order_processing_mcp.py
```

**Provides these tools**:
- `process_order`: Start order processing workflow
- `process_order_status`: Check workflow status
- `process_order_result`: Get processing result

### 3. Remote MCP Server (`remote_server_example.py`) - HTTP transport

An example demonstrating how to run an MCP server that can be accessed remotely over HTTP.

**Features**:
- Bearer token authentication
- Listens on all network interfaces (`0.0.0.0`)
- HTTP transport for remote access
- Can be accessed from MCP clients using `npx mcp-remote`
- Production-ready authentication pattern

**Run**:
```bash
# Set authentication token (optional, defaults to "demo-secret-token-123")
export MCP_AUTH_TOKEN="your-secret-token"

# Start the server
python remote_server_example.py
```

The server will start on `http://0.0.0.0:8000` with HTTP transport.

See the "Using as Remote MCP Server" section below for how to connect from MCP clients.

**Important**: Replace `/path/to/edda/` with the actual path to your Edda installation.

**Technical Note**: The simple and order processing examples use **stdio transport** (stdin/stdout for JSON-RPC). When MCP clients launch them as subprocesses, stdout must contain only JSON-RPC 2.0 messages. All diagnostic messages are written to stderr (using `sys.stderr.write()`) to keep stdout clean.

## Generated Tools

The server automatically generates three tools for each `@durable_tool`:

### 1. Main tool (starts workflow)

```python
# In MCP client
greet_user(name="Alice", style="friendly")
```

Returns an instance ID like: `abc123...`

### 2. Status tool (check progress)

```python
greet_user_status(instance_id="abc123...")
```

Returns: `"Workflow Status: completed\nCurrent Activity: N/A\nInstance ID: abc123..."`

### 3. Result tool (get final result)

```python
greet_user_result(instance_id="abc123...")
```

Returns: `"Workflow Result:\n{'greeting': 'Hello Alice! How are you doing today?'}"`

**Note**: The simple and order processing examples use stdio transport and can be used with any MCP client. For programmatic testing, use the remote server example with HTTP transport.

## Using as Remote MCP Server

The `remote_server_example.py` demonstrates how to run an MCP server that can be accessed remotely over HTTP.

### 1. Start the Remote Server

```bash
# Set authentication token (optional, defaults to "demo-secret-token-123")
export MCP_AUTH_TOKEN="your-secret-token"

# Start the server (listens on all interfaces)
python remote_server_example.py
```

The server will display:
```
====================================================================
Starting Remote MCP Server...
====================================================================
Server name: Remote Greeting Service
Database: sqlite+aiosqlite:///mcp_remote.db
Listening on: http://0.0.0.0:8000
Authentication: Bearer token required
Auth token: your-secret-token
...
```

### 2. Connect from MCP Clients

Add this to your MCP client configuration (e.g., Claude Desktop):

**macOS**: `~/Library/Application Support/Claude/claude_desktop_config.json`
**Windows**: `%APPDATA%\Claude\claude_desktop_config.json`

```json
{
  "mcpServers": {
    "remote-greeting-service": {
      "command": "npx",
      "args": [
        "mcp-remote",
        "http://localhost:8000",
        "--header",
        "Authorization: Bearer your-secret-token"
      ]
    }
  }
}
```

**Important**: Replace `your-secret-token` with the actual token you set in `MCP_AUTH_TOKEN`.

### 3. Test the Connection

1. Restart your MCP client
2. The "remote-greeting-service" should appear in the MCP tools list
3. Try using the `greet_user` tool from the MCP client

### 4. Accessing from Other Machines

To access the server from another machine on your network:

1. Find your machine's IP address:
   ```bash
   # macOS/Linux
   ifconfig | grep "inet "

   # Windows
   ipconfig
   ```

2. Update the MCP client configuration on the client machine (e.g., Claude Desktop):
   ```json
   {
     "mcpServers": {
       "remote-greeting-service": {
         "command": "npx",
         "args": [
           "mcp-remote",
           "http://YOUR_IP_ADDRESS:8000",
           "--header",
           "Authorization: Bearer your-secret-token"
         ]
       }
     }
   }
   ```

### 5. Security Notes

- **Authentication**: The example uses Bearer token authentication. Keep your token secret.
- **HTTPS**: For production, use HTTPS with a reverse proxy (nginx, Caddy) or deploy behind a load balancer.
- **Firewall**: Ensure port 8000 is open if accessing from other machines.
- **Token Storage**: Store tokens in environment variables, not in code.

## Database Files

The examples create SQLite database files in `~/.edda/`:
- `~/.edda/mcp_simple.db` - Simple server database
- `~/.edda/mcp_orders.db` - Order processing server database
- `~/.edda/mcp_remote.db` - Remote server database

These files store workflow state and history for crash recovery.

## Production Considerations

For production use:

1. **Use PostgreSQL or MySQL**:
   ```python
   server = EddaMCPServer(
       name="Production Service",
       db_url="postgresql://user:pass@localhost/workflows",
   )
   ```

2. **Enable Authentication**:
   ```python
   def verify_token(token: str) -> bool:
       return token == os.environ.get("MCP_TOKEN")

   server = EddaMCPServer(
       name="Production Service",
       db_url="postgresql://...",
       token_verifier=verify_token,
   )
   ```

3. **Use Multiple Workers**:
   ```bash
   uvicorn your_app:server.asgi_app --workers 4 --host 0.0.0.0 --port 8000
   ```

4. **Enable Transactional Outbox** (for event-driven architecture):
   ```python
   server = EddaMCPServer(
       name="Production Service",
       db_url="postgresql://...",
       outbox_enabled=True,
       broker_url="nats://localhost:4222",
   )
   ```

## Troubleshooting

### ImportError: MCP Python SDK is required

Install MCP dependencies:
```bash
pip install edda-framework[mcp]
# or
uv add edda-framework --extra mcp
```

### Workflow not starting

Ensure the database file is writable and the directory exists:
```bash
# Check database file
ls -la mcp_simple.db

# Create directory if needed
mkdir -p /path/to/db/directory
```

### MCP client not showing tools

1. Restart your MCP client after updating configuration
2. Check server logs for errors
3. Verify the server is running: `curl http://localhost:8000`

## Related Documentation

- [MCP Integration Guide](../../docs/integrations/mcp.md)
- [Edda Documentation](https://i2y.github.io/edda/)
- [Model Context Protocol](https://modelcontextprotocol.io/)

## Next Steps

- Modify the examples to fit your use case
- Add more activities and workflows
- Integrate with your existing systems
- Deploy to production with PostgreSQL
