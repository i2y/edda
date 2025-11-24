"""MCP Server implementation for Edda workflows."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

from edda.app import EddaApp
from edda.workflow import Workflow

try:
    from mcp.server.fastmcp import FastMCP
except ImportError as e:
    raise ImportError(
        "MCP Python SDK is required for MCP integration. "
        "Install it with: pip install edda-framework[mcp]"
    ) from e


class EddaMCPServer:
    """
    MCP (Model Context Protocol) server for Edda durable workflows.

    Integrates EddaApp (CloudEvents + Workflows) with FastMCP to provide
    long-running workflow tools via the MCP protocol.

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

        # Deploy with uvicorn (HTTP transport)
        if __name__ == "__main__":
            import uvicorn
            uvicorn.run(server.asgi_app(), host="0.0.0.0", port=8000)

        # Or deploy with stdio (for MCP clients, e.g., Claude Desktop)
        if __name__ == "__main__":
            import asyncio

            async def main():
                await server.initialize()
                await server.run_stdio()

            asyncio.run(main())
        ```

    The server automatically generates three MCP tools for each @durable_tool:
    - `tool_name`: Start the workflow, returns instance_id
    - `tool_name_status`: Check workflow status
    - `tool_name_result`: Get workflow result (if completed)
    """

    def __init__(
        self,
        name: str,
        db_url: str,
        *,
        outbox_enabled: bool = False,
        broker_url: str | None = None,
        token_verifier: Callable[[str], bool] | None = None,
    ):
        """
        Initialize MCP server.

        Args:
            name: Service name (shown in MCP client)
            db_url: Database URL for workflow storage
            outbox_enabled: Enable transactional outbox pattern
            broker_url: Message broker URL (if outbox enabled)
            token_verifier: Optional function to verify authentication tokens
        """
        self._name = name
        self._edda_app = EddaApp(
            service_name=name,
            db_url=db_url,
            outbox_enabled=outbox_enabled,
            broker_url=broker_url,
        )
        self._mcp = FastMCP(name, json_response=True, stateless_http=True)
        self._token_verifier = token_verifier

        # Registry of durable tools (workflow_name -> Workflow instance)
        self._workflows: dict[str, Workflow] = {}

    def durable_tool(
        self,
        func: Callable | None = None,
        *,
        description: str = "",
    ) -> Callable:
        """
        Decorator to define a durable workflow tool.

        Automatically generates three MCP tools:
        1. Main tool: Starts the workflow, returns instance_id
        2. Status tool: Checks workflow status
        3. Result tool: Gets workflow result (if completed)

        Args:
            func: Workflow function (async)
            description: Tool description for MCP clients

        Returns:
            Decorated workflow instance

        Example:
            ```python
            @server.durable_tool(description="Long-running order processing")
            async def process_order(ctx, order_id: str):
                # Workflow logic
                return {"status": "completed"}
            ```
        """
        from edda.integrations.mcp.decorators import create_durable_tool

        def decorator(f: Callable) -> Workflow:
            return create_durable_tool(self, f, description=description)

        if func is None:
            return decorator
        return decorator(func)

    def asgi_app(self) -> Callable:
        """
        Create ASGI application with MCP + CloudEvents support.

        This method uses the Issue #1367 workaround: instead of using Mount,
        we get the MCP's Starlette app directly and add Edda endpoints to it.

        Routing:
        - POST /    -> FastMCP (MCP tools via streamable HTTP)
        - POST /cancel/{instance_id} -> Workflow cancellation
        - Other POST -> CloudEvents

        Returns:
            ASGI callable (Starlette app)
        """
        from starlette.requests import Request
        from starlette.responses import Response

        # Get MCP's Starlette app (Issue #1367 workaround: use directly)
        app = self._mcp.streamable_http_app()

        # Add Edda endpoints to Starlette router BEFORE wrapping with middleware
        # Note: MCP's streamable HTTP is already mounted at "/" by default
        # We add additional routes for Edda's CloudEvents and cancellation

        async def edda_cancel_handler(request: Request) -> Response:
            """Handle workflow cancellation."""
            instance_id = request.path_params["instance_id"]

            # Create ASGI scope for EddaApp
            scope = dict(request.scope)
            scope["path"] = f"/cancel/{instance_id}"

            # Capture response
            response_data = {"status": 200, "headers": [], "body": b""}

            async def send(message: dict) -> None:
                if message["type"] == "http.response.start":
                    response_data["status"] = message["status"]
                    response_data["headers"] = message.get("headers", [])
                elif message["type"] == "http.response.body":
                    response_data["body"] += message.get("body", b"")

            # Forward to EddaApp
            await self._edda_app(scope, request.receive, send)

            # Return response
            return Response(
                content=response_data["body"],
                status_code=response_data["status"],
                headers=dict(response_data["headers"]),
            )

        # Add cancel route
        app.router.add_route("/cancel/{instance_id}", edda_cancel_handler, methods=["POST"])

        # Add authentication middleware if token_verifier provided (AFTER adding routes)
        if self._token_verifier is not None:
            from starlette.middleware.base import BaseHTTPMiddleware

            class AuthMiddleware(BaseHTTPMiddleware):
                def __init__(self, app: Any, token_verifier: Callable):
                    super().__init__(app)
                    self.token_verifier = token_verifier

                async def dispatch(self, request: Request, call_next: Callable):
                    auth_header = request.headers.get("authorization", "")
                    if auth_header.startswith("Bearer "):
                        token = auth_header[7:]
                        if not self.token_verifier(token):
                            return Response("Unauthorized", status_code=401)
                    return await call_next(request)

            # Wrap app with auth middleware
            app = AuthMiddleware(app, self._token_verifier)

        return app

    async def initialize(self) -> None:
        """
        Initialize the EddaApp (setup replay engine, storage, etc.).

        This method must be called before running the server in stdio mode.
        For HTTP mode (asgi_app()), initialization happens automatically
        when the ASGI app is deployed.

        Example:
            ```python
            async def main():
                await server.initialize()
                await server.run_stdio()

            if __name__ == "__main__":
                import asyncio
                asyncio.run(main())
            ```
        """
        await self._edda_app.initialize()

    async def run_stdio(self) -> None:
        """
        Run MCP server with stdio transport (for MCP clients, e.g., Claude Desktop).

        This method uses stdin/stdout for JSON-RPC communication.
        stderr can be used for diagnostic messages.

        The server will block until terminated (Ctrl+C or SIGTERM).

        Example:
            ```python
            async def main():
                await server.initialize()
                await server.run_stdio()

            if __name__ == "__main__":
                import asyncio
                asyncio.run(main())
            ```
        """
        await self._mcp.run_stdio_async()
