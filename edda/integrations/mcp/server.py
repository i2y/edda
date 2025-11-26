"""MCP Server implementation for Edda workflows."""

from __future__ import annotations

from collections.abc import Callable
from typing import TYPE_CHECKING, Any, cast

from edda.app import EddaApp
from edda.workflow import Workflow

if TYPE_CHECKING:
    from edda.replay import ReplayEngine
    from edda.storage.protocol import StorageProtocol

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
            import asyncio
            import uvicorn

            async def startup():
                await server.initialize()

            asyncio.run(startup())
            uvicorn.run(server.asgi_app(), host="0.0.0.0", port=8000)

        # Or deploy with stdio (for MCP clients, e.g., Claude Desktop)
        if __name__ == "__main__":
            import asyncio

            async def main():
                await server.initialize()
                await server.run_stdio()

            asyncio.run(main())
        ```

    The server automatically generates four MCP tools for each @durable_tool:
    - `tool_name`: Start the workflow, returns instance_id
    - `tool_name_status`: Check workflow status
    - `tool_name_result`: Get workflow result (if completed)
    - `tool_name_cancel`: Cancel workflow (if running or waiting)
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
            broker_url=broker_url or "",
        )
        self._mcp = FastMCP(name, json_response=True, stateless_http=True)
        self._token_verifier = token_verifier

        # Registry of durable tools (workflow_name -> Workflow instance)
        self._workflows: dict[str, Workflow] = {}

    @property
    def storage(self) -> StorageProtocol:
        """
        Access workflow storage for querying instances and history.

        Returns:
            StorageProtocol: Storage backend for workflow state

        Example:
            ```python
            instance = await server.storage.get_instance(instance_id)
            history = await server.storage.get_history(instance_id)
            ```
        """
        return self._edda_app.storage

    @property
    def replay_engine(self) -> ReplayEngine | None:
        """
        Access replay engine for workflow operations (cancel, resume, etc.).

        Returns:
            ReplayEngine or None if not initialized

        Example:
            ```python
            # Cancel a running workflow
            success = await server.replay_engine.cancel_workflow(
                instance_id, "mcp_user"
            )
            ```
        """
        return self._edda_app.replay_engine

    def durable_tool(
        self,
        func: Callable[..., Any] | None = None,
        *,
        description: str = "",
    ) -> Callable[..., Any]:
        """
        Decorator to define a durable workflow tool.

        Automatically generates four MCP tools:
        1. Main tool: Starts the workflow, returns instance_id
        2. Status tool: Checks workflow status
        3. Result tool: Gets workflow result (if completed)
        4. Cancel tool: Cancels workflow (if running or waiting)

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

        def decorator(f: Callable[..., Any]) -> Workflow:
            return create_durable_tool(self, f, description=description)

        if func is None:
            return decorator
        return decorator(func)

    def prompt(
        self,
        func: Callable[..., Any] | None = None,
        *,
        description: str = "",
    ) -> Callable[..., Any]:
        """
                Decorator to define a prompt template.

                Prompts can access workflow state to generate dynamic, context-aware
                prompts for AI clients (Claude Desktop, etc.).

                Args:
                    func: Prompt function (async or sync)
                    description: Prompt description for MCP clients

                Returns:
                    Decorated function

                Example:
                    ```python
                    from fastmcp.prompts.prompt import PromptMessage, TextContent

                    @server.prompt(description="Analyze workflow results")
                    async def analyze_workflow(instance_id: str) -> PromptMessage:
                        '''Generate a prompt to analyze a specific workflow execution.'''
                        instance = await server.storage.get_instance(instance_id)
                        history = await server.storage.get_history(instance_id)

                        text = f'''Analyze this workflow:

        Instance ID: {instance_id}
        Status: {instance['status']}
        Activities: {len(history)}

        Please identify any issues or optimization opportunities.'''

                        return PromptMessage(
                            role="user",
                            content=TextContent(type="text", text=text)
                        )
                    ```
        """

        def decorator(f: Callable[..., Any]) -> Callable[..., Any]:
            # Use FastMCP's native prompt decorator
            prompt_desc = description or f.__doc__ or f"Prompt: {f.__name__}"
            return self._mcp.prompt(description=prompt_desc)(f)

        if func is None:
            return decorator
        return decorator(func)

    def asgi_app(self) -> Callable[..., Any]:
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
            response_data: dict[str, Any] = {"status": 200, "headers": [], "body": b""}

            async def send(message: dict[str, Any]) -> None:
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
                headers=cast(dict[str, str], dict(response_data["headers"])),
            )

        # Add cancel route
        app.router.add_route("/cancel/{instance_id}", edda_cancel_handler, methods=["POST"])

        # Add authentication middleware if token_verifier provided (AFTER adding routes)
        result_app: Any = app
        if self._token_verifier is not None:
            from starlette.middleware.base import BaseHTTPMiddleware

            class AuthMiddleware(BaseHTTPMiddleware):
                def __init__(self, app_inner: Any, token_verifier: Callable[[str], bool]) -> None:
                    super().__init__(app_inner)
                    self.token_verifier = token_verifier

                async def dispatch(
                    self, request: Request, call_next: Callable[..., Any]
                ) -> Response:
                    auth_header = request.headers.get("authorization", "")
                    if auth_header.startswith("Bearer "):
                        token = auth_header[7:]
                        if not self.token_verifier(token):
                            return Response("Unauthorized", status_code=401)
                    response: Response = await call_next(request)
                    return response

            # Wrap app with auth middleware
            result_app = AuthMiddleware(app, self._token_verifier)

        return cast(Callable[..., Any], result_app)

    async def initialize(self) -> None:
        """
        Initialize the EddaApp (setup replay engine, storage, etc.).

        This method must be called before running the server in either stdio or HTTP mode.

        Example (stdio mode):
            ```python
            async def main():
                await server.initialize()
                await server.run_stdio()

            if __name__ == "__main__":
                import asyncio
                asyncio.run(main())
            ```

        Example (HTTP mode):
            ```python
            import asyncio
            import uvicorn

            async def startup():
                await server.initialize()

            asyncio.run(startup())
            uvicorn.run(server.asgi_app(), host="0.0.0.0", port=8000)
            ```
        """
        await self._edda_app.initialize()

    async def shutdown(self) -> None:
        """
        Shutdown the server and cleanup resources.

        Stops background tasks (auto-resume, timer checks, event timeouts),
        closes storage connections, and performs graceful shutdown.

        This method should be called when the server is shutting down.

        Example (stdio mode):
            ```python
            import signal
            import asyncio

            async def main():
                server = EddaMCPServer(...)
                await server.initialize()

                # Setup signal handlers for graceful shutdown
                loop = asyncio.get_running_loop()
                shutdown_event = asyncio.Event()

                def signal_handler():
                    shutdown_event.set()

                for sig in (signal.SIGTERM, signal.SIGINT):
                    loop.add_signal_handler(sig, signal_handler)

                # Run server
                try:
                    await server.run_stdio()
                finally:
                    await server.shutdown()

            if __name__ == "__main__":
                asyncio.run(main())
            ```

        Example (HTTP mode with uvicorn):
            ```python
            import asyncio
            import uvicorn

            async def startup():
                await server.initialize()

            async def shutdown_handler():
                await server.shutdown()

            # Use uvicorn lifecycle events
            config = uvicorn.Config(
                server.asgi_app(),
                host="0.0.0.0",
                port=8000,
            )
            server_instance = uvicorn.Server(config)

            # Uvicorn handles SIGTERM/SIGINT automatically
            await server_instance.serve()
            await shutdown_handler()
            ```
        """
        await self._edda_app.shutdown()

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
