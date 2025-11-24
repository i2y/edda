"""Decorators for MCP durable tools."""

from __future__ import annotations

import inspect
from collections.abc import Callable
from typing import TYPE_CHECKING, Any, cast

from edda.workflow import workflow

if TYPE_CHECKING:
    from edda.integrations.mcp.server import EddaMCPServer
    from edda.workflow import Workflow


def create_durable_tool(
    server: EddaMCPServer,
    func: Callable[..., Any],
    *,
    description: str = "",
) -> Workflow:
    """
    Create a durable workflow tool with auto-generated status/result tools.

    This function:
    1. Wraps the function as an Edda @workflow
    2. Registers three MCP tools:
       - {name}: Start workflow, return instance_id
       - {name}_status: Check workflow status
       - {name}_result: Get workflow result

    Args:
        server: EddaMCPServer instance
        func: Async workflow function
        description: Tool description

    Returns:
        Workflow instance
    """
    # 1. Create Edda workflow
    workflow_instance = cast(Workflow, workflow(func, event_handler=False))
    workflow_name = func.__name__

    # Register in server's workflow registry
    server._workflows[workflow_name] = workflow_instance

    # 2. Generate main tool (start workflow)
    tool_description = description or func.__doc__ or f"Start {workflow_name} workflow"

    # Extract parameters from workflow function (excluding ctx)
    sig = inspect.signature(func)
    params = [
        param
        for name, param in sig.parameters.items()
        if name != "ctx"  # Exclude WorkflowContext parameter
    ]

    # Create the tool function
    async def start_tool(**kwargs: Any) -> dict[str, Any]:
        """
        Start workflow and return instance_id.

        This is the main entry point for the durable tool.
        """
        # Remove 'ctx' if provided by client (workflow will inject it)
        kwargs.pop("ctx", None)

        # Start Edda workflow
        instance_id = await workflow_instance.start(**kwargs)

        # Return MCP-compliant response
        return {
            "content": [
                {
                    "type": "text",
                    "text": (
                        f"Workflow '{workflow_name}' started successfully.\n"
                        f"Instance ID: {instance_id}\n\n"
                        f"Use '{workflow_name}_status' tool with instance_id='{instance_id}' to check progress.\n"
                        f"Use '{workflow_name}_result' tool to get the final result once completed."
                    ),
                }
            ],
            "isError": False,
        }

    # Override the function's signature for introspection (FastMCP uses this for schema generation)
    start_tool.__signature__ = inspect.Signature(parameters=params)  # type: ignore[attr-defined]

    # Register with FastMCP (call as function, not decorator syntax)
    server._mcp.tool(name=workflow_name, description=tool_description)(start_tool)

    # 3. Generate status tool
    status_tool_name = f"{workflow_name}_status"
    status_tool_description = f"Check status of {workflow_name} workflow"

    @server._mcp.tool(name=status_tool_name, description=status_tool_description)  # type: ignore[misc]
    async def status_tool(instance_id: str) -> dict[str, Any]:
        """Check workflow status."""
        try:
            instance = await server._edda_app.storage.get_instance(instance_id)
            if instance is None:
                return {
                    "content": [
                        {
                            "type": "text",
                            "text": f"Workflow instance not found: {instance_id}",
                        }
                    ],
                    "isError": True,
                }

            status = instance["status"]
            current_activity_id = instance.get("current_activity_id", "N/A")

            status_text = (
                f"Workflow Status: {status}\n"
                f"Current Activity: {current_activity_id}\n"
                f"Instance ID: {instance_id}"
            )

            return {
                "content": [{"type": "text", "text": status_text}],
                "isError": False,
            }
        except Exception as e:
            return {
                "content": [
                    {
                        "type": "text",
                        "text": f"Error checking status: {str(e)}",
                    }
                ],
                "isError": True,
            }

    # 4. Generate result tool
    result_tool_name = f"{workflow_name}_result"
    result_tool_description = f"Get result of {workflow_name} workflow (if completed)"

    @server._mcp.tool(name=result_tool_name, description=result_tool_description)  # type: ignore[misc]
    async def result_tool(instance_id: str) -> dict[str, Any]:
        """Get workflow result (if completed)."""
        try:
            instance = await server._edda_app.storage.get_instance(instance_id)
            if instance is None:
                return {
                    "content": [
                        {
                            "type": "text",
                            "text": f"Workflow instance not found: {instance_id}",
                        }
                    ],
                    "isError": True,
                }

            status = instance["status"]

            if status != "completed":
                return {
                    "content": [
                        {
                            "type": "text",
                            "text": f"Workflow not completed yet. Current status: {status}",
                        }
                    ],
                    "isError": True,
                }

            output_data = instance.get("output_data")
            result_text = f"Workflow Result:\n{output_data}"

            return {
                "content": [{"type": "text", "text": result_text}],
                "isError": False,
            }
        except Exception as e:
            return {
                "content": [
                    {
                        "type": "text",
                        "text": f"Error getting result: {str(e)}",
                    }
                ],
                "isError": True,
            }

    return workflow_instance
