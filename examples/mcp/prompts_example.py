"""
MCP Prompts Example

This example demonstrates how to use MCP prompts with Edda workflows.
Prompts can access workflow state to generate dynamic, context-aware
prompts for AI clients.
"""

import asyncio
import sys
from pathlib import Path

# Import prompt types from MCP
from mcp.server.fastmcp.prompts.base import UserMessage  # type: ignore[import-not-found]
from mcp.types import TextContent  # type: ignore[import-not-found]

from edda import WorkflowContext, activity, workflow
from edda.integrations.mcp import EddaMCPServer

# Create database directory in user's home directory
db_dir = Path.home() / ".edda"
db_dir.mkdir(exist_ok=True)
db_path = db_dir / "mcp_prompts.db"

# Create MCP server
server = EddaMCPServer(
    name="Workflow Analysis Service",
    db_url=f"sqlite+aiosqlite:///{db_path}",
)


# ============================================================================
# Demo Workflows (for testing prompts)
# ============================================================================


@activity
async def process_data(_ctx: WorkflowContext, data: str) -> dict:
    """Process some data."""
    await asyncio.sleep(0.3)
    return {"processed": data.upper(), "length": len(data)}


@activity
async def validate_result(_ctx: WorkflowContext, data: dict) -> dict:
    """Validate processing result."""
    await asyncio.sleep(0.2)
    is_valid = data.get("length", 0) > 0
    return {"valid": is_valid, "data": data}


@server.durable_tool(description="Demo workflow for testing prompts")
@workflow
async def demo_workflow(ctx: WorkflowContext, input_data: str) -> dict:
    """
    Simple demo workflow.

    Args:
        input_data: Input string to process

    Returns:
        Processing result
    """
    # Process data
    processed = await process_data(ctx, input_data)

    # Validate
    result = await validate_result(ctx, processed)

    return result


# ============================================================================
# Prompt Definitions
# ============================================================================


@server.prompt(description="Analyze a completed workflow execution")
async def analyze_workflow(instance_id: str) -> UserMessage:
    """
    Generate a prompt to analyze a specific workflow execution.

    This prompt fetches the workflow state and execution history,
    then generates a detailed analysis prompt for the AI.

    Args:
        instance_id: Workflow instance ID

    Returns:
        UserMessage with analysis prompt
    """
    # Fetch workflow state
    instance = await server.storage.get_instance(instance_id)

    if instance is None:
        text = f"❌ Workflow instance '{instance_id}' not found."
    else:
        # Fetch execution history
        history = await server.storage.get_history(instance_id)

        # Format history
        history_text = "\n".join(
            f"  {i+1}. {h['event_type']}: {h.get('activity_id', 'N/A')}"
            for i, h in enumerate(history)
        )

        # Calculate duration (simplified)
        duration = f"{len(history) * 0.5:.1f} seconds (estimated)"

        text = f"""Analyze this workflow execution:

**Workflow Details:**
- **Name**: {instance['workflow_name']}
- **Instance ID**: {instance_id}
- **Status**: {instance['status']}
- **Activities Executed**: {len(history)}
- **Estimated Duration**: {duration}

**Execution History:**
{history_text}

**Result:**
{instance.get('output_data', 'No output data available')}

**Analysis Request:**
Please provide:
1. A summary of what this workflow did
2. Any unexpected errors or issues
3. Performance characteristics
4. Suggestions for optimization (if any)
"""

    return UserMessage(content=TextContent(type="text", text=text))


@server.prompt(description="Debug a failed workflow")
async def debug_workflow(instance_id: str) -> UserMessage:
    """
    Generate a debugging prompt for a failed workflow.

    Args:
        instance_id: Workflow instance ID

    Returns:
        UserMessage with debugging prompt
    """
    # Fetch workflow state
    instance = await server.storage.get_instance(instance_id)

    if instance is None:
        text = f"❌ Workflow instance '{instance_id}' not found."
    elif instance["status"] != "failed":
        text = f"⚠️ Workflow '{instance_id}' is '{instance['status']}', not failed. Use 'analyze_workflow' instead."
    else:
        # Fetch execution history
        history = await server.storage.get_history(instance_id)

        # Find failed activity
        failed_activity = None
        for h in reversed(history):
            if h.get("event_type") == "ActivityFailed":
                failed_activity = h
                break

        error_details = instance.get("error_details", "No error details available")

        text = f"""Debug this failed workflow:

**Workflow Details:**
- **Name**: {instance['workflow_name']}
- **Instance ID**: {instance_id}
- **Status**: ❌ FAILED
- **Error**: {error_details}

**Failed Activity:**
{failed_activity if failed_activity else 'Not identified'}

**Full Execution History:**
{chr(10).join(f"  {i+1}. {h['event_type']}: {h.get('activity_id', 'N/A')}" for i, h in enumerate(history))}

**Debugging Request:**
Please help me:
1. Identify the root cause of the failure
2. Suggest how to fix the issue
3. Recommend prevention strategies
4. Determine if compensations were executed (if applicable)
"""

    return UserMessage(content=TextContent(type="text", text=text))


@server.prompt(description="Compare two workflow executions side-by-side")
async def compare_workflows(instance_id_a: str, instance_id_b: str) -> UserMessage:
    """
    Generate a prompt to compare two workflow executions.

    Args:
        instance_id_a: First workflow instance ID
        instance_id_b: Second workflow instance ID

    Returns:
        UserMessage with comparison prompt
    """
    # Fetch both workflows
    instance_a = await server.storage.get_instance(instance_id_a)
    instance_b = await server.storage.get_instance(instance_id_b)

    if instance_a is None or instance_b is None:
        missing = []
        if instance_a is None:
            missing.append(instance_id_a)
        if instance_b is None:
            missing.append(instance_id_b)
        text = f"❌ Workflow instance(s) not found: {', '.join(missing)}"
    else:
        # Fetch histories
        history_a = await server.storage.get_history(instance_id_a)
        history_b = await server.storage.get_history(instance_id_b)

        text = f"""Compare these two workflow executions:

**Workflow A:**
- **Instance ID**: {instance_id_a}
- **Name**: {instance_a['workflow_name']}
- **Status**: {instance_a['status']}
- **Activities**: {len(history_a)}

**Workflow B:**
- **Instance ID**: {instance_id_b}
- **Name**: {instance_b['workflow_name']}
- **Status**: {instance_b['status']}
- **Activities**: {len(history_b)}

**Execution History (Workflow A):**
{chr(10).join(f"  {i+1}. {h['event_type']}" for i, h in enumerate(history_a))}

**Execution History (Workflow B):**
{chr(10).join(f"  {i+1}. {h['event_type']}" for i, h in enumerate(history_b))}

**Comparison Request:**
Please identify:
1. Key differences in execution paths
2. Performance differences (number of activities, etc.)
3. Which execution was more efficient
4. Any notable patterns or anomalies
"""

    return UserMessage(content=TextContent(type="text", text=text))


# ============================================================================
# Server Deployment
# ============================================================================


async def main():
    """Initialize and run the MCP server."""
    # Write to stderr to keep stdout clean for JSON-RPC messages
    sys.stderr.write("Starting MCP Server with Prompts (stdio transport)...\n")
    sys.stderr.write("Server name: Workflow Analysis Service\n")
    sys.stderr.write(f"Database: {db_path}\n")
    sys.stderr.write("\n=== Available MCP Tools ===\n")
    sys.stderr.write("  - demo_workflow: Start demo workflow\n")
    sys.stderr.write("  - demo_workflow_status: Check status\n")
    sys.stderr.write("  - demo_workflow_result: Get result\n")
    sys.stderr.write("\n=== Available MCP Prompts ===\n")
    sys.stderr.write("  - analyze_workflow: Analyze a completed workflow\n")
    sys.stderr.write("  - debug_workflow: Debug a failed workflow\n")
    sys.stderr.write("  - compare_workflows: Compare two executions\n")
    sys.stderr.write("\nPress Ctrl+C to stop\n")
    sys.stderr.flush()

    # Initialize EddaApp
    await server.initialize()

    # Run with stdio transport
    await server.run_stdio()


if __name__ == "__main__":
    asyncio.run(main())
