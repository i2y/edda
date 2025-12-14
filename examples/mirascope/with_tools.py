"""
Tool use example for Edda + Mirascope V2 integration.

This example demonstrates:
- Defining tools for LLM function calling
- Using @durable_call with tools
- Handling tool calls in workflows

Requirements:
    pip install 'edda-framework[mirascope]'

Environment:
    ANTHROPIC_API_KEY: Your Anthropic API key

Run with:
    uv run python -m examples.mirascope.with_tools
"""

import asyncio
import os
from datetime import datetime

from mirascope import llm

from edda import EddaApp, WorkflowContext, activity, workflow
from edda.integrations.mirascope import durable_call

# Check for API key
if not os.environ.get("ANTHROPIC_API_KEY"):
    print("Warning: ANTHROPIC_API_KEY not set. Set it before running this example.")
    print("  export ANTHROPIC_API_KEY=your_api_key")


# Define tools with @llm.tool decorator (Mirascope V2)
@llm.tool
def get_current_time() -> str:
    """Get the current date and time."""
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


@llm.tool
def get_weather(city: str) -> str:
    """
    Get the weather for a city.

    Args:
        city: The name of the city to get weather for.
    """
    # In a real application, this would call a weather API
    weather_data = {
        "Tokyo": "Sunny, 22C",
        "London": "Cloudy, 15C",
        "New York": "Rainy, 18C",
        "Paris": "Partly cloudy, 20C",
    }
    return weather_data.get(city, f"Weather data not available for {city}")


@llm.tool
def calculate(expression: str) -> str:
    """
    Evaluate a mathematical expression.

    Args:
        expression: A mathematical expression to evaluate (e.g., "2 + 2").
    """
    try:
        # Simple and safe evaluation for basic math
        allowed_chars = set("0123456789+-*/.(). ")
        if not all(c in allowed_chars for c in expression):
            return "Error: Invalid characters in expression"
        result = eval(expression)  # noqa: S307 - Safe for demo with allowed chars
        return str(result)
    except Exception as e:
        return f"Error: {e}"


# Create a durable LLM call with tools
@durable_call(
    "anthropic/claude-sonnet-4-20250514",
    tools=[get_current_time, get_weather, calculate],
)
async def assistant_with_tools(query: str) -> str:
    """An assistant that can use tools to answer questions."""
    return query


# Activity to execute tool calls
@activity
async def execute_tool_call(
    ctx: WorkflowContext,  # noqa: ARG001
    tool_name: str,
    tool_args: dict,
) -> str:
    """Execute a tool call and return the result."""
    tools = {
        "get_current_time": get_current_time,
        "get_weather": get_weather,
        "calculate": calculate,
    }

    if tool_name not in tools:
        return f"Unknown tool: {tool_name}"

    tool_func = tools[tool_name]
    if tool_args:
        return tool_func(**tool_args)
    else:
        return tool_func()


@workflow
async def tool_use_workflow(ctx: WorkflowContext, user_query: str) -> str:
    """
    Workflow that demonstrates tool use with durable LLM calls.

    The workflow:
    1. Sends the query to the LLM with available tools
    2. If the LLM wants to use tools, executes them
    3. Returns the final response
    """
    print(f"\n[Workflow] Processing query: {user_query}")

    # First LLM call - may return tool_calls
    response = await assistant_with_tools(ctx, user_query)

    # Check if the LLM wants to use tools
    if response.get("tool_calls"):
        print("[Workflow] LLM requested tool calls:")
        tool_results = []

        for tc in response["tool_calls"]:
            tool_name = tc.get("name", "")
            tool_args = tc.get("args", {})
            print(f"  - {tool_name}({tool_args})")

            # Execute each tool call as a separate durable activity
            result = await execute_tool_call(ctx, tool_name, tool_args)
            print(f"    Result: {result}")
            tool_results.append({"tool": tool_name, "result": result})

        # In a full implementation, you would send tool results back to the LLM
        # For this demo, we just return the tool results
        return f"Tool results: {tool_results}"

    # No tool calls - return the direct response
    return response.get("content", "No response")


async def main():
    """Main function to demonstrate tool use."""
    print("=" * 60)
    print("Edda Framework - Tool Use Example")
    print("=" * 60)

    # Create Edda app
    app = EddaApp(
        service_name="tools-example",
        db_url="sqlite:///tools_demo.db",
    )

    # Initialize the app
    await app.initialize()

    try:
        # Test queries that may trigger tool use
        queries = [
            "What time is it now?",
            "What's the weather like in Tokyo?",
            "Calculate 25 * 4 + 10",
        ]

        for query in queries:
            print(f"\n>>> Query: {query}")
            # start() runs the workflow to completion
            instance_id = await tool_use_workflow.start(user_query=query)

            # Get the result from storage
            instance = await app.storage.get_instance(instance_id)
            output = instance.get("output_data", {}) if instance else {}
            if output.get("result"):
                print(f">>> Result: {output['result']}")

            print("-" * 40)

    except Exception as e:
        print(f"\n>>> Error: {e}")
        print(">>> Make sure ANTHROPIC_API_KEY is set correctly.")

    finally:
        await app.shutdown()
        print("\n" + "=" * 60)
        print("Example completed!")
        print("=" * 60)


if __name__ == "__main__":
    asyncio.run(main())
