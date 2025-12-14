"""
DurableAgent example for Edda + Mirascope V2 integration.

This example demonstrates:
- Using DurableAgent for context-aware conversations
- Dependency injection via llm.Context
- Multi-turn conversations with automatic history management
- RAG-style document injection
- Tool usage with context access

Requirements:
    pip install 'edda-framework[mirascope]'

Environment:
    ANTHROPIC_API_KEY: Your Anthropic API key

Run with:
    uv run python -m examples.mirascope.durable_agent
"""

import asyncio
import os
from dataclasses import dataclass

from edda import EddaApp, WorkflowContext, activity, workflow
from edda.integrations.mirascope import DurableAgent, DurableDeps

# Check for API key
if not os.environ.get("ANTHROPIC_API_KEY"):
    print("Warning: ANTHROPIC_API_KEY not set. Set it before running this example.")
    print("  export ANTHROPIC_API_KEY=your_api_key")


# =============================================================================
# Example 1: Simple Agent with System Prompt
# =============================================================================


@dataclass
class SimpleDeps:
    """Dependencies for simple assistant."""

    system_prompt: str
    user_name: str


class SimpleAssistant(DurableAgent[SimpleDeps]):
    """A simple assistant with customizable system prompt."""

    model = "anthropic/claude-sonnet-4-20250514"

    def build_prompt(self, ctx, message):
        """Build prompt with personalized system message."""
        # Import here to avoid issues when mirascope not installed
        try:
            from mirascope import llm
        except ImportError:
            # Fallback for when mirascope isn't available
            return [{"role": "user", "content": message}]

        return [
            llm.messages.system(
                f"{ctx.deps.system_prompt}\n\nUser's name: {ctx.deps.user_name}"
            ),
            llm.messages.user(message),
        ]


@workflow
async def simple_agent_workflow(ctx: WorkflowContext, question: str) -> str:
    """Workflow using SimpleAssistant."""
    print("\n[Workflow] Starting simple agent workflow")

    deps = SimpleDeps(
        system_prompt="You are a helpful assistant. Be concise and friendly.",
        user_name="Alice",
    )

    agent = SimpleAssistant(ctx)
    response = await agent.chat(deps, question)

    return response["content"]


# =============================================================================
# Example 2: RAG Agent with Document Context
# =============================================================================


@dataclass
class RAGDeps:
    """Dependencies for RAG-style agent."""

    documents: list[str]
    search_index: dict[str, str]


class RAGAssistant(DurableAgent[RAGDeps]):
    """RAG-style assistant with document context."""

    model = "anthropic/claude-sonnet-4-20250514"

    def build_prompt(self, ctx, message):
        """Build prompt with document context."""
        try:
            from mirascope import llm
        except ImportError:
            return [{"role": "user", "content": message}]

        # Format documents for context
        docs_str = "\n---\n".join(
            f"Document {i + 1}:\n{doc}" for i, doc in enumerate(ctx.deps.documents)
        )

        return [
            llm.messages.system(
                f"You are a knowledge assistant. Answer questions based on the "
                f"following documents:\n\n{docs_str}\n\n"
                f"If the answer isn't in the documents, say so."
            ),
            llm.messages.user(message),
        ]


@workflow
async def rag_agent_workflow(ctx: WorkflowContext, question: str) -> str:
    """Workflow using RAG-style agent."""
    print("\n[Workflow] Starting RAG agent workflow")

    # Simulated document retrieval
    deps = RAGDeps(
        documents=[
            "Edda is a durable execution framework for Python. It provides "
            "automatic crash recovery and workflow persistence.",
            "Mirascope is an LLM toolkit that provides a unified interface "
            "for multiple AI providers including Anthropic, OpenAI, and Google.",
            "The Edda + Mirascope integration makes LLM calls durable, meaning "
            "they survive crashes and can be replayed without re-calling the API.",
        ],
        search_index={
            "edda": "Edda is a durable execution framework",
            "mirascope": "Mirascope is an LLM toolkit",
        },
    )

    agent = RAGAssistant(ctx)
    response = await agent.chat(deps, question)

    return response["content"]


# =============================================================================
# Example 3: Multi-turn Conversation Agent
# =============================================================================


@dataclass
class ChatDeps:
    """Dependencies for multi-turn chat."""

    topic: str
    expertise_level: str


class ChatAssistant(DurableAgent[ChatDeps]):
    """Multi-turn conversation assistant."""

    model = "anthropic/claude-sonnet-4-20250514"

    def build_prompt(self, ctx, message):
        """Build prompt including conversation history."""
        try:
            from mirascope import llm
        except ImportError:
            return [{"role": "user", "content": message}]

        # Extract provider from model string for assistant messages
        provider = self.model.split("/")[0] if "/" in self.model else "unknown"

        messages = [
            llm.messages.system(
                f"You are an expert on {ctx.deps.topic}. "
                f"Adjust your explanations for a {ctx.deps.expertise_level} level. "
                f"Keep responses concise but informative."
            ),
        ]

        # Include conversation history (from DurableDeps)
        # Note: history is managed automatically by DurableDeps
        # The parent class's default implementation handles this,
        # but we override to show explicit history handling
        if hasattr(ctx.deps, "history"):
            for msg in ctx.deps.history:
                role = msg.get("role", "user")
                content = msg.get("content", "")
                if role == "assistant":
                    # Mirascope V2: assistant messages require model_id and provider_id
                    messages.append(
                        llm.messages.assistant(
                            content, model_id=self.model, provider_id=provider
                        )
                    )
                elif role == "user":
                    messages.append(llm.messages.user(content))

        # Add current message
        messages.append(llm.messages.user(message))
        return messages


@workflow
async def multi_turn_workflow(
    ctx: WorkflowContext,
    questions: list[str],
) -> list[dict[str, str]]:
    """Workflow with multi-turn conversation."""
    print("\n[Workflow] Starting multi-turn conversation")

    deps_data = ChatDeps(
        topic="durable execution and workflow systems",
        expertise_level="intermediate",
    )

    # Wrap in DurableDeps to track history
    deps = DurableDeps(data=deps_data)

    agent = ChatAssistant(ctx)
    exchanges = []

    for i, question in enumerate(questions, start=1):
        print(f"\n[Turn {i}] User: {question}")
        response = await agent.chat(deps, question)
        answer = response["content"]
        print(f"[Turn {i}] Assistant: {answer[:150]}...")
        exchanges.append({"user": question, "assistant": answer})

    return exchanges


# =============================================================================
# Example 4: Agent with Tool Execution
# =============================================================================


@dataclass
class ToolDeps:
    """Dependencies for tool-using agent."""

    weather_cache: dict[str, str]
    calculator_enabled: bool


# Define tools as regular functions (will be executed via activity)
def get_weather(city: str, weather_cache: dict[str, str]) -> str:
    """Get weather for a city."""
    return weather_cache.get(city, f"Weather data not available for {city}")


def calculate(expression: str) -> str:
    """Evaluate a math expression."""
    try:
        allowed = set("0123456789+-*/.(). ")
        if not all(c in allowed for c in expression):
            return "Error: Invalid characters"
        result = eval(expression)  # noqa: S307
        return str(result)
    except Exception as e:
        return f"Error: {e}"


class ToolAssistant(DurableAgent[ToolDeps]):
    """Assistant that can use tools."""

    model = "anthropic/claude-sonnet-4-20250514"

    def get_tools(self):
        """Provide tools for the agent."""
        # Note: In a real implementation, you'd use @llm.tool() decorated functions
        # For this demo, we'll use simple functions
        return None  # Tools are handled via activity

    def build_prompt(self, ctx, message):
        """Build prompt with tool instructions."""
        try:
            from mirascope import llm
        except ImportError:
            return [{"role": "user", "content": message}]

        tools_available = ["get_weather(city)", "calculate(expression)"]
        if not ctx.deps.calculator_enabled:
            tools_available.remove("calculate(expression)")

        return [
            llm.messages.system(
                f"You are a helpful assistant with access to these tools:\n"
                f"{', '.join(tools_available)}\n\n"
                f"When you need to use a tool, clearly state which tool and "
                f"what arguments you would use."
            ),
            llm.messages.user(message),
        ]


@activity
async def execute_tool(
    ctx: WorkflowContext,  # noqa: ARG001
    tool_name: str,
    tool_args: dict,
    deps: ToolDeps,
) -> str:
    """Execute a tool call as a durable activity."""
    if tool_name == "get_weather":
        return get_weather(tool_args.get("city", ""), deps.weather_cache)
    elif tool_name == "calculate":
        return calculate(tool_args.get("expression", ""))
    else:
        return f"Unknown tool: {tool_name}"


@workflow
async def tool_agent_workflow(ctx: WorkflowContext, query: str) -> str:
    """Workflow using tool-capable agent."""
    print("\n[Workflow] Starting tool agent workflow")

    deps = ToolDeps(
        weather_cache={
            "Tokyo": "Sunny, 22C",
            "London": "Rainy, 15C",
            "New York": "Cloudy, 18C",
        },
        calculator_enabled=True,
    )

    agent = ToolAssistant(ctx)
    response = await agent.chat(deps, query)

    return response["content"]


# =============================================================================
# Main
# =============================================================================


async def main():
    """Main function demonstrating DurableAgent usage."""
    print("=" * 60)
    print("Edda Framework - DurableAgent Example")
    print("=" * 60)

    app = EddaApp(
        service_name="durable-agent-example",
        db_url="sqlite:///durable_agent_demo.db",
    )

    await app.initialize()

    try:
        # Example 1: Simple Agent
        print("\n>>> Example 1: Simple Agent")
        instance_id = await simple_agent_workflow.start(
            question="What is durable execution?"
        )
        instance = await app.storage.get_instance(instance_id)
        output = instance.get("output_data", {}) if instance else {}
        if output.get("result"):
            print(f">>> Result: {output['result'][:200]}...")
        print("-" * 40)

        # Example 2: RAG Agent
        print("\n>>> Example 2: RAG Agent")
        instance_id = await rag_agent_workflow.start(
            question="How does Edda integrate with Mirascope?"
        )
        instance = await app.storage.get_instance(instance_id)
        output = instance.get("output_data", {}) if instance else {}
        if output.get("result"):
            print(f">>> Result: {output['result'][:200]}...")
        print("-" * 40)

        # Example 3: Multi-turn Conversation
        print("\n>>> Example 3: Multi-turn Conversation")
        questions = [
            "What is durable execution?",
            "How does it handle crashes?",
            "Give me a real-world example.",
        ]
        instance_id = await multi_turn_workflow.start(questions=questions)
        instance = await app.storage.get_instance(instance_id)
        output = instance.get("output_data", {}) if instance else {}
        if output.get("result"):
            print(">>> Conversation Summary:")
            for i, exchange in enumerate(output["result"], start=1):
                print(f"Turn {i}:")
                print(f"  User: {exchange['user']}")
                print(f"  Assistant: {exchange['assistant'][:100]}...")
        print("-" * 40)

        # Example 4: Tool Agent
        print("\n>>> Example 4: Tool Agent")
        instance_id = await tool_agent_workflow.start(
            query="What's the weather in Tokyo?"
        )
        instance = await app.storage.get_instance(instance_id)
        output = instance.get("output_data", {}) if instance else {}
        if output.get("result"):
            print(f">>> Result: {output['result'][:200]}...")

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
