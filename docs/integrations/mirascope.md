# Mirascope Integration

Edda provides seamless integration with [Mirascope](https://mirascope.com/), making your LLM calls **durable**. This means LLM responses are automatically cached and can survive crashes, restarts, and replays without re-calling the API.

## Overview

When building AI-powered workflows, LLM calls are often the most expensive and time-consuming operations. Edda's Mirascope integration provides:

- **Crash Recovery**: If your workflow crashes mid-execution, completed LLM calls are replayed from cache
- **Cost Savings**: Replaying a workflow doesn't re-call the LLM API (no duplicate charges)
- **Deterministic Replay**: Same inputs always produce same outputs during replay
- **Multi-turn Conversations**: Automatic conversation history management with `DurableAgent`

## Installation

```bash
pip install 'edda-framework[mirascope]'

# Or using uv
uv add edda-framework --extra mirascope
```

You'll also need to set your LLM provider's API key:

```bash
export ANTHROPIC_API_KEY=your_api_key
# or
export OPENAI_API_KEY=your_api_key
```

## Quick Start

Here's a minimal example using the `@durable_call` decorator:

```python
from edda import EddaApp, workflow, WorkflowContext
from edda.integrations.mirascope import durable_call

# Define a durable LLM call
@durable_call("anthropic/claude-sonnet-4-20250514")
async def summarize(text: str) -> str:
    """Summarize the given text."""
    return f"Please summarize this text in 2 sentences:\n\n{text}"

# Use it in a workflow
@workflow
async def summarize_workflow(ctx: WorkflowContext, text: str) -> str:
    response = await summarize(ctx, text)
    return response["content"]

# Run the workflow
async def main():
    app = EddaApp(service_name="summarizer", db_url="sqlite:///app.db")
    await app.initialize()

    # start() runs the workflow to completion and returns the instance ID
    instance_id = await summarize_workflow.start(text="Long article here...")

    # If the app crashes mid-workflow, use resume() to continue:
    # await summarize_workflow.resume(instance_id)
```

## Choosing the Right API

Edda provides three ways to make durable LLM calls:

| Use Case | Recommended API | Description |
|----------|-----------------|-------------|
| **Reusable LLM function** | `@durable_call` | Best for defining prompt templates you'll reuse |
| **One-off LLM call** | `call()` | Best for ad-hoc calls where you build the prompt dynamically |
| **Multi-turn conversation** | `DurableAgent` | Best for chat-style interactions with history management |
| **RAG with context** | `DurableAgent` | Best when injecting retrieved documents into prompts |

## @durable_call Decorator

The `@durable_call` decorator is the most common way to define durable LLM calls. It wraps a function that returns a prompt string.

### Basic Usage

```python
from edda.integrations.mirascope import durable_call

@durable_call("anthropic/claude-sonnet-4-20250514")
async def translate(text: str, target_language: str) -> str:
    """Translate text to the target language."""
    return f"Translate the following to {target_language}:\n\n{text}"

# In a workflow:
@workflow
async def translation_workflow(ctx: WorkflowContext, text: str) -> str:
    response = await translate(ctx, text, "Japanese")
    return response["content"]
```

### With Tools (Function Calling)

```python
from mirascope import llm

@llm.tool
def get_weather(city: str) -> str:
    """Get the current weather for a city."""
    # Your weather API call here
    return f"Sunny, 22°C in {city}"

@llm.tool
def calculate(expression: str) -> str:
    """Evaluate a mathematical expression."""
    return str(eval(expression))

@durable_call(
    "anthropic/claude-sonnet-4-20250514",
    tools=[get_weather, calculate],
)
async def assistant(query: str) -> str:
    """An assistant that can check weather and do math."""
    return query

# In a workflow:
@workflow
async def assistant_workflow(ctx: WorkflowContext, query: str) -> dict:
    response = await assistant(ctx, query)

    # Check if the LLM wants to use tools
    if response.get("tool_calls"):
        for tc in response["tool_calls"]:
            print(f"Tool: {tc['name']}, Args: {tc['args']}")

    return response
```

### With Structured Output

Use Pydantic models to get structured responses:

```python
from pydantic import BaseModel

class BookInfo(BaseModel):
    title: str
    author: str
    year: int
    summary: str

@durable_call(
    "anthropic/claude-sonnet-4-20250514",
    response_model=BookInfo,
)
async def extract_book_info(text: str) -> str:
    """Extract book information from text."""
    return f"Extract book information from:\n\n{text}"

# In a workflow:
@workflow
async def extraction_workflow(ctx: WorkflowContext, text: str) -> dict:
    response = await extract_book_info(ctx, text)
    book = response["structured_output"]  # BookInfo as dict
    return book
```

## call() and call_with_messages()

For ad-hoc LLM calls where you don't need a reusable function:

### Simple Call

```python
from edda.integrations.mirascope import call

@workflow
async def qa_workflow(ctx: WorkflowContext, question: str) -> str:
    response = await call(
        ctx,
        model="anthropic/claude-sonnet-4-20250514",
        prompt=question,
        system="You are a helpful assistant. Be concise.",
    )
    return response["content"]
```

### With Message History

```python
from edda.integrations.mirascope import call_with_messages

@workflow
async def chat_workflow(ctx: WorkflowContext, messages: list[dict]) -> str:
    response = await call_with_messages(
        ctx,
        model="anthropic/claude-sonnet-4-20250514",
        messages=messages,
    )
    return response["content"]

# Usage:
messages = [
    {"role": "system", "content": "You are a helpful assistant."},
    {"role": "user", "content": "What is Python?"},
    {"role": "assistant", "content": "Python is a programming language."},
    {"role": "user", "content": "What makes it popular?"},
]
await chat_workflow.start(messages=messages)
```

## DurableAgent Class

For complex scenarios like multi-turn conversations or RAG, use the `DurableAgent` class:

### Why Class-Based?

- **Automatic history management**: Conversation history is tracked in `DurableDeps`
- **Dependency injection**: Inject documents, search results, or other context via `build_prompt()`
- **Each turn is durable**: Every `chat()` call is a separate cached activity

### Basic Multi-Turn Conversation

```python
from dataclasses import dataclass
from edda.integrations.mirascope import DurableAgent, DurableDeps

@dataclass
class ChatDeps:
    system_prompt: str
    user_name: str

class ChatAssistant(DurableAgent[ChatDeps]):
    model = "anthropic/claude-sonnet-4-20250514"

    def build_prompt(self, ctx, message):
        from mirascope import llm
        return [
            llm.messages.system(
                f"{ctx.deps.system_prompt}\nUser's name: {ctx.deps.user_name}"
            ),
            # History is automatically included by parent class
            llm.messages.user(message),
        ]

@workflow
async def chat_workflow(ctx: WorkflowContext, questions: list[str]) -> list[str]:
    deps_data = ChatDeps(
        system_prompt="You are a helpful assistant.",
        user_name="Alice",
    )
    # DurableDeps wraps your data and tracks conversation history
    deps = DurableDeps(data=deps_data)

    agent = ChatAssistant(ctx)
    answers = []

    for question in questions:
        response = await agent.chat(deps, question)
        answers.append(response["content"])
        # History is automatically updated in deps

    return answers
```

### RAG Pattern

Inject retrieved documents into the prompt:

```python
@dataclass
class RAGDeps:
    documents: list[str]
    query: str

class RAGAssistant(DurableAgent[RAGDeps]):
    model = "anthropic/claude-sonnet-4-20250514"

    def build_prompt(self, ctx, message):
        from mirascope import llm

        # Format documents for context
        docs_str = "\n---\n".join(
            f"Document {i+1}:\n{doc}"
            for i, doc in enumerate(ctx.deps.documents)
        )

        return [
            llm.messages.system(
                f"Answer based on these documents:\n\n{docs_str}\n\n"
                f"If the answer isn't in the documents, say so."
            ),
            llm.messages.user(message),
        ]

@workflow
async def rag_workflow(ctx: WorkflowContext, query: str) -> str:
    # In real usage, retrieve documents from a vector database
    docs = [
        "Edda is a durable execution framework for Python.",
        "Mirascope provides a unified LLM interface.",
    ]

    deps = RAGDeps(documents=docs, query=query)
    agent = RAGAssistant(ctx)
    response = await agent.chat(deps, query)
    return response["content"]
```

## How It Works

When you use any of the durable LLM APIs, here's what happens:

```
Workflow Execution
       │
       ▼
┌──────────────────┐
│  @durable_call   │  ← Decorator wraps your function
│  or call()       │
└────────┬─────────┘
         │
         ▼
┌──────────────────┐
│  Edda @activity  │  ← Makes the call a durable activity
└────────┬─────────┘
         │
         ▼
┌──────────────────┐     ┌─────────────┐
│  Check Cache     │────▶│ Cache Hit?  │
└────────┬─────────┘     └──────┬──────┘
         │                      │
         │ No                   │ Yes
         ▼                      ▼
┌──────────────────┐     ┌─────────────────┐
│  Call LLM API    │     │ Return Cached   │
│  (Mirascope)     │     │ Response        │
└────────┬─────────┘     └─────────────────┘
         │
         ▼
┌──────────────────┐
│  Cache Response  │
│  in Database     │
└──────────────────┘
```

**On Replay**: If the workflow is resumed after a crash, completed LLM calls are replayed from the cache. The LLM API is not called again.

## Response Format

All durable LLM calls return a dictionary with these fields:

| Field | Type | Description |
|-------|------|-------------|
| `content` | `str` | The text response from the LLM |
| `model` | `str` | The model that was used |
| `provider` | `str` | The provider (anthropic, openai, etc.) |
| `tool_calls` | `list[dict]` or `None` | Tool calls requested by the LLM |
| `usage` | `dict` or `None` | Token usage statistics |
| `structured_output` | `dict` or `None` | Parsed response when using `response_model` |

## Supported Providers

Mirascope supports multiple LLM providers. Use the `provider/model` format:

| Provider | Example Model String |
|----------|---------------------|
| Anthropic | `anthropic/claude-sonnet-4-20250514` |
| OpenAI | `openai/gpt-4` |
| Google | `google/gemini-pro` |
| Mistral | `mistral/mistral-large-latest` |

Set the appropriate API key environment variable for your provider.

## Related Documentation

- [Workflows and Activities](../core-features/workflows-activities.md) - Core concepts of durable execution
- [Durable Execution Replay](../core-features/durable-execution/replay.md) - How replay works
- [Mirascope Documentation](https://mirascope.com/docs) - Mirascope's official docs
- [Examples](https://github.com/i2y/edda/tree/main/examples/mirascope) - Complete working examples
