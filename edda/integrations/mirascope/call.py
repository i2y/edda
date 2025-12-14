"""
Simple durable LLM call function for Edda + Mirascope V2 integration.

This module provides a straightforward way to make durable LLM calls
without needing to define a separate function with @durable_call.
"""

from __future__ import annotations

from typing import Any

from edda.activity import activity
from edda.context import WorkflowContext

from .types import DurableResponse


def _import_mirascope() -> Any:
    """
    Lazy import Mirascope components.

    Raises:
        ImportError: If mirascope is not installed.
    """
    try:
        from mirascope import llm

        return llm
    except ImportError as e:
        raise ImportError(
            "Mirascope not installed. Install with: pip install 'mirascope[anthropic]' "
            "or pip install 'edda-framework[mirascope]'"
        ) from e


@activity
async def call(
    ctx: WorkflowContext,  # noqa: ARG001 - Required by @activity decorator
    *,
    model: str,
    prompt: str,
    system: str | None = None,
    tools: list[Any] | None = None,
    response_model: type | None = None,
    **call_params: Any,
) -> dict[str, Any]:
    """
    Make a durable LLM call.

    This is a simple, ad-hoc way to make LLM calls within workflows.
    For more complex use cases, consider using the @durable_call decorator.

    Args:
        ctx: Workflow context (automatically provided by Edda).
        model: Model identifier in "provider/model" format
            (e.g., "anthropic/claude-sonnet-4-20250514", "openai/gpt-4").
        prompt: The user prompt/message.
        system: Optional system prompt.
        tools: Optional list of tool functions for function calling.
        response_model: Optional Pydantic model for structured output.
        **call_params: Additional parameters passed to the LLM provider.

    Returns:
        Dictionary representation of DurableResponse.

    Example:
        >>> @workflow
        ... async def my_workflow(ctx: WorkflowContext, question: str) -> str:
        ...     response = await call(
        ...         ctx,
        ...         model="anthropic/claude-sonnet-4-20250514",
        ...         prompt=question,
        ...         system="You are a helpful assistant.",
        ...     )
        ...     return response["content"]
    """
    llm = _import_mirascope()

    # Extract provider from model string (e.g., "anthropic/claude-..." -> "anthropic")
    provider = model.split("/")[0] if "/" in model else "unknown"

    # Build the call function dynamically using V2 API
    @llm.call(model, tools=tools, response_model=response_model, **call_params)  # type: ignore[misc]
    async def _call() -> list[Any]:
        # V2: Use llm.messages.system/user and return list directly
        messages = []
        if system:
            messages.append(llm.messages.system(system))
        messages.append(llm.messages.user(prompt))
        return messages

    # Execute the call
    response = await _call()

    # Convert to serializable format
    return DurableResponse.from_mirascope(response, provider).to_dict()


@activity
async def call_with_messages(
    ctx: WorkflowContext,  # noqa: ARG001 - Required by @activity decorator
    *,
    model: str,
    messages: list[dict[str, str]],
    tools: list[Any] | None = None,
    response_model: type | None = None,
    **call_params: Any,
) -> dict[str, Any]:
    """
    Make a durable LLM call with a full message history.

    This is useful for multi-turn conversations where you need to pass
    the full conversation history.

    Args:
        ctx: Workflow context (automatically provided by Edda).
        model: Model identifier in "provider/model" format
            (e.g., "anthropic/claude-sonnet-4-20250514", "openai/gpt-4").
        messages: List of message dicts with "role" and "content" keys.
        tools: Optional list of tool functions for function calling.
        response_model: Optional Pydantic model for structured output.
        **call_params: Additional parameters passed to the LLM provider.

    Returns:
        Dictionary representation of DurableResponse.

    Example:
        >>> @workflow
        ... async def chat_workflow(ctx: WorkflowContext, history: list[dict]) -> str:
        ...     response = await call_with_messages(
        ...         ctx,
        ...         model="anthropic/claude-sonnet-4-20250514",
        ...         messages=history,
        ...     )
        ...     return response["content"]
    """
    llm = _import_mirascope()

    # Extract provider and model_id from model string
    # e.g., "anthropic/claude-sonnet-4-20250514" -> provider="anthropic", model_id="anthropic/claude-sonnet-4-20250514"
    provider = model.split("/")[0] if "/" in model else "unknown"

    # Convert message dicts to Mirascope V2 message objects
    def convert_messages(msgs: list[dict[str, str]]) -> list[Any]:
        result = []
        for msg in msgs:
            role = msg.get("role", "user")
            content = msg.get("content", "")
            if role == "system":
                result.append(llm.messages.system(content))
            elif role == "assistant":
                # Mirascope V2: assistant messages require model_id and provider_id
                result.append(llm.messages.assistant(content, model_id=model, provider_id=provider))
            else:
                result.append(llm.messages.user(content))
        return result

    @llm.call(model, tools=tools, response_model=response_model, **call_params)  # type: ignore[misc]
    async def _call() -> list[Any]:
        return convert_messages(messages)

    # Execute the call
    response = await _call()

    # Convert to serializable format
    return DurableResponse.from_mirascope(response, provider).to_dict()
