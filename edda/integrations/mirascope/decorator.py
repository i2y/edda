"""
Durable LLM call decorator for Edda + Mirascope V2 integration.

This module provides the @durable_call decorator that combines
Mirascope's @llm.call with Edda's @activity for durable LLM calls.
"""

from __future__ import annotations

import functools
import inspect
from collections.abc import Callable
from typing import Any, TypeVar

from edda.activity import activity
from edda.context import WorkflowContext

from .types import DurableResponse

F = TypeVar("F", bound=Callable[..., Any])


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


def durable_call(
    model: str,
    *,
    tools: list[Any] | None = None,
    response_model: type | None = None,
    json_mode: bool = False,
    **call_params: Any,
) -> Callable[[F], F]:
    """
    Decorator that makes an LLM call durable through Edda's activity system.

    This decorator combines Mirascope V2's @llm.call with Edda's @activity,
    providing automatic caching, retry, and crash recovery for LLM calls.

    Args:
        model: Model identifier in "provider/model" format
            (e.g., "anthropic/claude-sonnet-4-20250514", "openai/gpt-4").
        tools: Optional list of tool functions for function calling.
        response_model: Optional Pydantic model for structured output.
        json_mode: Whether to enable JSON mode.
        **call_params: Additional parameters passed to the LLM provider.

    Returns:
        A decorator that transforms the function into a durable LLM call.

    Example:
        Basic usage::

            @durable_call("anthropic/claude-sonnet-4-20250514")
            async def summarize(text: str) -> str:
                return f"Summarize this text: {text}"

            @workflow
            async def my_workflow(ctx: WorkflowContext, text: str) -> str:
                response = await summarize(ctx, text)
                return response["content"]

        With tools::

            def get_weather(city: str) -> str:
                '''Get the weather for a city.'''
                return f"Sunny in {city}"

            @durable_call(
                "anthropic/claude-sonnet-4-20250514",
                tools=[get_weather],
            )
            async def weather_assistant(query: str) -> str:
                return query

        With structured output::

            class BookInfo(BaseModel):
                title: str
                author: str
                year: int

            @durable_call(
                "anthropic/claude-sonnet-4-20250514",
                response_model=BookInfo,
            )
            async def extract_book_info(text: str) -> str:
                return f"Extract book information from: {text}"

    Note:
        - The decorated function must return a string (the prompt).
        - When called, the first argument must be the WorkflowContext.
        - The response is returned as a dictionary (DurableResponse.to_dict()).
    """
    llm = _import_mirascope()

    # Extract provider from model string (e.g., "anthropic/claude-..." -> "anthropic")
    provider = model.split("/")[0] if "/" in model else "unknown"

    def decorator(func: F) -> F:
        # Apply Mirascope V2's @llm.call decorator with unified model string
        mirascope_decorated = llm.call(
            model,
            tools=tools,
            response_model=response_model,
            json_mode=json_mode,
            **call_params,
        )(func)

        # Determine if the original function is async
        is_async = inspect.iscoroutinefunction(func)

        @activity
        @functools.wraps(func)
        async def async_wrapper(
            ctx: WorkflowContext,  # noqa: ARG001 - Required by @activity decorator
            *args: Any,
            **kwargs: Any,
        ) -> dict[str, Any]:
            # Call the Mirascope-decorated function
            if is_async or inspect.iscoroutinefunction(mirascope_decorated):
                response = await mirascope_decorated(*args, **kwargs)
            else:
                response = mirascope_decorated(*args, **kwargs)

            # Handle structured output (response_model)
            # For structured output, the response is the Pydantic model itself
            if response_model is not None and hasattr(response, "model_dump"):
                return {
                    "content": "",
                    "model": model,
                    "provider": provider,
                    "structured_output": response.model_dump(),
                }

            # Convert to serializable format
            return DurableResponse.from_mirascope(response, provider).to_dict()

        # Store metadata for introspection
        async_wrapper._mirascope_func = mirascope_decorated  # type: ignore[union-attr]
        async_wrapper._provider = provider  # type: ignore[union-attr]
        async_wrapper._model = model  # type: ignore[union-attr]
        async_wrapper._tools = tools  # type: ignore[union-attr]
        async_wrapper._response_model = response_model  # type: ignore[union-attr]

        return async_wrapper  # type: ignore[return-value]

    return decorator
