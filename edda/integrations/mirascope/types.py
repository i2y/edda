"""
Type definitions for Edda + Mirascope integration.

This module provides serializable response types that bridge
Mirascope's response objects with Edda's activity system.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass
class DurableResponse:
    """
    Serializable representation of a Mirascope LLM response.

    This class captures the essential parts of an LLM response
    in a JSON-serializable format for Edda's activity caching.

    Attributes:
        content: The text content of the response.
        model: The model identifier used for the call.
        provider: The provider name (e.g., "anthropic", "openai").
        usage: Token usage statistics (input, output, total).
        tool_calls: List of tool calls requested by the model.
        stop_reason: The reason the model stopped generating.
        raw: Raw response data for debugging/advanced use.
    """

    content: str
    model: str
    provider: str
    usage: dict[str, int] | None = None
    tool_calls: list[dict[str, Any]] | None = None
    stop_reason: str | None = None
    raw: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        """Convert to JSON-serializable dictionary."""
        return {
            "content": self.content,
            "model": self.model,
            "provider": self.provider,
            "usage": self.usage,
            "tool_calls": self.tool_calls,
            "stop_reason": self.stop_reason,
            "raw": self.raw,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> DurableResponse:
        """Create from dictionary (for replay)."""
        return cls(
            content=data.get("content", ""),
            model=data.get("model", ""),
            provider=data.get("provider", ""),
            usage=data.get("usage"),
            tool_calls=data.get("tool_calls"),
            stop_reason=data.get("stop_reason"),
            raw=data.get("raw", {}),
        )

    @classmethod
    def _extract_content(cls, response: Any) -> str:
        """
        Extract text content from a Mirascope response.

        Handles Mirascope V2's response format where content can be:
        - A plain string
        - A list of Text/ContentBlock objects with .text attribute
        - None

        Args:
            response: The Mirascope CallResponse object.

        Returns:
            The extracted text content as a string.
        """
        if not hasattr(response, "content"):
            return str(response)

        content = response.content
        if content is None:
            return ""
        if isinstance(content, str):
            return content

        # Handle Mirascope V2's list of Text/ContentBlock objects
        # e.g., [Text(type='text', text='Hello!')]
        if isinstance(content, list):
            text_parts = []
            for item in content:
                if hasattr(item, "text"):
                    text_parts.append(item.text)
                elif isinstance(item, str):
                    text_parts.append(item)
                else:
                    text_parts.append(str(item))
            return "".join(text_parts)

        return str(content)

    @classmethod
    def _extract_model(cls, response: Any) -> str:
        """
        Extract model string from a Mirascope response.

        Handles Mirascope V2 where response.model is a Model object,
        not a string. Use model_id for the string version.

        Args:
            response: The Mirascope CallResponse object.

        Returns:
            The model identifier as a string.
        """
        # Mirascope V2: use model_id (string) instead of model (Model object)
        if hasattr(response, "model_id"):
            return str(response.model_id)

        # Fallback: try model attribute
        model = getattr(response, "model", "")
        if isinstance(model, str):
            return model

        # If model is an object, try to get a string representation
        return str(model) if model else ""

    @classmethod
    def _extract_usage(cls, response: Any) -> dict[str, Any] | None:
        """
        Extract usage statistics from a Mirascope response.

        Handles Mirascope V2 where usage may be in response.raw.usage
        instead of response.usage.

        Args:
            response: The Mirascope CallResponse object.

        Returns:
            Usage statistics as a dict, or None if not available.
        """
        usage = None

        # Try direct usage attribute first
        if hasattr(response, "usage") and response.usage is not None:
            if hasattr(response.usage, "model_dump"):
                usage = response.usage.model_dump()
            elif isinstance(response.usage, dict):
                usage = response.usage

        # Mirascope V2: try response.raw.usage
        if usage is None and hasattr(response, "raw") and response.raw is not None:
            raw = response.raw
            if hasattr(raw, "usage") and raw.usage is not None:
                if hasattr(raw.usage, "model_dump"):
                    usage = raw.usage.model_dump()
                elif isinstance(raw.usage, dict):
                    usage = raw.usage

        return usage

    @classmethod
    def _extract_stop_reason(cls, response: Any) -> str | None:
        """
        Extract stop reason from a Mirascope response.

        Handles various attribute names across different providers
        and Mirascope versions.

        Args:
            response: The Mirascope CallResponse object.

        Returns:
            The stop reason as a string, or None if not available.
        """
        # Try common attribute names
        stop_reason = getattr(response, "stop_reason", None)
        if stop_reason is None:
            stop_reason = getattr(response, "finish_reason", None)

        # Mirascope V2: try response.raw.stop_reason
        if stop_reason is None and hasattr(response, "raw") and response.raw is not None:
            stop_reason = getattr(response.raw, "stop_reason", None)
            if stop_reason is None:
                stop_reason = getattr(response.raw, "finish_reason", None)

        return stop_reason

    @classmethod
    def _parse_tool_args(cls, args: Any) -> dict[str, Any]:
        """
        Parse tool arguments from various formats.

        Mirascope V2 returns args as a JSON string (e.g., '{"city": "Tokyo"}'),
        while we need a dict for execution.

        Args:
            args: Tool arguments (string, dict, or None).

        Returns:
            Parsed arguments as a dict.
        """
        import json

        if args is None:
            return {}
        if isinstance(args, dict):
            return args
        if isinstance(args, str):
            try:
                parsed = json.loads(args)
                return parsed if isinstance(parsed, dict) else {}
            except json.JSONDecodeError:
                return {}
        return {}

    @classmethod
    def from_mirascope(cls, response: Any, provider: str) -> DurableResponse:
        """
        Convert a Mirascope response to DurableResponse.

        Args:
            response: The Mirascope CallResponse object.
            provider: The provider name (e.g., "anthropic").

        Returns:
            A DurableResponse instance with serializable data.
        """
        # Extract tool calls if available
        tool_calls = None
        if hasattr(response, "tool_calls") and response.tool_calls:
            tool_calls = []
            for tc in response.tool_calls:
                if hasattr(tc, "model_dump"):
                    tc_dict = tc.model_dump()
                    # Ensure args is a dict, not a JSON string
                    tc_dict["args"] = cls._parse_tool_args(tc_dict.get("args"))
                    tool_calls.append(tc_dict)
                elif isinstance(tc, dict):
                    tc["args"] = cls._parse_tool_args(tc.get("args"))
                    tool_calls.append(tc)
                else:
                    # Fallback: extract common attributes
                    raw_args = getattr(tc, "args", None) or getattr(tc, "arguments", {})
                    tool_calls.append(
                        {
                            "name": getattr(tc, "name", None) or getattr(tc, "tool_name", None),
                            "args": cls._parse_tool_args(raw_args),
                            "id": getattr(tc, "id", None) or getattr(tc, "tool_call_id", None),
                        }
                    )

        return cls(
            content=cls._extract_content(response),
            model=cls._extract_model(response),
            provider=provider,
            usage=cls._extract_usage(response),
            tool_calls=tool_calls,
            stop_reason=cls._extract_stop_reason(response),
        )

    @property
    def has_tool_calls(self) -> bool:
        """Check if the response contains tool calls."""
        return bool(self.tool_calls)
