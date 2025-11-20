"""
JSON serialization implementation for Edda framework.

This is the default serializer, using Python's standard library json module.
"""

import json
from typing import Any


class JSONSerializer:
    """
    JSON serializer implementation.

    Uses Python's standard library json module for serialization.
    This is the default and recommended serializer for most use cases.
    """

    @property
    def content_type(self) -> str:
        """Get Content-Type header."""
        return "application/json"

    def serialize(self, data: Any) -> bytes:
        """
        Serialize data to JSON bytes.

        Args:
            data: Data to serialize (must be JSON-serializable)

        Returns:
            UTF-8 encoded JSON bytes

        Raises:
            TypeError: If data is not JSON-serializable
        """
        try:
            json_str = json.dumps(data, ensure_ascii=False, sort_keys=True)
            return json_str.encode("utf-8")
        except (TypeError, ValueError) as e:
            raise ValueError(f"Failed to serialize data to JSON: {e}") from e

    def deserialize(self, data: bytes, _message_type: type[Any] | None = None) -> Any:
        """
        Deserialize JSON bytes to data.

        Args:
            data: UTF-8 encoded JSON bytes
            _message_type: Ignored for JSON serializer

        Returns:
            Deserialized Python data (dict, list, etc.)

        Raises:
            ValueError: If data is not valid JSON
        """
        try:
            json_str = data.decode("utf-8")
            return json.loads(json_str)
        except (UnicodeDecodeError, json.JSONDecodeError) as e:
            raise ValueError(f"Failed to deserialize JSON data: {e}") from e

    def to_dict(self, data: Any) -> dict[str, Any]:
        """
        Convert data to dictionary.

        For JSON serializer, this is typically a no-op if data is already a dict.

        Args:
            data: Data to convert

        Returns:
            Dictionary representation
        """
        if isinstance(data, dict):
            return data
        elif isinstance(data, str):
            # Try to parse as JSON
            try:
                result = json.loads(data)
                if isinstance(result, dict):
                    return result
            except json.JSONDecodeError:
                pass

        # Wrap in dict if not already
        return {"value": data}

    def from_dict(self, data: dict[str, Any], _message_type: type[Any] | None = None) -> Any:
        """
        Convert dictionary to data.

        For JSON serializer, this is typically a no-op.

        Args:
            data: Dictionary representation
            _message_type: Ignored for JSON serializer

        Returns:
            Data (usually just returns the dict)
        """
        return data
