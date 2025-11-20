"""
Base serialization protocol for Edda framework.

This module defines the SerializerProtocol that all serializers must implement.
"""

from typing import Any, Protocol, runtime_checkable


@runtime_checkable
class SerializerProtocol(Protocol):
    """
    Protocol for serialization implementations.

    Serializers are used to encode/decode CloudEvent data payloads.
    Edda supports JSON serialization.
    """

    @property
    def content_type(self) -> str:
        """
        Get the Content-Type header value for this serializer.

        Returns:
            Content-Type string (e.g., "application/json")
        """
        ...

    def serialize(self, data: Any) -> bytes:
        """
        Serialize data to bytes.

        Args:
            data: Data to serialize (typically a dict for JSON)

        Returns:
            Serialized bytes

        Raises:
            ValueError: If data cannot be serialized
        """
        ...

    def deserialize(self, data: bytes, message_type: type[Any] | None = None) -> Any:
        """
        Deserialize bytes to data.

        Args:
            data: Serialized bytes
            message_type: Optional message type (unused for JSON serializer)

        Returns:
            Deserialized data (typically a dict for JSON)

        Raises:
            ValueError: If data cannot be deserialized
        """
        ...

    def to_dict(self, data: Any) -> dict[str, Any]:
        """
        Convert data to dictionary (for storage).

        Args:
            data: Data to convert

        Returns:
            Dictionary representation
        """
        ...

    def from_dict(self, data: dict[str, Any], message_type: type[Any] | None = None) -> Any:
        """
        Convert dictionary to data (from storage).

        Args:
            data: Dictionary representation
            message_type: Optional message type (unused for JSON serializer)

        Returns:
            Reconstructed data
        """
        ...
