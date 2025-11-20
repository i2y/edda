"""Serialization layer for Edda framework."""

from edda.serialization.base import SerializerProtocol
from edda.serialization.json import JSONSerializer

__all__ = [
    "SerializerProtocol",
    "JSONSerializer",
]
