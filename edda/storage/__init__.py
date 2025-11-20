"""Storage layer for Edda framework."""

from edda.storage.protocol import StorageProtocol
from edda.storage.sqlalchemy_storage import SQLAlchemyStorage

__all__ = [
    "StorageProtocol",
    "SQLAlchemyStorage",
]
