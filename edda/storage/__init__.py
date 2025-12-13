"""Storage layer for Edda framework."""

from edda.storage.notify_base import (
    NoopNotifyListener,
    NotifyProtocol,
    create_notify_listener,
)
from edda.storage.protocol import StorageProtocol
from edda.storage.sqlalchemy_storage import SQLAlchemyStorage

__all__ = [
    "StorageProtocol",
    "SQLAlchemyStorage",
    "NotifyProtocol",
    "NoopNotifyListener",
    "create_notify_listener",
]
