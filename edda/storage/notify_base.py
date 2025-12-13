"""Base classes and protocols for notification systems.

This module defines the NotifyProtocol interface and provides a NoopNotifyListener
implementation for databases that don't support LISTEN/NOTIFY (SQLite, MySQL).
"""

from __future__ import annotations

import logging
from collections.abc import Awaitable, Callable
from typing import Protocol, runtime_checkable

logger = logging.getLogger(__name__)


NotifyCallback = Callable[[str], Awaitable[None]]
"""Type alias for notification callback functions.

The callback receives the payload string from the notification.
"""


@runtime_checkable
class NotifyProtocol(Protocol):
    """Protocol for notification systems.

    This protocol defines the interface for LISTEN/NOTIFY style notification
    systems. Implementations should handle:
    - Connection management with automatic reconnection
    - Channel subscription/unsubscription
    - Callback dispatch on notification receipt
    """

    async def start(self) -> None:
        """Start the notification listener.

        Establishes the connection and begins listening for notifications.
        Should be called before any subscribe() calls.
        """
        ...

    async def stop(self) -> None:
        """Stop the notification listener.

        Closes the connection and cleans up resources.
        All subscriptions are automatically removed.
        """
        ...

    async def subscribe(self, channel: str, callback: NotifyCallback) -> None:
        """Subscribe to notifications on a channel.

        Args:
            channel: The channel name to listen on.
            callback: Async function called when a notification arrives.
                     Receives the payload string as its argument.
        """
        ...

    async def unsubscribe(self, channel: str) -> None:
        """Unsubscribe from notifications on a channel.

        Args:
            channel: The channel name to stop listening on.
        """
        ...

    async def notify(self, channel: str, payload: str) -> None:
        """Send a notification on a channel.

        Args:
            channel: The channel name to send the notification on.
            payload: The payload string (typically JSON, max ~7500 bytes for PostgreSQL).
        """
        ...

    @property
    def is_connected(self) -> bool:
        """Check if the listener is currently connected."""
        ...


class NoopNotifyListener:
    """No-op implementation of NotifyProtocol for SQLite/MySQL.

    This implementation does nothing - all methods are no-ops.
    When using SQLite or MySQL, the application falls back to polling-based
    updates with the default intervals (no reduction in polling frequency).
    """

    def __init__(self) -> None:
        """Initialize the no-op listener."""
        self._connected = False

    async def start(self) -> None:
        """No-op start - does nothing."""
        self._connected = True
        logger.debug("NoopNotifyListener started (no-op)")

    async def stop(self) -> None:
        """No-op stop - does nothing."""
        self._connected = False
        logger.debug("NoopNotifyListener stopped (no-op)")

    async def subscribe(self, channel: str, _callback: NotifyCallback) -> None:
        """No-op subscribe - callbacks will never be called.

        Args:
            channel: Ignored.
            _callback: Ignored - will never be called.
        """
        logger.debug(f"NoopNotifyListener: subscribe to '{channel}' (no-op)")

    async def unsubscribe(self, channel: str) -> None:
        """No-op unsubscribe.

        Args:
            channel: Ignored.
        """
        logger.debug(f"NoopNotifyListener: unsubscribe from '{channel}' (no-op)")

    async def notify(self, channel: str, payload: str) -> None:
        """No-op notify - does nothing.

        Args:
            channel: Ignored.
            payload: Ignored.
        """
        # Intentionally silent - this is called frequently during normal operation
        pass

    @property
    def is_connected(self) -> bool:
        """Always returns the internal connected state."""
        return self._connected


def create_notify_listener(db_url: str) -> NotifyProtocol:
    """Create appropriate notify listener based on database type.

    Args:
        db_url: Database connection URL.

    Returns:
        PostgresNotifyListener for PostgreSQL, NoopNotifyListener for others.

    Example:
        >>> listener = create_notify_listener("postgresql://localhost/db")
        >>> await listener.start()
        >>> await listener.subscribe("my_channel", handle_notification)
    """
    if db_url.startswith("postgresql"):
        # Import here to avoid requiring asyncpg when not using PostgreSQL
        from edda.storage.pg_notify import PostgresNotifyListener

        return PostgresNotifyListener(dsn=db_url)
    else:
        logger.info(
            "Database URL does not start with 'postgresql', "
            "using NoopNotifyListener (polling-based updates)"
        )
        return NoopNotifyListener()
