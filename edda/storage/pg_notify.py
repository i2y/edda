"""PostgreSQL LISTEN/NOTIFY implementation using asyncpg.

This module provides a dedicated listener for PostgreSQL's LISTEN/NOTIFY
mechanism, enabling near-instant notification delivery for workflow events.
"""

from __future__ import annotations

import asyncio
import json
import logging
from collections.abc import Awaitable, Callable
from contextlib import suppress
from typing import Any

logger = logging.getLogger(__name__)


NotifyCallback = Callable[[str], Awaitable[None]]


class PostgresNotifyListener:
    """PostgreSQL LISTEN/NOTIFY listener using asyncpg.

    This class maintains a dedicated connection for LISTEN/NOTIFY operations.
    It provides:
    - Automatic reconnection on connection loss
    - Channel subscription management
    - Callback dispatch for notifications

    Example:
        >>> listener = PostgresNotifyListener(dsn="postgresql://localhost/db")
        >>> await listener.start()
        >>> await listener.subscribe("my_channel", handle_notification)
        >>> # ... later
        >>> await listener.stop()
    """

    def __init__(
        self,
        dsn: str,
        reconnect_interval: float = 5.0,
        max_reconnect_attempts: int | None = None,
    ) -> None:
        """Initialize the PostgreSQL notify listener.

        Args:
            dsn: PostgreSQL connection string (postgresql://user:pass@host/db).
            reconnect_interval: Seconds to wait between reconnection attempts.
            max_reconnect_attempts: Maximum number of reconnection attempts.
                                   None means unlimited.
        """
        self._dsn = dsn
        self._reconnect_interval = reconnect_interval
        self._max_reconnect_attempts = max_reconnect_attempts

        self._connection: Any = None  # asyncpg.Connection
        self._callbacks: dict[str, list[NotifyCallback]] = {}
        self._channel_handlers: dict[str, Callable[..., None]] = {}
        self._running = False
        self._reconnect_task: asyncio.Task[None] | None = None
        self._lock = asyncio.Lock()

    async def start(self) -> None:
        """Start the notification listener.

        Establishes the connection and begins listening for notifications.
        Starts the automatic reconnection task.

        Raises:
            ImportError: If asyncpg is not installed.
        """
        if self._running:
            logger.warning("PostgresNotifyListener already running")
            return

        self._running = True
        await self._establish_connection()

        # Start reconnection monitor
        self._reconnect_task = asyncio.create_task(self._reconnect_loop())
        logger.info("PostgresNotifyListener started")

    async def stop(self) -> None:
        """Stop the notification listener.

        Closes the connection and stops the reconnection task.
        """
        self._running = False

        # Cancel reconnection task
        if self._reconnect_task is not None:
            self._reconnect_task.cancel()
            with suppress(asyncio.CancelledError):
                await self._reconnect_task
            self._reconnect_task = None

        # Close connection
        await self._close_connection()
        self._callbacks.clear()
        logger.info("PostgresNotifyListener stopped")

    async def subscribe(self, channel: str, callback: NotifyCallback) -> None:
        """Subscribe to notifications on a channel.

        Args:
            channel: The PostgreSQL channel name to listen on.
            callback: Async function called when a notification arrives.

        Note:
            Channel names must be valid PostgreSQL identifiers (max 63 chars).
            Multiple callbacks can be registered for the same channel.
        """
        async with self._lock:
            is_new_channel = channel not in self._callbacks

            if channel not in self._callbacks:
                self._callbacks[channel] = []
            self._callbacks[channel].append(callback)

            # Register listener if this is a new channel and we're connected
            if is_new_channel and self._connection is not None:
                try:
                    await self._connection.add_listener(
                        channel, self._create_notification_handler(channel)
                    )
                    logger.debug(f"Subscribed to channel: {channel}")
                except Exception as e:
                    logger.error(f"Failed to LISTEN on channel {channel}: {e}")

    async def unsubscribe(self, channel: str) -> None:
        """Unsubscribe from notifications on a channel.

        Args:
            channel: The PostgreSQL channel name to stop listening on.
        """
        async with self._lock:
            if channel in self._callbacks:
                del self._callbacks[channel]

                # Remove listener if we're connected
                if self._connection is not None:
                    try:
                        await self._connection.remove_listener(
                            channel, self._create_notification_handler(channel)
                        )
                        logger.debug(f"Unsubscribed from channel: {channel}")
                    except Exception as e:
                        logger.error(f"Failed to UNLISTEN on channel {channel}: {e}")

    async def notify(self, channel: str, payload: str) -> None:
        """Send a notification on a channel.

        Args:
            channel: The PostgreSQL channel name.
            payload: The payload string (max ~7500 bytes recommended).

        Note:
            This uses the existing connection pool from SQLAlchemy,
            not the dedicated listener connection.
        """
        if self._connection is None:
            logger.warning("Cannot send NOTIFY: not connected")
            return

        try:
            # Use pg_notify function to properly escape the payload
            await self._connection.execute("SELECT pg_notify($1, $2)", channel, payload)
        except Exception as e:
            logger.warning(f"Failed to send NOTIFY on channel {channel}: {e}")

    @property
    def is_connected(self) -> bool:
        """Check if the listener is currently connected."""
        return self._connection is not None and not self._connection.is_closed()

    async def _establish_connection(self) -> None:
        """Establish connection to PostgreSQL."""
        try:
            import asyncpg
        except ImportError as e:
            raise ImportError(
                "asyncpg is required for PostgreSQL LISTEN/NOTIFY support. "
                "Install it with: pip install edda[postgres-notify]"
            ) from e

        try:
            self._connection = await asyncpg.connect(self._dsn)

            # Re-subscribe to all channels (this also registers listeners)
            await self._resubscribe_all()

            logger.info("PostgresNotifyListener connected to database")
        except Exception as e:
            logger.error(f"Failed to connect to PostgreSQL: {e}")
            self._connection = None
            raise

    async def _close_connection(self) -> None:
        """Close the database connection."""
        if self._connection is not None:
            try:
                await self._connection.close()
            except Exception as e:
                logger.warning(f"Error closing connection: {e}")
            finally:
                self._connection = None

    async def _resubscribe_all(self) -> None:
        """Re-subscribe to all registered channels after reconnection."""
        if self._connection is None:
            return

        for channel in self._callbacks:
            try:
                # Register listener for this channel
                await self._connection.add_listener(
                    channel, self._create_notification_handler(channel)
                )
                logger.debug(f"Re-subscribed to channel: {channel}")
            except Exception as e:
                logger.error(f"Failed to re-subscribe to channel {channel}: {e}")

    def _create_notification_handler(self, channel: str) -> Callable[..., None]:
        """Create or retrieve a notification handler for a channel.

        Args:
            channel: The channel name.

        Returns:
            A handler function that can be passed to add_listener/remove_listener.
        """
        if channel not in self._channel_handlers:

            def handler(_connection: Any, _pid: int, ch: str, payload: str) -> None:
                """Handle incoming notification from PostgreSQL."""
                callbacks = self._callbacks.get(ch, [])
                for callback in callbacks:
                    asyncio.create_task(self._safe_callback(callback, payload, ch))

            self._channel_handlers[channel] = handler

        return self._channel_handlers[channel]

    async def _safe_callback(self, callback: NotifyCallback, payload: str, channel: str) -> None:
        """Execute callback with error handling."""
        try:
            await callback(payload)
        except Exception as e:
            logger.error(
                f"Error in notification callback for channel {channel}: {e}",
                exc_info=True,
            )

    async def _reconnect_loop(self) -> None:
        """Monitor connection and reconnect on failure."""
        attempt = 0

        with suppress(asyncio.CancelledError):
            while self._running:
                await asyncio.sleep(1)  # Check every second

                if self._connection is None or self._connection.is_closed():
                    attempt += 1
                    if (
                        self._max_reconnect_attempts is not None
                        and attempt > self._max_reconnect_attempts
                    ):
                        logger.error(
                            f"Max reconnection attempts ({self._max_reconnect_attempts}) "
                            "exceeded, giving up"
                        )
                        break

                    logger.info(
                        f"Connection lost, attempting reconnection " f"(attempt {attempt})..."
                    )

                    try:
                        await self._close_connection()
                        await self._establish_connection()
                        attempt = 0  # Reset on success
                        logger.info("Reconnection successful")
                    except Exception as e:
                        logger.error(
                            f"Reconnection failed: {e}, " f"retrying in {self._reconnect_interval}s"
                        )
                        await asyncio.sleep(self._reconnect_interval)


def get_notify_channel_for_message(_channel: str) -> str:
    """Convert Edda channel name to PostgreSQL NOTIFY channel.

    Returns a unified channel name that both Python and Go frameworks use.

    Args:
        _channel: The Edda channel name (unused, kept for API compatibility).

    Returns:
        Unified PostgreSQL channel name.
    """
    return "workflow_channel_message"


def make_notify_payload(data: dict[str, Any]) -> str:
    """Create JSON payload for NOTIFY.

    Args:
        data: Dictionary to serialize as JSON.

    Returns:
        JSON string (kept under 7500 bytes for PostgreSQL safety).
    """
    payload = json.dumps(data, separators=(",", ":"))  # Compact JSON
    if len(payload) > 7500:
        logger.warning(
            f"NOTIFY payload exceeds recommended size " f"({len(payload)} > 7500 bytes), truncating"
        )
        # For safety, just include essential fields
        minimal_data = {k: v for k, v in data.items() if k in ("wf_id", "ts")}
        payload = json.dumps(minimal_data, separators=(",", ":"))
    return payload
