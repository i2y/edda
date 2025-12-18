"""
Outbox Relayer - Background publisher for transactional outbox pattern.

This module implements a background task that polls the database for pending
outbox events and publishes them to a Message Broker as CloudEvents.
"""

import asyncio
import contextlib
import logging
import random
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Any

import httpx
from cloudevents.conversion import to_structured
from cloudevents.http import CloudEvent

if TYPE_CHECKING:
    from edda.storage.protocol import StorageProtocol

logger = logging.getLogger(__name__)


class OutboxRelayer:
    """
    Background relayer for publishing outbox events.

    The relayer polls the database for pending events and publishes them
    to a Message Broker. It implements exponential backoff for retries
    and graceful shutdown.

    Example:
        >>> storage = SQLiteStorage("saga.db")
        >>> relayer = OutboxRelayer(
        ...     storage=storage,
        ...     broker_url="http://broker-ingress.svc.cluster.local/default/default",
        ...     poll_interval=1.0,
        ...     max_retries=3
        ... )
        >>> await relayer.start()
    """

    def __init__(
        self,
        storage: "StorageProtocol",
        broker_url: str,
        poll_interval: float = 1.0,
        max_retries: int = 3,
        batch_size: int = 10,
        max_age_hours: float | None = None,
        wake_event: asyncio.Event | None = None,
    ):
        """
        Initialize the Outbox Relayer.

        Args:
            storage: Storage backend for outbox events
            broker_url: Message Broker URL for publishing events
            poll_interval: Polling interval in seconds (default: 1.0)
            max_retries: Maximum retry attempts (default: 3)
            batch_size: Number of events to process per batch (default: 10)
            max_age_hours: Maximum event age in hours before expiration (default: None, disabled)
                          Events older than this are marked as 'expired' and won't be retried.
            wake_event: Optional asyncio.Event to wake the relayer immediately when new
                       events are added. Used with PostgreSQL LISTEN/NOTIFY integration.
        """
        self.storage = storage
        self.broker_url = broker_url
        self.poll_interval = poll_interval
        self.max_retries = max_retries
        self.batch_size = batch_size
        self.max_age_hours = max_age_hours
        self._wake_event = wake_event

        self._task: asyncio.Task[Any] | None = None
        self._running = False
        self._http_client: httpx.AsyncClient | None = None

    async def start(self) -> None:
        """
        Start the background relayer task.

        This creates an HTTP client and starts the polling loop in a background task.
        """
        if self._running:
            logger.warning("Outbox relayer is already running")
            return

        self._running = True
        self._http_client = httpx.AsyncClient(timeout=30.0)

        # Start background task
        self._task = asyncio.create_task(self._poll_loop())
        logger.info(
            f"Outbox relayer started (broker={self.broker_url}, "
            f"poll_interval={self.poll_interval}s)"
        )

    async def stop(self) -> None:
        """
        Stop the background relayer task gracefully.

        This cancels the polling loop and closes the HTTP client.
        """
        if not self._running:
            return

        self._running = False

        # Cancel background task
        if self._task:
            self._task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._task

        # Close HTTP client
        if self._http_client:
            await self._http_client.aclose()

        logger.info("Outbox relayer stopped")

    async def _poll_loop(self) -> None:
        """
        Main polling loop with adaptive backoff.

        Continuously polls the database for pending events and publishes them.
        When wake_event is provided (PostgreSQL NOTIFY integration), wakes up
        immediately on notification, otherwise uses poll_interval as fallback.

        Adaptive backoff behavior:
        - When no events are found, exponentially backs off up to 30 seconds
        - When events are processed, resets to base poll_interval
        - When woken by NOTIFY, resets backoff
        """
        consecutive_empty = 0

        while self._running:
            try:
                count = await self._poll_and_publish()
                if count == 0:
                    consecutive_empty += 1
                else:
                    consecutive_empty = 0
            except Exception as e:
                logger.error(f"Error in outbox relayer poll loop: {e}")
                consecutive_empty = 0  # Reset on error

            # Adaptive backoff calculation
            if consecutive_empty > 0:
                # Exponential backoff: 2s, 4s, 8s, 16s, max 30s (with poll_interval=1)
                backoff = min(self.poll_interval * (2 ** min(consecutive_empty, 4)), 30.0)
            else:
                backoff = self.poll_interval
            jitter = random.uniform(0, backoff * 0.3)

            # Wait before next poll (with optional NOTIFY wake)
            if self._wake_event is not None:
                try:
                    await asyncio.wait_for(
                        self._wake_event.wait(),
                        timeout=backoff + jitter,
                    )
                    # Clear the event for next notification
                    self._wake_event.clear()
                    consecutive_empty = 0  # Reset on NOTIFY wake
                    logger.debug("Outbox relayer woken by NOTIFY")
                except TimeoutError:
                    # Fallback polling timeout reached
                    pass
            else:
                await asyncio.sleep(backoff + jitter)

    async def _poll_and_publish(self) -> int:
        """
        Poll for pending events and publish them.

        Fetches a batch of pending events from the database and attempts
        to publish each one to the Message Broker.

        Returns:
            Number of events processed
        """
        # Get pending events
        events = await self.storage.get_pending_outbox_events(limit=self.batch_size)

        if not events:
            return 0

        logger.debug(f"Processing {len(events)} pending outbox events")

        # Publish each event
        for event in events:
            try:
                await self._publish_event(event)
            except Exception as e:
                logger.error(
                    f"Failed to publish event {event['event_id']}: {e}",
                    exc_info=True,
                )

        return len(events)

    async def _publish_event(self, event: dict[str, Any]) -> None:
        """
        Publish a single outbox event to the Message Broker.

        Args:
            event: Outbox event record from database

        Raises:
            Exception: If publishing fails after max retries
        """
        event_id = event["event_id"]
        retry_count = event["retry_count"]

        # Check if max age exceeded (optional feature)
        if self.max_age_hours is not None:
            created_at = event["created_at"]
            # Convert to datetime if it's a string (from database)
            if isinstance(created_at, str):
                created_at = datetime.fromisoformat(created_at.replace("Z", "+00:00"))
            elif not hasattr(created_at, "tzinfo") or created_at.tzinfo is None:
                # Assume UTC if no timezone info
                created_at = created_at.replace(tzinfo=UTC)

            event_age = datetime.now(UTC) - created_at
            max_age = timedelta(hours=self.max_age_hours)

            if event_age > max_age:
                logger.warning(
                    f"Event {event_id} exceeded max age "
                    f"({self.max_age_hours} hours, age={event_age}), "
                    "marking as expired"
                )
                await self.storage.mark_outbox_expired(
                    event_id,
                    f"Exceeded max age ({self.max_age_hours} hours, age={event_age})",
                )
                return

        # Check if max retries exceeded
        if retry_count >= self.max_retries:
            logger.warning(
                f"Event {event_id} exceeded max retries ({self.max_retries}), "
                "marking as permanently failed"
            )
            await self.storage.mark_outbox_permanently_failed(
                event_id, f"Exceeded max retries ({self.max_retries})"
            )
            return

        try:
            # Build CloudEvent
            ce = CloudEvent(
                {
                    "type": event["event_type"],
                    "source": event["event_source"],
                    "id": event_id,
                },
                event["event_data"],
            )

            # Convert to structured format
            headers, body = to_structured(ce)

            # Publish to Message Broker
            if self._http_client is None:
                raise RuntimeError("HTTP client not initialized")

            response = await self._http_client.post(
                self.broker_url,
                headers=headers,
                content=body,
            )

            # Check response
            response.raise_for_status()

            # Mark as published
            await self.storage.mark_outbox_published(event_id)
            logger.info(f"Successfully published event {event_id}")

        except httpx.HTTPStatusError as e:
            # HTTP error with status code - distinguish 4xx (client) vs 5xx (server)
            status_code = e.response.status_code
            error_msg = f"HTTP {status_code}: {str(e)}"

            if 400 <= status_code < 500:
                # Client error (4xx) - permanent failure, don't retry
                logger.error(
                    f"Permanent error for event {event_id}: {error_msg}. "
                    "Marking as invalid (won't retry)"
                )
                await self.storage.mark_outbox_invalid(event_id, error_msg)
            else:
                # Server error (5xx) - temporary failure, retry
                logger.warning(
                    f"Server error for event {event_id} "
                    f"(retry {retry_count + 1}/{self.max_retries}): {error_msg}"
                )
                await self.storage.mark_outbox_failed(event_id, error_msg)

        except httpx.RequestError as e:
            # Network error (connection timeout, DNS failure, etc.) - retry
            error_msg = f"Network error: {str(e)}"
            logger.warning(
                f"Network error for event {event_id} "
                f"(retry {retry_count + 1}/{self.max_retries}): {error_msg}"
            )
            await self.storage.mark_outbox_failed(event_id, error_msg)

        except Exception as e:
            # Unknown error - retry (safety net)
            error_msg = f"{type(e).__name__}: {str(e)}"
            logger.warning(
                f"Unknown error for event {event_id} "
                f"(retry {retry_count + 1}/{self.max_retries}): {error_msg}"
            )
            await self.storage.mark_outbox_failed(event_id, error_msg)
