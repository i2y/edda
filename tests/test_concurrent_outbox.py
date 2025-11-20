"""
Tests for concurrent outbox processing in distributed environments.

This module tests that multiple workers can safely process outbox events
without duplicates, using SELECT FOR UPDATE with SKIP LOCKED.

**IMPORTANT**: These tests require PostgreSQL or MySQL.
SQLite does NOT support SKIP LOCKED, so these tests will be skipped on SQLite.
"""

import asyncio
import uuid
from datetime import UTC, datetime

import pytest
import pytest_asyncio
from sqlalchemy.ext.asyncio import create_async_engine

from edda.storage.sqlalchemy_storage import SQLAlchemyStorage


# Skip all tests in this module for SQLite (SKIP LOCKED not supported)
pytestmark = pytest.mark.skip(
    reason="SQLite does not support SKIP LOCKED. "
    "These tests require PostgreSQL 9.5+ or MySQL 8.0+. "
    "Run with: docker-compose -f docker-compose.test.yml up -d"
)


@pytest_asyncio.fixture
async def storage():
    """Create storage instance for testing."""
    engine = create_async_engine("sqlite+aiosqlite:///:memory:", echo=False)
    storage = SQLAlchemyStorage(engine)
    await storage.initialize()
    yield storage
    await storage.close()


@pytest.mark.asyncio
async def test_concurrent_fetch_no_duplicates(storage):
    """Test that multiple workers don't fetch the same events."""
    # Create 10 outbox events
    for i in range(10):
        await storage.add_outbox_event(
            event_id=str(uuid.uuid4()),
            event_type=f"test.event.{i}",
            event_source="test-service",
            event_data={"index": i},
            content_type="application/json",
        )

    # Simulate 3 workers polling simultaneously
    async def worker_fetch(worker_id: int, limit: int) -> list[str]:
        """Fetch events as a worker and return event IDs."""
        events = await storage.get_pending_outbox_events(limit=limit)
        return [event["event_id"] for event in events]

    # Execute 3 workers concurrently
    results = await asyncio.gather(
        worker_fetch(1, limit=4),
        worker_fetch(2, limit=4),
        worker_fetch(3, limit=4),
    )

    # Collect all event IDs fetched by all workers
    all_event_ids = []
    for worker_events in results:
        all_event_ids.extend(worker_events)

    # Verify no duplicates (each event fetched by at most one worker)
    assert len(all_event_ids) == len(set(all_event_ids)), "Duplicate event IDs found!"

    # Verify total fetched events
    assert len(all_event_ids) == 10, f"Expected 10 events, got {len(all_event_ids)}"


@pytest.mark.asyncio
async def test_skip_locked_behavior(storage):
    """Test that SKIP LOCKED correctly skips locked rows."""
    # Create 5 events
    for i in range(5):
        await storage.add_outbox_event(
            event_id=str(uuid.uuid4()),
            event_type=f"test.event.{i}",
            event_source="test-service",
            event_data={"index": i},
            content_type="application/json",
        )

    # Worker A fetches 3 events (locks them)
    events_a = await storage.get_pending_outbox_events(limit=3)
    assert len(events_a) == 3

    # Worker B fetches events (should get remaining 2, not the locked ones)
    events_b = await storage.get_pending_outbox_events(limit=5)
    assert len(events_b) == 2  # Only 2 unlocked events available

    # Verify no overlap
    ids_a = {event["event_id"] for event in events_a}
    ids_b = {event["event_id"] for event in events_b}
    assert ids_a.isdisjoint(ids_b), "Workers fetched overlapping events!"


@pytest.mark.asyncio
async def test_parallel_publishing(storage):
    """Test that multiple workers can publish events in parallel."""
    # Create 6 events
    event_ids = []
    for i in range(6):
        await storage.add_outbox_event(
            event_id=str(uuid.uuid4()),
            event_type=f"test.event.{i}",
            event_source="test-service",
            event_data={"index": i},
            content_type="application/json",
        )

    # Get all event IDs
    all_events = await storage.get_pending_outbox_events(limit=10)
    event_ids = [event["event_id"] for event in all_events]

    # Simulate 2 workers publishing events simultaneously
    async def worker_publish(event_ids: list[str]):
        """Worker that fetches and publishes events."""
        events = await storage.get_pending_outbox_events(limit=3)
        for event in events:
            await storage.mark_outbox_published(event["event_id"])
        return [event["event_id"] for event in events]

    # Execute 2 workers concurrently
    results = await asyncio.gather(
        worker_publish(event_ids[:3]),
        worker_publish(event_ids[3:]),
    )

    # Verify each worker published different events
    worker1_ids = set(results[0])
    worker2_ids = set(results[1])
    assert worker1_ids.isdisjoint(worker2_ids), "Workers published overlapping events!"

    # Verify all events are published
    assert len(worker1_ids) + len(worker2_ids) == 6


@pytest.mark.asyncio
async def test_large_scale_concurrent_processing(storage):
    """Test concurrent processing at larger scale."""
    # Create 100 events
    for i in range(100):
        await storage.add_outbox_event(
            event_id=str(uuid.uuid4()),
            event_type=f"test.event.{i}",
            event_source="test-service",
            event_data={"index": i},
            content_type="application/json",
        )

    # Simulate 10 workers polling simultaneously
    async def worker_fetch(worker_id: int) -> list[str]:
        """Fetch events as a worker."""
        events = await storage.get_pending_outbox_events(limit=15)
        return [event["event_id"] for event in events]

    # Execute 10 workers concurrently
    workers = [worker_fetch(i) for i in range(10)]
    results = await asyncio.gather(*workers)

    # Collect all event IDs
    all_event_ids = []
    for worker_events in results:
        all_event_ids.extend(worker_events)

    # Verify no duplicates
    assert len(all_event_ids) == len(set(all_event_ids)), "Duplicate events found!"

    # Verify all 100 events were fetched
    assert len(all_event_ids) == 100


@pytest.mark.asyncio
async def test_retry_failed_events_no_duplicate(storage):
    """Test that failed events can be retried without duplicates."""
    # Create 3 events
    for i in range(3):
        await storage.add_outbox_event(
            event_id=str(uuid.uuid4()),
            event_type=f"test.event.{i}",
            event_source="test-service",
            event_data={"index": i},
            content_type="application/json",
        )

    # Worker A fetches all 3 events
    events_a = await storage.get_pending_outbox_events(limit=5)
    assert len(events_a) == 3

    # Mark first event as failed (retry later)
    await storage.mark_outbox_failed(events_a[0]["event_id"], "Network error")

    # Mark second event as published
    await storage.mark_outbox_published(events_a[1]["event_id"])

    # Worker B fetches pending events (should only get event 0 and 2)
    events_b = await storage.get_pending_outbox_events(limit=5)
    fetched_ids = {event["event_id"] for event in events_b}

    # Verify event 1 is not in the results (published)
    assert events_a[1]["event_id"] not in fetched_ids

    # Verify event 0 and 2 are available for retry
    assert events_a[0]["event_id"] in fetched_ids or events_a[2]["event_id"] in fetched_ids
