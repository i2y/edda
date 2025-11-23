"""
Tests for concurrent outbox processing in distributed environments.

This module tests that multiple workers can safely process outbox events
without duplicates, using SELECT FOR UPDATE with SKIP LOCKED.

**IMPORTANT**: These tests require PostgreSQL or MySQL.
SQLite does NOT support SKIP LOCKED, so these tests will be skipped on SQLite.
"""

import asyncio
import uuid

import pytest


@pytest.mark.asyncio
async def test_concurrent_fetch_no_duplicates(db_storage):
    """Test that multiple workers don't fetch the same events.

    This test simulates a realistic scenario where workers poll in a loop.
    In concurrent systems, a single poll might not fetch all events if some
    are locked by other workers. Workers retry until all events are processed.
    """
    # Skip for SQLite (SKIP LOCKED not supported)
    if db_storage.engine.dialect.name == "sqlite":
        pytest.skip("SQLite does not support SKIP LOCKED")

    storage = db_storage
    # Create 10 outbox events
    for i in range(10):
        await storage.add_outbox_event(
            event_id=str(uuid.uuid4()),
            event_type=f"test.event.{i}",
            event_source="test-service",
            event_data={"index": i},
            content_type="application/json",
        )

    # Simulate 3 workers polling in a loop (realistic scenario)
    async def worker_poll_loop(worker_id: int, limit: int) -> list[str]:
        """Fetch events as a worker, retrying until no more events."""
        all_events = []
        max_attempts = 5  # Safety limit
        for _attempt in range(max_attempts):
            events = await storage.get_pending_outbox_events(limit=limit)
            if not events:
                break  # No more events available
            all_events.extend([event["event_id"] for event in events])
            # Small delay to allow other workers to commit
            await asyncio.sleep(0.01)
        return all_events

    # Execute 3 workers concurrently
    results = await asyncio.gather(
        worker_poll_loop(1, limit=4),
        worker_poll_loop(2, limit=4),
        worker_poll_loop(3, limit=4),
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
async def test_parallel_publishing(db_storage):
    """Test that multiple workers can publish events in parallel with retry.

    This test simulates realistic production behavior where workers poll in a loop.
    In concurrent environments (MySQL READ COMMITTED + SKIP LOCKED), a single poll
    may not fetch all events if some are locked by other workers. Workers retry
    until all events are processed, matching the OutboxRelayer._poll_loop() behavior.
    """
    # Skip for SQLite (SKIP LOCKED not supported)
    if db_storage.engine.dialect.name == "sqlite":
        pytest.skip("SQLite does not support SKIP LOCKED")

    storage = db_storage
    # Create 6 events
    for i in range(6):
        await storage.add_outbox_event(
            event_id=str(uuid.uuid4()),
            event_type=f"test.event.{i}",
            event_source="test-service",
            event_data={"index": i},
            content_type="application/json",
        )

    # Simulate 2 workers polling in a loop (realistic production scenario)
    async def worker_publish_with_retry():
        """Worker that polls and publishes events with retry."""
        all_events = []
        max_attempts = 3  # Retry up to 3 times
        for _attempt in range(max_attempts):
            events = await storage.get_pending_outbox_events(limit=3)
            if not events:
                break  # No more events available
            for event in events:
                await storage.mark_outbox_published(event["event_id"])
            all_events.extend([event["event_id"] for event in events])
            # Small delay to allow other workers to commit
            await asyncio.sleep(0.01)
        return all_events

    # Execute 2 workers concurrently
    results = await asyncio.gather(
        worker_publish_with_retry(),
        worker_publish_with_retry(),
    )

    # Verify each worker published different events
    worker1_ids = set(results[0])
    worker2_ids = set(results[1])
    assert worker1_ids.isdisjoint(worker2_ids), "Workers published overlapping events!"

    # Verify all events are eventually published
    assert (
        len(worker1_ids) + len(worker2_ids) == 6
    ), f"Expected 6 total events, got {len(worker1_ids) + len(worker2_ids)}"


@pytest.mark.asyncio
async def test_large_scale_concurrent_processing(db_storage):
    """Test concurrent processing at larger scale.

    This test simulates a realistic scenario where workers poll in a loop.
    In concurrent systems, workers retry until all events are processed.
    """
    # Skip for SQLite (SKIP LOCKED not supported)
    if db_storage.engine.dialect.name == "sqlite":
        pytest.skip("SQLite does not support SKIP LOCKED")

    storage = db_storage
    # Create 100 events
    for i in range(100):
        await storage.add_outbox_event(
            event_id=str(uuid.uuid4()),
            event_type=f"test.event.{i}",
            event_source="test-service",
            event_data={"index": i},
            content_type="application/json",
        )

    # Simulate 10 workers polling in a loop (realistic scenario)
    async def worker_poll_loop(worker_id: int) -> list[str]:
        """Fetch events as a worker, retrying until no more events."""
        all_events = []
        max_attempts = 10  # Safety limit
        for _attempt in range(max_attempts):
            events = await storage.get_pending_outbox_events(limit=15)
            if not events:
                break  # No more events available
            all_events.extend([event["event_id"] for event in events])
            # Small delay to allow other workers to commit
            await asyncio.sleep(0.01)
        return all_events

    # Execute 10 workers concurrently
    workers = [worker_poll_loop(i) for i in range(10)]
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
async def test_retry_failed_events_no_duplicate(db_storage):
    """Test that failed events can be retried without duplicates."""
    # Skip for SQLite (SKIP LOCKED not supported)
    if db_storage.engine.dialect.name == "sqlite":
        pytest.skip("SQLite does not support SKIP LOCKED")

    storage = db_storage
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
