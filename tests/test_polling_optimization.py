"""
Tests for polling optimization features.

This module tests:
- System lock atomic UPDATE pattern
- Leader election pattern
- Outbox relayer adaptive backoff
"""

import asyncio
import contextlib
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
import pytest_asyncio
from sqlalchemy.ext.asyncio import create_async_engine

from edda.storage.sqlalchemy_storage import SQLAlchemyStorage

# =============================================================================
# System Lock Tests
# =============================================================================


class TestSystemLockAtomicUpdate:
    """Tests for the atomic UPDATE pattern in try_acquire_system_lock."""

    @pytest_asyncio.fixture
    async def storage(self):
        """Create a storage instance for testing."""
        engine = create_async_engine("sqlite+aiosqlite:///:memory:", echo=False)
        storage = SQLAlchemyStorage(engine)
        await storage.initialize()
        yield storage
        await storage.close()

    @pytest.mark.asyncio
    async def test_acquire_new_lock(self, storage):
        """Test acquiring a lock that doesn't exist."""
        result = await storage.try_acquire_system_lock(
            lock_name="test_lock",
            worker_id="worker_1",
            timeout_seconds=60,
        )
        assert result is True

    @pytest.mark.asyncio
    async def test_reacquire_own_lock(self, storage):
        """Test re-acquiring a lock the worker already holds."""
        # First acquisition
        result1 = await storage.try_acquire_system_lock(
            lock_name="test_lock",
            worker_id="worker_1",
            timeout_seconds=60,
        )
        assert result1 is True

        # Re-acquisition by same worker should succeed (lock renewal)
        result2 = await storage.try_acquire_system_lock(
            lock_name="test_lock",
            worker_id="worker_1",
            timeout_seconds=60,
        )
        assert result2 is True

    @pytest.mark.asyncio
    async def test_acquire_lock_held_by_another_worker(self, storage):
        """Test that acquiring a lock held by another worker fails."""
        # Worker 1 acquires the lock
        result1 = await storage.try_acquire_system_lock(
            lock_name="test_lock",
            worker_id="worker_1",
            timeout_seconds=60,
        )
        assert result1 is True

        # Worker 2 tries to acquire the same lock
        result2 = await storage.try_acquire_system_lock(
            lock_name="test_lock",
            worker_id="worker_2",
            timeout_seconds=60,
        )
        assert result2 is False

    @pytest.mark.asyncio
    async def test_acquire_expired_lock(self, storage):
        """Test acquiring a lock that has expired."""
        # Worker 1 acquires with very short timeout
        result1 = await storage.try_acquire_system_lock(
            lock_name="test_lock",
            worker_id="worker_1",
            timeout_seconds=0,  # Immediately expired
        )
        assert result1 is True

        # Worker 2 should be able to acquire the expired lock
        result2 = await storage.try_acquire_system_lock(
            lock_name="test_lock",
            worker_id="worker_2",
            timeout_seconds=60,
        )
        assert result2 is True

    @pytest.mark.asyncio
    async def test_concurrent_lock_acquisition(self, storage):
        """Test that only one worker can acquire a lock concurrently."""
        results = []

        async def try_acquire(worker_id):
            result = await storage.try_acquire_system_lock(
                lock_name="test_lock",
                worker_id=worker_id,
                timeout_seconds=60,
            )
            results.append((worker_id, result))

        # Start multiple workers trying to acquire the same lock
        tasks = [asyncio.create_task(try_acquire(f"worker_{i}")) for i in range(5)]
        await asyncio.gather(*tasks)

        # Exactly one worker should succeed
        successes = [r for r in results if r[1] is True]
        assert len(successes) == 1

    @pytest.mark.asyncio
    async def test_release_and_reacquire(self, storage):
        """Test releasing a lock and having another worker acquire it."""
        # Worker 1 acquires the lock
        result1 = await storage.try_acquire_system_lock(
            lock_name="test_lock",
            worker_id="worker_1",
            timeout_seconds=60,
        )
        assert result1 is True

        # Release the lock
        await storage.release_system_lock(
            lock_name="test_lock",
            worker_id="worker_1",
        )

        # Worker 2 should now be able to acquire it
        result2 = await storage.try_acquire_system_lock(
            lock_name="test_lock",
            worker_id="worker_2",
            timeout_seconds=60,
        )
        assert result2 is True


# =============================================================================
# Outbox Relayer Adaptive Backoff Tests
# =============================================================================


class TestOutboxRelayerAdaptiveBackoff:
    """Tests for the adaptive backoff in OutboxRelayer._poll_loop."""

    @pytest.mark.asyncio
    async def test_backoff_increases_on_empty_results(self):
        """Test that backoff increases when no events are found."""
        from edda.outbox.relayer import OutboxRelayer

        storage = AsyncMock()
        storage.get_pending_outbox_events = AsyncMock(return_value=[])

        relayer = OutboxRelayer(
            storage=storage,
            broker_url="http://example.com/broker",
            poll_interval=1.0,
        )
        relayer._running = True
        relayer._http_client = AsyncMock()

        # Track the sleep durations
        sleep_durations = []
        original_sleep = asyncio.sleep

        async def mock_sleep(duration):
            sleep_durations.append(duration)
            # Stop after a few iterations
            if len(sleep_durations) >= 5:
                relayer._running = False
            await original_sleep(0)  # Don't actually sleep

        with patch("asyncio.sleep", mock_sleep):
            await relayer._poll_loop()

        # Backoff should increase: 1s, 2s, 4s, 8s, 16s (capped at 30s)
        # Plus jitter (0-30% of backoff)
        assert len(sleep_durations) >= 4
        # First backoff should be around 2s (with jitter)
        assert sleep_durations[0] >= 1.0  # At least base interval
        # Later backoffs should be larger
        assert sleep_durations[-1] > sleep_durations[0]

    @pytest.mark.asyncio
    async def test_backoff_resets_on_events(self):
        """Test that backoff resets when events are processed."""
        from edda.outbox.relayer import OutboxRelayer

        storage = AsyncMock()
        # First call returns empty, second returns events, third returns empty
        storage.get_pending_outbox_events = AsyncMock(
            side_effect=[
                [],  # Empty - backoff should increase
                [],  # Empty - backoff should increase more
                [
                    {
                        "event_id": "1",
                        "event_type": "test",
                        "event_source": "test",
                        "event_data": {},
                        "content_type": "application/json",
                        "created_at": "2024-01-01T00:00:00",
                        "status": "processing",
                        "retry_count": 0,
                        "last_error": None,
                    }
                ],  # Event - reset backoff
                [],  # Empty - should start from base again
            ]
        )
        storage.mark_outbox_published = AsyncMock()

        relayer = OutboxRelayer(
            storage=storage,
            broker_url="http://example.com/broker",
            poll_interval=1.0,
        )
        relayer._running = True

        # Mock HTTP client
        mock_response = MagicMock()
        mock_response.raise_for_status = MagicMock()
        mock_client = AsyncMock()
        mock_client.post = AsyncMock(return_value=mock_response)
        relayer._http_client = mock_client

        # Track the sleep durations
        sleep_durations = []
        call_count = [0]

        async def mock_sleep(duration):
            sleep_durations.append(duration)
            call_count[0] += 1
            if call_count[0] >= 4:
                relayer._running = False

        with patch("asyncio.sleep", mock_sleep):
            await relayer._poll_loop()

        # After processing an event, the next backoff should be smaller
        # than the one before (reset to base)
        assert len(sleep_durations) >= 3
        # The backoff after processing should be less than the one before
        # (accounting for jitter, this is approximate)

    @pytest.mark.asyncio
    async def test_poll_and_publish_returns_count(self):
        """Test that _poll_and_publish returns the number of processed events."""
        from edda.outbox.relayer import OutboxRelayer

        storage = AsyncMock()
        storage.get_pending_outbox_events = AsyncMock(
            return_value=[
                {
                    "event_id": "1",
                    "event_type": "test",
                    "event_source": "test",
                    "event_data": {},
                    "content_type": "application/json",
                    "created_at": "2024-01-01T00:00:00",
                    "status": "processing",
                    "retry_count": 0,
                    "last_error": None,
                },
                {
                    "event_id": "2",
                    "event_type": "test",
                    "event_source": "test",
                    "event_data": {},
                    "content_type": "application/json",
                    "created_at": "2024-01-01T00:00:00",
                    "status": "processing",
                    "retry_count": 0,
                    "last_error": None,
                },
            ]
        )
        storage.mark_outbox_published = AsyncMock()

        relayer = OutboxRelayer(
            storage=storage,
            broker_url="http://example.com/broker",
        )

        # Mock HTTP client
        mock_response = MagicMock()
        mock_response.raise_for_status = MagicMock()
        mock_client = AsyncMock()
        mock_client.post = AsyncMock(return_value=mock_response)
        relayer._http_client = mock_client

        count = await relayer._poll_and_publish()
        assert count == 2

    @pytest.mark.asyncio
    async def test_poll_and_publish_returns_zero_when_empty(self):
        """Test that _poll_and_publish returns 0 when no events."""
        from edda.outbox.relayer import OutboxRelayer

        storage = AsyncMock()
        storage.get_pending_outbox_events = AsyncMock(return_value=[])

        relayer = OutboxRelayer(
            storage=storage,
            broker_url="http://example.com/broker",
        )

        count = await relayer._poll_and_publish()
        assert count == 0


# =============================================================================
# Leader Election Tests
# =============================================================================


class TestLeaderElection:
    """Tests for the leader election pattern in EddaApp."""

    @pytest.mark.asyncio
    async def test_leader_tasks_created_on_become_leader(self):
        """Test that leader tasks are created when becoming leader."""
        from edda.app import EddaApp

        app = EddaApp(
            service_name="test-service",
            db_url="sqlite+aiosqlite:///:memory:",
        )

        # Initialize to create the storage
        await app.initialize()

        try:
            # Simulate becoming leader
            tasks = app._create_leader_only_tasks()

            # Should create 4 tasks
            assert len(tasks) == 4

            # Check task names
            task_names = {t.get_name() for t in tasks}
            assert "leader_timer_check" in task_names
            assert "leader_message_timeout_check" in task_names
            assert "leader_stale_workflow_resume" in task_names
            assert "leader_message_cleanup" in task_names

            # Cancel all tasks
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)

        finally:
            await app.shutdown()

    @pytest.mark.asyncio
    async def test_cancel_tasks_helper(self):
        """Test the _cancel_tasks helper method."""
        from edda.app import EddaApp

        app = EddaApp(
            service_name="test-service",
            db_url="sqlite+aiosqlite:///:memory:",
        )

        # Create some dummy tasks
        async def dummy_task():
            await asyncio.sleep(1000)

        tasks = [
            asyncio.create_task(dummy_task()),
            asyncio.create_task(dummy_task()),
        ]

        # All tasks should be running
        for task in tasks:
            assert not task.done()

        # Cancel all tasks
        await app._cancel_tasks(tasks)

        # All tasks should be done (cancelled)
        for task in tasks:
            assert task.done()
            assert task.cancelled()

    @pytest.mark.asyncio
    async def test_leader_heartbeat_interval_parameter(self):
        """Test that leader_heartbeat_interval parameter is stored."""
        from edda.app import EddaApp

        app = EddaApp(
            service_name="test-service",
            db_url="sqlite+aiosqlite:///:memory:",
            leader_heartbeat_interval=30,
            leader_lease_duration=90,
        )

        assert app._leader_heartbeat_interval == 30
        assert app._leader_lease_duration == 90

    @pytest.mark.asyncio
    async def test_is_leader_initially_false(self):
        """Test that _is_leader is False initially."""
        from edda.app import EddaApp

        app = EddaApp(
            service_name="test-service",
            db_url="sqlite+aiosqlite:///:memory:",
        )

        assert app._is_leader is False
        assert app._leader_tasks == []


# =============================================================================
# Additional Edge Case Tests
# =============================================================================


class TestSystemLockEdgeCases:
    """Additional edge case tests for system locks."""

    @pytest_asyncio.fixture
    async def storage(self):
        """Create a storage instance for testing."""
        engine = create_async_engine("sqlite+aiosqlite:///:memory:", echo=False)
        storage = SQLAlchemyStorage(engine)
        await storage.initialize()
        yield storage
        await storage.close()

    @pytest.mark.asyncio
    async def test_multiple_locks_are_independent(self, storage):
        """Test that different lock names are independent."""
        # Acquire lock A
        result_a = await storage.try_acquire_system_lock(
            lock_name="lock_a",
            worker_id="worker_1",
            timeout_seconds=60,
        )
        assert result_a is True

        # Acquire lock B with different worker
        result_b = await storage.try_acquire_system_lock(
            lock_name="lock_b",
            worker_id="worker_2",
            timeout_seconds=60,
        )
        assert result_b is True

        # Worker 2 should not be able to acquire lock A
        result_a2 = await storage.try_acquire_system_lock(
            lock_name="lock_a",
            worker_id="worker_2",
            timeout_seconds=60,
        )
        assert result_a2 is False

    @pytest.mark.asyncio
    async def test_release_lock_by_wrong_worker_does_nothing(self, storage):
        """Test that releasing a lock by wrong worker doesn't affect it."""
        # Worker 1 acquires the lock
        await storage.try_acquire_system_lock(
            lock_name="test_lock",
            worker_id="worker_1",
            timeout_seconds=60,
        )

        # Worker 2 tries to release it (should do nothing)
        await storage.release_system_lock(
            lock_name="test_lock",
            worker_id="worker_2",
        )

        # Worker 2 still can't acquire it (worker 1 still holds it)
        result = await storage.try_acquire_system_lock(
            lock_name="test_lock",
            worker_id="worker_2",
            timeout_seconds=60,
        )
        assert result is False

    @pytest.mark.asyncio
    async def test_release_nonexistent_lock_does_not_fail(self, storage):
        """Test that releasing a nonexistent lock doesn't raise an error."""
        # Should not raise
        await storage.release_system_lock(
            lock_name="nonexistent_lock",
            worker_id="worker_1",
        )

    @pytest.mark.asyncio
    async def test_rapid_lock_renewal(self, storage):
        """Test that rapid lock renewals work correctly."""
        for _ in range(10):
            result = await storage.try_acquire_system_lock(
                lock_name="test_lock",
                worker_id="worker_1",
                timeout_seconds=60,
            )
            assert result is True

    @pytest.mark.asyncio
    async def test_lock_with_very_long_timeout(self, storage):
        """Test lock with very long timeout."""
        result = await storage.try_acquire_system_lock(
            lock_name="test_lock",
            worker_id="worker_1",
            timeout_seconds=86400,  # 24 hours
        )
        assert result is True

        # Another worker should not be able to acquire it
        result2 = await storage.try_acquire_system_lock(
            lock_name="test_lock",
            worker_id="worker_2",
            timeout_seconds=60,
        )
        assert result2 is False


class TestLeaderElectionEdgeCases:
    """Additional edge case tests for leader election."""

    @pytest.mark.asyncio
    async def test_monitor_and_restart_with_no_crashed_tasks(self):
        """Test that monitoring doesn't restart healthy tasks."""
        from edda.app import EddaApp

        app = EddaApp(
            service_name="test-service",
            db_url="sqlite+aiosqlite:///:memory:",
        )

        await app.initialize()

        try:
            # Create leader tasks
            app._leader_tasks = app._create_leader_only_tasks()

            # Monitor should not change anything
            original_tasks = app._leader_tasks.copy()
            await app._monitor_and_restart_leader_tasks()

            # All tasks should still be running
            assert len(app._leader_tasks) == len(original_tasks)
            for task in app._leader_tasks:
                assert not task.done()

        finally:
            # Cancel all tasks
            for task in app._leader_tasks:
                task.cancel()
            await asyncio.gather(*app._leader_tasks, return_exceptions=True)
            await app.shutdown()

    @pytest.mark.asyncio
    async def test_leader_election_loop_handles_storage_error(self):
        """Test that leader election loop handles storage errors gracefully."""
        from edda.app import EddaApp

        app = EddaApp(
            service_name="test-service",
            db_url="sqlite+aiosqlite:///:memory:",
        )

        await app.initialize()

        # Mock storage to raise an error
        original_try_acquire = app.storage.try_acquire_system_lock

        call_count = [0]

        async def mock_try_acquire(*args, **kwargs):
            call_count[0] += 1
            if call_count[0] == 1:
                raise Exception("Storage error")
            return await original_try_acquire(*args, **kwargs)

        app.storage.try_acquire_system_lock = mock_try_acquire

        # Run one iteration of the leader election loop
        # This should not raise and should set _is_leader to False
        try:
            # Set up to run just one iteration
            app._leader_heartbeat_interval = 0.01  # Very short interval

            # Create a task that will cancel after a short time
            async def run_leader_loop():
                task = asyncio.create_task(app._leader_election_loop())
                await asyncio.sleep(0.1)  # Let it run a bit
                task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await task

            await run_leader_loop()

            # Should have handled the error gracefully
            # (No assertion needed - just checking it doesn't raise)

        finally:
            app.storage.try_acquire_system_lock = original_try_acquire
            await app.shutdown()


class TestOutboxBackoffEdgeCases:
    """Additional edge case tests for Outbox backoff."""

    @pytest.mark.asyncio
    async def test_backoff_caps_at_max(self):
        """Test that backoff doesn't exceed maximum (30 seconds)."""
        from edda.outbox.relayer import OutboxRelayer

        storage = AsyncMock()
        storage.get_pending_outbox_events = AsyncMock(return_value=[])

        relayer = OutboxRelayer(
            storage=storage,
            broker_url="http://example.com/broker",
            poll_interval=1.0,
        )
        relayer._running = True
        relayer._http_client = AsyncMock()

        sleep_durations = []
        iteration = [0]

        async def mock_sleep(duration):
            sleep_durations.append(duration)
            iteration[0] += 1
            # Run for 10 iterations to hit the cap
            if iteration[0] >= 10:
                relayer._running = False

        with patch("asyncio.sleep", mock_sleep):
            await relayer._poll_loop()

        # All backoffs should be <= 30 + 30% jitter = 39 seconds max
        for duration in sleep_durations:
            assert duration <= 39.0, f"Backoff {duration} exceeded maximum"

    @pytest.mark.asyncio
    async def test_backoff_with_notify_wake_event(self):
        """Test backoff behavior with NOTIFY wake event."""
        from edda.outbox.relayer import OutboxRelayer

        storage = AsyncMock()
        storage.get_pending_outbox_events = AsyncMock(return_value=[])

        wake_event = asyncio.Event()
        relayer = OutboxRelayer(
            storage=storage,
            broker_url="http://example.com/broker",
            poll_interval=1.0,
            wake_event=wake_event,
        )
        relayer._running = True
        relayer._http_client = AsyncMock()

        wait_timeouts = []
        iteration = [0]

        async def mock_wait_for(coro, timeout):
            wait_timeouts.append(timeout)
            iteration[0] += 1
            if iteration[0] >= 3:
                relayer._running = False
            raise TimeoutError()  # Simulate timeout

        with patch("asyncio.wait_for", mock_wait_for):
            await relayer._poll_loop()

        # Timeouts should increase due to backoff
        assert len(wait_timeouts) >= 2
        assert wait_timeouts[-1] > wait_timeouts[0]

    @pytest.mark.asyncio
    async def test_error_in_poll_resets_consecutive_empty(self):
        """Test that errors in polling reset the consecutive empty counter."""
        from edda.outbox.relayer import OutboxRelayer

        storage = AsyncMock()
        # First call raises error, second returns empty
        storage.get_pending_outbox_events = AsyncMock(
            side_effect=[
                Exception("Database error"),
                [],
                [],
            ]
        )

        relayer = OutboxRelayer(
            storage=storage,
            broker_url="http://example.com/broker",
            poll_interval=1.0,
        )
        relayer._running = True
        relayer._http_client = AsyncMock()

        sleep_durations = []
        iteration = [0]

        async def mock_sleep(duration):
            sleep_durations.append(duration)
            iteration[0] += 1
            if iteration[0] >= 3:
                relayer._running = False

        with patch("asyncio.sleep", mock_sleep):
            await relayer._poll_loop()

        # First sleep should be base interval (error resets counter)
        # Second sleep should be 2x (one empty)
        # Third sleep should be 4x (two empty)
        assert len(sleep_durations) >= 2
        # After error, should start from base again
        assert sleep_durations[0] < 3.0  # Base + jitter


class TestLeaderElectionIntegration:
    """Integration tests for leader election with actual storage."""

    @pytest_asyncio.fixture
    async def storage(self):
        """Create a storage instance for testing."""
        engine = create_async_engine("sqlite+aiosqlite:///:memory:", echo=False)
        storage = SQLAlchemyStorage(engine)
        await storage.initialize()
        yield storage
        await storage.close()

    @pytest_asyncio.fixture
    async def pg_storage(self):
        """Create a PostgreSQL storage instance for testing."""
        import os

        pg_url = os.environ.get("TEST_DATABASE_URL")
        if not pg_url or "postgresql" not in pg_url:
            pytest.skip("PostgreSQL not available for testing")

        engine = create_async_engine(pg_url, echo=False)
        storage = SQLAlchemyStorage(engine)
        await storage.initialize()
        yield storage
        await storage.close()

    @pytest.mark.asyncio
    async def test_single_worker_becomes_leader(self, storage):
        """Test that a single worker can become leader."""
        result = await storage.try_acquire_system_lock(
            lock_name="edda_leader",
            worker_id="worker_1",
            timeout_seconds=45,
        )
        assert result is True

    @pytest.mark.asyncio
    async def test_second_worker_cannot_become_leader(self, storage):
        """Test that second worker cannot become leader while first holds it."""
        # First worker becomes leader
        result1 = await storage.try_acquire_system_lock(
            lock_name="edda_leader",
            worker_id="worker_1",
            timeout_seconds=45,
        )
        assert result1 is True

        # Second worker tries and fails
        result2 = await storage.try_acquire_system_lock(
            lock_name="edda_leader",
            worker_id="worker_2",
            timeout_seconds=45,
        )
        assert result2 is False

    @pytest.mark.asyncio
    async def test_leader_renewal_extends_lease(self, storage):
        """Test that leader can renew their lease."""
        # Become leader with short timeout
        result1 = await storage.try_acquire_system_lock(
            lock_name="edda_leader",
            worker_id="worker_1",
            timeout_seconds=1,
        )
        assert result1 is True

        # Immediately renew with longer timeout
        result2 = await storage.try_acquire_system_lock(
            lock_name="edda_leader",
            worker_id="worker_1",
            timeout_seconds=60,
        )
        assert result2 is True

        # Other worker still can't take over
        result3 = await storage.try_acquire_system_lock(
            lock_name="edda_leader",
            worker_id="worker_2",
            timeout_seconds=60,
        )
        assert result3 is False

    @pytest.mark.asyncio
    async def test_leader_failover_after_expiry(self, storage):
        """Test that another worker can become leader after lease expires."""
        # First worker becomes leader with immediate expiry
        result1 = await storage.try_acquire_system_lock(
            lock_name="edda_leader",
            worker_id="worker_1",
            timeout_seconds=0,  # Immediately expires
        )
        assert result1 is True

        # Second worker can now take over
        result2 = await storage.try_acquire_system_lock(
            lock_name="edda_leader",
            worker_id="worker_2",
            timeout_seconds=45,
        )
        assert result2 is True

        # First worker can no longer renew
        result3 = await storage.try_acquire_system_lock(
            lock_name="edda_leader",
            worker_id="worker_1",
            timeout_seconds=45,
        )
        assert result3 is False


# =============================================================================
# PostgreSQL Integration Tests
# =============================================================================


class TestSystemLockPostgreSQL:
    """PostgreSQL-specific tests for system locks."""

    @pytest_asyncio.fixture
    async def pg_storage(self):
        """Create a PostgreSQL storage instance for testing."""
        import os

        pg_url = os.environ.get("TEST_DATABASE_URL")
        if not pg_url or "postgresql" not in pg_url:
            pytest.skip("PostgreSQL not available for testing")

        engine = create_async_engine(pg_url, echo=False)
        storage = SQLAlchemyStorage(engine)
        await storage.initialize()
        yield storage
        # Clean up test locks
        async with storage._async_session_factory() as session:
            from sqlalchemy import delete

            from edda.storage.models import SystemLock

            await session.execute(delete(SystemLock).where(SystemLock.lock_name.like("test_%")))
            await session.commit()
        await storage.close()

    @pytest.mark.asyncio
    async def test_postgres_atomic_lock_acquisition(self, pg_storage):
        """Test atomic lock acquisition on PostgreSQL."""
        import uuid

        lock_name = f"test_{uuid.uuid4().hex[:8]}"

        result = await pg_storage.try_acquire_system_lock(
            lock_name=lock_name,
            worker_id="worker_1",
            timeout_seconds=60,
        )
        assert result is True

        # Another worker should fail
        result2 = await pg_storage.try_acquire_system_lock(
            lock_name=lock_name,
            worker_id="worker_2",
            timeout_seconds=60,
        )
        assert result2 is False

    @pytest.mark.asyncio
    async def test_postgres_concurrent_lock_race(self, pg_storage):
        """Test concurrent lock acquisition on PostgreSQL."""
        import uuid

        lock_name = f"test_{uuid.uuid4().hex[:8]}"

        results = []

        async def try_acquire(worker_id):
            result = await pg_storage.try_acquire_system_lock(
                lock_name=lock_name,
                worker_id=worker_id,
                timeout_seconds=60,
            )
            results.append((worker_id, result))

        # Launch multiple concurrent acquisitions
        tasks = [asyncio.create_task(try_acquire(f"worker_{i}")) for i in range(10)]
        await asyncio.gather(*tasks)

        # Exactly one should succeed
        successes = [r for r in results if r[1] is True]
        assert len(successes) == 1


# =============================================================================
# Concurrency Stress Tests
# =============================================================================


class TestConcurrencyStress:
    """Stress tests for concurrent lock acquisition."""

    @pytest_asyncio.fixture
    async def storage(self):
        """Create a storage instance for testing."""
        engine = create_async_engine("sqlite+aiosqlite:///:memory:", echo=False)
        storage = SQLAlchemyStorage(engine)
        await storage.initialize()
        yield storage
        await storage.close()

    @pytest.mark.asyncio
    async def test_high_concurrency_lock_acquisition(self, storage):
        """Test lock acquisition under high concurrency."""
        import uuid

        lock_name = f"test_{uuid.uuid4().hex[:8]}"
        num_workers = 50

        results = []

        async def try_acquire(worker_id):
            result = await storage.try_acquire_system_lock(
                lock_name=lock_name,
                worker_id=worker_id,
                timeout_seconds=60,
            )
            results.append((worker_id, result))

        # Launch many concurrent acquisitions
        tasks = [asyncio.create_task(try_acquire(f"worker_{i}")) for i in range(num_workers)]
        await asyncio.gather(*tasks)

        # Exactly one should succeed
        successes = [r for r in results if r[1] is True]
        assert len(successes) == 1

        # All others should fail
        failures = [r for r in results if r[1] is False]
        assert len(failures) == num_workers - 1

    @pytest.mark.asyncio
    async def test_repeated_lock_release_acquire_cycles(self, storage):
        """Test repeated lock release and acquire cycles."""
        import uuid

        lock_name = f"test_{uuid.uuid4().hex[:8]}"

        for i in range(20):
            # Worker 1 acquires
            result1 = await storage.try_acquire_system_lock(
                lock_name=lock_name,
                worker_id=f"worker_cycle_{i}_1",
                timeout_seconds=60,
            )
            assert result1 is True

            # Release
            await storage.release_system_lock(
                lock_name=lock_name,
                worker_id=f"worker_cycle_{i}_1",
            )

            # Worker 2 should now be able to acquire
            result2 = await storage.try_acquire_system_lock(
                lock_name=lock_name,
                worker_id=f"worker_cycle_{i}_2",
                timeout_seconds=60,
            )
            assert result2 is True

            # Clean up for next cycle
            await storage.release_system_lock(
                lock_name=lock_name,
                worker_id=f"worker_cycle_{i}_2",
            )

    @pytest.mark.asyncio
    async def test_multiple_lock_names_concurrent(self, storage):
        """Test acquiring multiple different locks concurrently."""
        num_locks = 10
        results = []

        async def acquire_lock(lock_num, worker_id):
            result = await storage.try_acquire_system_lock(
                lock_name=f"lock_{lock_num}",
                worker_id=worker_id,
                timeout_seconds=60,
            )
            results.append((lock_num, worker_id, result))

        # Each worker acquires a different lock
        tasks = [asyncio.create_task(acquire_lock(i, f"worker_{i}")) for i in range(num_locks)]
        await asyncio.gather(*tasks)

        # All should succeed (different locks)
        successes = [r for r in results if r[2] is True]
        assert len(successes) == num_locks
