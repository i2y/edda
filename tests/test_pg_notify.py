"""
Tests for PostgreSQL LISTEN/NOTIFY integration.

This module tests:
- NoopNotifyListener (for SQLite/MySQL)
- PostgresNotifyListener (for PostgreSQL)
- create_notify_listener factory function
"""

import asyncio

import pytest

from edda.storage.notify_base import (
    NoopNotifyListener,
    NotifyProtocol,
    create_notify_listener,
)


class TestNoopNotifyListener:
    """Tests for NoopNotifyListener (SQLite/MySQL fallback)."""

    async def test_implements_protocol(self):
        """NoopNotifyListener should implement NotifyProtocol."""
        listener = NoopNotifyListener()
        assert isinstance(listener, NotifyProtocol)

    async def test_start_stop(self):
        """Test start and stop lifecycle."""
        listener = NoopNotifyListener()

        assert not listener.is_connected

        await listener.start()
        assert listener.is_connected

        await listener.stop()
        assert not listener.is_connected

    async def test_subscribe_noop(self):
        """Subscribe should be a no-op (callback never called)."""
        listener = NoopNotifyListener()
        await listener.start()

        callback_called = False

        async def callback(payload: str) -> None:
            nonlocal callback_called
            callback_called = True

        await listener.subscribe("test_channel", callback)

        # Notify should also be a no-op
        await listener.notify("test_channel", '{"test": true}')

        # Give event loop a chance to run
        await asyncio.sleep(0.01)

        # Callback should never be called in noop mode
        assert not callback_called

        await listener.stop()

    async def test_unsubscribe_noop(self):
        """Unsubscribe should be a no-op."""
        listener = NoopNotifyListener()
        await listener.start()

        async def callback(payload: str) -> None:
            pass

        await listener.subscribe("test_channel", callback)
        await listener.unsubscribe("test_channel")

        await listener.stop()


class TestCreateNotifyListener:
    """Tests for create_notify_listener factory function."""

    def test_sqlite_returns_noop(self):
        """SQLite URL should return NoopNotifyListener."""
        listener = create_notify_listener("sqlite+aiosqlite:///test.db")
        assert isinstance(listener, NoopNotifyListener)

    def test_sqlite_memory_returns_noop(self):
        """SQLite in-memory URL should return NoopNotifyListener."""
        listener = create_notify_listener("sqlite+aiosqlite:///:memory:")
        assert isinstance(listener, NoopNotifyListener)

    def test_mysql_returns_noop(self):
        """MySQL URL should return NoopNotifyListener."""
        listener = create_notify_listener("mysql+aiomysql://user:pass@localhost/db")
        assert isinstance(listener, NoopNotifyListener)

    def test_postgresql_returns_postgres_listener(self):
        """PostgreSQL URL should return PostgresNotifyListener."""
        # This test requires asyncpg to be installed
        try:
            import asyncpg  # noqa: F401
        except ImportError:
            pytest.skip("asyncpg not installed")

        from edda.storage.pg_notify import PostgresNotifyListener

        listener = create_notify_listener("postgresql://user:pass@localhost/db")
        assert isinstance(listener, PostgresNotifyListener)

    def test_postgresql_with_asyncpg_driver(self):
        """PostgreSQL URL with asyncpg driver should return PostgresNotifyListener."""
        try:
            import asyncpg  # noqa: F401
        except ImportError:
            pytest.skip("asyncpg not installed")

        from edda.storage.pg_notify import PostgresNotifyListener

        listener = create_notify_listener("postgresql+asyncpg://user:pass@localhost/db")
        assert isinstance(listener, PostgresNotifyListener)


class TestPostgresNotifyListenerUnit:
    """Unit tests for PostgresNotifyListener (without actual PostgreSQL connection)."""

    @pytest.fixture
    def listener(self):
        """Create a PostgresNotifyListener instance (not connected)."""
        try:
            import asyncpg  # noqa: F401
        except ImportError:
            pytest.skip("asyncpg not installed")

        from edda.storage.pg_notify import PostgresNotifyListener

        return PostgresNotifyListener(dsn="postgresql://localhost/test")

    def test_initial_state(self, listener):
        """Test initial state before start()."""
        assert not listener.is_connected
        assert listener._connection is None
        assert listener._callbacks == {}
        assert not listener._running

    async def test_subscribe_before_connect(self, listener):
        """Subscribe should register callback even before connect."""
        callback_registered = False

        async def callback(payload: str) -> None:
            nonlocal callback_registered
            callback_registered = True

        # Subscribe before starting (will register in _callbacks but not LISTEN)
        await listener.subscribe("test_channel", callback)

        assert "test_channel" in listener._callbacks
        assert callback in listener._callbacks["test_channel"]


class TestPostgresNotifyListenerIntegration:
    """Integration tests for PostgresNotifyListener with real PostgreSQL."""

    @pytest.fixture
    async def pg_listener(self):
        """Create and start a PostgresNotifyListener with testcontainer."""
        try:
            import asyncpg  # noqa: F401
        except ImportError:
            pytest.skip("asyncpg not installed")

        try:
            from testcontainers.postgres import PostgresContainer
        except ImportError:
            pytest.skip("testcontainers not installed")

        from edda.storage.pg_notify import PostgresNotifyListener

        with PostgresContainer(
            "postgres:17", username="edda", password="edda_test", dbname="edda_test"
        ) as postgres:
            # Get asyncpg-compatible DSN
            db_url = postgres.get_connection_url().replace("psycopg2", "asyncpg")
            # Remove +asyncpg if present for asyncpg.connect()
            dsn = db_url.replace("+asyncpg", "")

            listener = PostgresNotifyListener(dsn=dsn)
            await listener.start()

            yield listener

            await listener.stop()

    async def test_connect_and_disconnect(self, pg_listener):
        """Test that listener connects and disconnects properly."""
        assert pg_listener.is_connected

    async def test_subscribe_and_receive(self, pg_listener):
        """Test subscribing and receiving notifications."""
        received_payloads = []

        async def callback(payload: str) -> None:
            received_payloads.append(payload)

        await pg_listener.subscribe("test_channel", callback)

        # Send a notification
        await pg_listener.notify("test_channel", '{"message": "hello"}')

        # Wait for notification to be processed
        await asyncio.sleep(0.5)

        assert len(received_payloads) == 1
        assert received_payloads[0] == '{"message": "hello"}'

    async def test_multiple_channels(self, pg_listener):
        """Test subscribing to multiple channels."""
        channel1_payloads = []
        channel2_payloads = []

        async def callback1(payload: str) -> None:
            channel1_payloads.append(payload)

        async def callback2(payload: str) -> None:
            channel2_payloads.append(payload)

        await pg_listener.subscribe("channel1", callback1)
        await pg_listener.subscribe("channel2", callback2)

        await pg_listener.notify("channel1", '{"channel": 1}')
        await pg_listener.notify("channel2", '{"channel": 2}')

        await asyncio.sleep(0.5)

        assert len(channel1_payloads) == 1
        assert len(channel2_payloads) == 1
        assert '{"channel": 1}' in channel1_payloads[0]
        assert '{"channel": 2}' in channel2_payloads[0]

    async def test_unsubscribe(self, pg_listener):
        """Test unsubscribing from a channel."""
        received_payloads = []

        async def callback(payload: str) -> None:
            received_payloads.append(payload)

        await pg_listener.subscribe("test_channel", callback)

        # Verify subscription works
        await pg_listener.notify("test_channel", '{"msg": "before"}')
        await asyncio.sleep(0.3)
        assert len(received_payloads) == 1

        # Unsubscribe
        await pg_listener.unsubscribe("test_channel")

        # Send another notification
        await pg_listener.notify("test_channel", '{"msg": "after"}')
        await asyncio.sleep(0.3)

        # Should not receive the second notification
        assert len(received_payloads) == 1


class TestHelperFunctions:
    """Tests for helper functions in pg_notify module."""

    def test_get_notify_channel_for_message(self):
        """Test unified channel name for message notifications."""
        try:
            from edda.storage.pg_notify import get_notify_channel_for_message
        except ImportError:
            pytest.skip("asyncpg not installed")

        # All channels map to unified name for cross-framework compatibility
        channel = get_notify_channel_for_message("order.created")
        assert channel == "workflow_channel_message"
        assert len(channel) <= 63  # PostgreSQL identifier limit

        # Same output for any input (unified channel)
        channel2 = get_notify_channel_for_message("order.created")
        assert channel == channel2

        channel3 = get_notify_channel_for_message("order.updated")
        assert channel == channel3  # Same unified channel

    def test_make_notify_payload(self):
        """Test payload JSON serialization."""
        try:
            from edda.storage.pg_notify import make_notify_payload
        except ImportError:
            pytest.skip("asyncpg not installed")

        payload = make_notify_payload({"wf_id": "test-123", "ts": 1234567890})
        assert '"wf_id":"test-123"' in payload
        assert '"ts":1234567890' in payload

        # Should be compact JSON (no spaces)
        assert " " not in payload
