"""Tests for automatic message cleanup functionality."""

from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy import update

from edda.app import EddaApp
from edda.storage.sqlalchemy_storage import ChannelMessage, SQLAlchemyStorage


@pytest.fixture
async def storage():
    """Create an in-memory SQLite storage for testing."""
    from sqlalchemy.ext.asyncio import create_async_engine

    engine = create_async_engine("sqlite+aiosqlite:///:memory:", future=True)
    storage = SQLAlchemyStorage(engine)
    await storage.initialize()
    yield storage
    await storage.close()


@pytest.fixture
async def app(storage):
    """Create an EddaApp with the test storage."""
    app = EddaApp(
        service_name="test-service",
        db_url="sqlite+aiosqlite:///:memory:",
        message_retention_days=7,
    )
    # Replace the default storage with our test storage
    app.storage = storage
    yield app


class TestMessageCleanup:
    """Tests for message cleanup functionality."""

    @pytest.mark.asyncio
    async def test_cleanup_old_channel_messages_deletes_old_messages(self, storage):
        """Test that messages older than retention period are deleted."""
        # Create an old message (8 days ago)
        await storage.publish_to_channel(
            channel="test-channel",
            data={"test": "old message"},
            metadata=None,
        )

        # Get the message and manually set its published_at to 8 days ago
        async with storage.engine.begin() as conn:
            old_time = datetime.now(UTC) - timedelta(days=8)
            await conn.execute(update(ChannelMessage).values(published_at=old_time))

        # Create a recent message (today)
        await storage.publish_to_channel(
            channel="test-channel",
            data={"test": "new message"},
            metadata=None,
        )

        # Run cleanup with 7 days retention
        deleted_count = await storage.cleanup_old_channel_messages(older_than_days=7)

        # Should have deleted 1 old message
        assert deleted_count == 1

    @pytest.mark.asyncio
    async def test_cleanup_old_channel_messages_keeps_recent_messages(self, storage):
        """Test that messages within retention period are kept."""
        # Create a recent message (today)
        await storage.publish_to_channel(
            channel="test-channel",
            data={"test": "recent message"},
            metadata=None,
        )

        # Run cleanup with 7 days retention
        deleted_count = await storage.cleanup_old_channel_messages(older_than_days=7)

        # Should have deleted 0 messages
        assert deleted_count == 0

    @pytest.mark.asyncio
    async def test_message_retention_days_parameter(self):
        """Test that message_retention_days parameter is correctly stored."""
        app = EddaApp(
            service_name="test-service",
            db_url="sqlite+aiosqlite:///:memory:",
            message_retention_days=14,
        )
        assert app._message_retention_days == 14
        await app.shutdown()

    @pytest.mark.asyncio
    async def test_message_retention_days_default(self):
        """Test that default message_retention_days is 7."""
        app = EddaApp(
            service_name="test-service",
            db_url="sqlite+aiosqlite:///:memory:",
        )
        assert app._message_retention_days == 7
        await app.shutdown()

    @pytest.mark.asyncio
    async def test_cleanup_multiple_old_messages(self, storage):
        """Test that multiple old messages are deleted."""
        # Create 3 messages
        for i in range(3):
            await storage.publish_to_channel(
                channel="test-channel",
                data={"test": f"message {i}"},
                metadata=None,
            )

        # Set all messages to be 10 days old
        async with storage.engine.begin() as conn:
            old_time = datetime.now(UTC) - timedelta(days=10)
            await conn.execute(update(ChannelMessage).values(published_at=old_time))

        # Run cleanup
        deleted_count = await storage.cleanup_old_channel_messages(older_than_days=7)

        # Should have deleted all 3 messages
        assert deleted_count == 3

    @pytest.mark.asyncio
    async def test_cleanup_with_mixed_ages(self, storage):
        """Test cleanup with messages of different ages."""
        # Create an old message (8 days old)
        await storage.publish_to_channel(
            channel="test-channel",
            data={"test": "old message 1"},
            metadata=None,
        )

        # Set this message to be old
        async with storage.engine.begin() as conn:
            old_time = datetime.now(UTC) - timedelta(days=8)
            await conn.execute(
                update(ChannelMessage).where(ChannelMessage.id == 1).values(published_at=old_time)
            )

        # Create another old message (10 days old)
        await storage.publish_to_channel(
            channel="test-channel",
            data={"test": "old message 2"},
            metadata=None,
        )

        # Set this message to be older
        async with storage.engine.begin() as conn:
            older_time = datetime.now(UTC) - timedelta(days=10)
            await conn.execute(
                update(ChannelMessage).where(ChannelMessage.id == 2).values(published_at=older_time)
            )

        # Create a recent message (today)
        await storage.publish_to_channel(
            channel="test-channel",
            data={"test": "new message"},
            metadata=None,
        )

        # Run cleanup with 7 days retention
        deleted_count = await storage.cleanup_old_channel_messages(older_than_days=7)

        # Should have deleted 2 old messages
        assert deleted_count == 2

    @pytest.mark.asyncio
    async def test_cleanup_with_custom_retention_period(self, storage):
        """Test cleanup with custom retention period."""
        # Create a message that is 3 days old
        await storage.publish_to_channel(
            channel="test-channel",
            data={"test": "message"},
            metadata=None,
        )

        # Set message to be 3 days old
        async with storage.engine.begin() as conn:
            old_time = datetime.now(UTC) - timedelta(days=3)
            await conn.execute(update(ChannelMessage).values(published_at=old_time))

        # With 7 days retention, should NOT be deleted
        deleted_count = await storage.cleanup_old_channel_messages(older_than_days=7)
        assert deleted_count == 0

        # With 2 days retention, SHOULD be deleted
        deleted_count = await storage.cleanup_old_channel_messages(older_than_days=2)
        assert deleted_count == 1

    @pytest.mark.asyncio
    async def test_cleanup_empty_database(self, storage):
        """Test cleanup when there are no messages."""
        deleted_count = await storage.cleanup_old_channel_messages(older_than_days=7)
        assert deleted_count == 0
