"""
Tests for automatic schema migration functionality.

These tests verify that SQLAlchemyStorage.initialize() automatically
adds missing columns to existing tables.
"""

import pytest
import pytest_asyncio
from sqlalchemy import inspect, text
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine

from edda.storage.sqlalchemy_storage import SQLAlchemyStorage


@pytest_asyncio.fixture
async def storage_with_old_schema():
    """
    Create a storage instance with a "legacy" schema (missing columns).

    This simulates upgrading from an older version of Edda where
    certain columns don't exist yet.
    """
    engine = create_async_engine("sqlite+aiosqlite:///:memory:", echo=False)

    # Create a minimal schema WITHOUT some columns that exist in current ORM
    # We'll manually create tables to simulate an old schema
    async with engine.begin() as conn:
        # Create workflow_instances table without 'continued_from' column
        await conn.execute(
            text(
                """
            CREATE TABLE workflow_instances (
                instance_id VARCHAR(255) PRIMARY KEY,
                workflow_name VARCHAR(255) NOT NULL,
                source_hash VARCHAR(64) NOT NULL,
                owner_service VARCHAR(255) NOT NULL,
                status VARCHAR(50) NOT NULL DEFAULT 'running',
                current_activity_id VARCHAR(255),
                started_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                input_data TEXT NOT NULL,
                output_data TEXT,
                locked_by VARCHAR(255),
                locked_at DATETIME,
                lock_timeout_seconds INTEGER,
                lock_expires_at DATETIME
            )
        """
            )
        )

        # Create schema_version table
        await conn.execute(
            text(
                """
            CREATE TABLE schema_version (
                version INTEGER PRIMARY KEY,
                applied_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                description TEXT NOT NULL
            )
        """
            )
        )

        # Create workflow_definitions table (required for FK)
        await conn.execute(
            text(
                """
            CREATE TABLE workflow_definitions (
                workflow_name VARCHAR(255) NOT NULL,
                source_hash VARCHAR(64) NOT NULL,
                source_code TEXT NOT NULL,
                created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (workflow_name, source_hash)
            )
        """
            )
        )

        # Create other required tables with minimal columns
        await conn.execute(
            text(
                """
            CREATE TABLE workflow_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                instance_id VARCHAR(255) NOT NULL,
                activity_id VARCHAR(255) NOT NULL,
                event_type VARCHAR(100) NOT NULL,
                data_type VARCHAR(10) NOT NULL,
                event_data TEXT,
                event_data_binary BLOB,
                created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
            )
        """
            )
        )

        await conn.execute(
            text(
                """
            CREATE TABLE workflow_history_archive (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                instance_id VARCHAR(255) NOT NULL,
                activity_id VARCHAR(255) NOT NULL,
                event_type VARCHAR(100) NOT NULL,
                event_data TEXT NOT NULL,
                created_at DATETIME NOT NULL,
                archived_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
            )
        """
            )
        )

        await conn.execute(
            text(
                """
            CREATE TABLE workflow_compensations (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                instance_id VARCHAR(255) NOT NULL,
                activity_id VARCHAR(255) NOT NULL,
                activity_name VARCHAR(255) NOT NULL,
                args TEXT NOT NULL,
                created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
            )
        """
            )
        )

        await conn.execute(
            text(
                """
            CREATE TABLE workflow_timer_subscriptions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                instance_id VARCHAR(255) NOT NULL,
                timer_id VARCHAR(255) NOT NULL,
                expires_at DATETIME NOT NULL,
                activity_id VARCHAR(255),
                created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
            )
        """
            )
        )

        await conn.execute(
            text(
                """
            CREATE TABLE outbox_events (
                event_id VARCHAR(255) PRIMARY KEY,
                event_type VARCHAR(255) NOT NULL,
                event_source VARCHAR(255) NOT NULL,
                data_type VARCHAR(10) NOT NULL,
                event_data TEXT,
                event_data_binary BLOB,
                content_type VARCHAR(100) NOT NULL DEFAULT 'application/json',
                created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                published_at DATETIME,
                status VARCHAR(50) NOT NULL DEFAULT 'pending',
                retry_count INTEGER NOT NULL DEFAULT 0,
                last_error TEXT
            )
        """
            )
        )

        await conn.execute(
            text(
                """
            CREATE TABLE workflow_group_memberships (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                instance_id VARCHAR(255) NOT NULL,
                group_name VARCHAR(255) NOT NULL,
                joined_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
            )
        """
            )
        )

    storage = SQLAlchemyStorage(engine)
    yield storage, engine
    await engine.dispose()


@pytest.mark.asyncio
@pytest.mark.skip(reason="Auto-migration is now handled by dbmate. See schema/db/migrations/")
class TestAutoMigration:
    """Test suite for automatic schema migration.

    NOTE: These tests are skipped because schema migrations are now handled
    externally by dbmate. See the schema/ directory for migration files.
    """

    async def test_auto_migration_adds_missing_column(self, storage_with_old_schema):
        """Test that initialize() adds the missing 'continued_from' column."""
        storage, engine = storage_with_old_schema

        # Verify column doesn't exist before migration
        async with engine.begin() as conn:

            def check_column_exists(conn):
                inspector = inspect(conn)
                columns = {col["name"] for col in inspector.get_columns("workflow_instances")}
                return "continued_from" in columns

            exists_before = await conn.run_sync(check_column_exists)
            assert not exists_before, "continued_from should not exist before migration"

        # Run initialize() which should trigger auto-migration
        await storage.initialize()

        # Verify column now exists
        async with engine.begin() as conn:
            exists_after = await conn.run_sync(check_column_exists)
            assert exists_after, "continued_from should exist after migration"

    async def test_auto_migration_is_idempotent(self, storage_with_old_schema):
        """Test that running initialize() multiple times is safe."""
        storage, engine = storage_with_old_schema

        # Run initialize() multiple times
        await storage.initialize()
        await storage.initialize()
        await storage.initialize()

        # Should not raise any errors and column should exist
        async with engine.begin() as conn:

            def check_column_exists(conn):
                inspector = inspect(conn)
                columns = {col["name"] for col in inspector.get_columns("workflow_instances")}
                return "continued_from" in columns

            exists = await conn.run_sync(check_column_exists)
            assert exists, "continued_from should exist after multiple initialize() calls"

    async def test_auto_migration_preserves_existing_data(self, storage_with_old_schema):
        """Test that auto-migration doesn't lose existing data."""
        storage, engine = storage_with_old_schema

        # Insert some data before migration
        async with AsyncSession(engine) as session:
            await session.execute(
                text(
                    """
                INSERT INTO workflow_definitions (workflow_name, source_hash, source_code)
                VALUES ('test_workflow', 'hash123', 'async def test(): pass')
            """
                )
            )
            await session.execute(
                text(
                    """
                INSERT INTO workflow_instances
                    (instance_id, workflow_name, source_hash, owner_service, input_data)
                VALUES ('inst-1', 'test_workflow', 'hash123', 'test-service', '{}')
            """
                )
            )
            await session.commit()

        # Run migration
        await storage.initialize()

        # Verify data still exists
        async with AsyncSession(engine) as session:
            result = await session.execute(
                text("SELECT instance_id, workflow_name FROM workflow_instances")
            )
            row = result.fetchone()
            assert row is not None
            assert row[0] == "inst-1"
            assert row[1] == "test_workflow"

    async def test_migrated_column_is_usable(self, storage_with_old_schema):
        """Test that the migrated column can be used after migration."""
        storage, engine = storage_with_old_schema

        # Run migration
        await storage.initialize()

        # Create a workflow definition first
        async with AsyncSession(engine) as session:
            await session.execute(
                text(
                    """
                INSERT INTO workflow_definitions (workflow_name, source_hash, source_code)
                VALUES ('test_workflow', 'hash123', 'async def test(): pass')
            """
                )
            )
            await session.commit()

        # Try to insert data using the new column
        async with AsyncSession(engine) as session:
            await session.execute(
                text(
                    """
                INSERT INTO workflow_instances
                    (instance_id, workflow_name, source_hash, owner_service, input_data, continued_from)
                VALUES ('inst-new', 'test_workflow', 'hash123', 'test-service', '{}', 'inst-old')
            """
                )
            )
            await session.commit()

        # Verify it was stored correctly
        async with AsyncSession(engine) as session:
            result = await session.execute(
                text(
                    "SELECT instance_id, continued_from FROM workflow_instances WHERE instance_id = 'inst-new'"
                )
            )
            row = result.fetchone()
            assert row is not None
            assert row[0] == "inst-new"
            assert row[1] == "inst-old"


@pytest.mark.asyncio
class TestFreshDatabaseInitialization:
    """Test that fresh database initialization works correctly."""

    async def test_fresh_database_creates_all_tables(self):
        """Test that initialize() creates all tables in a fresh database."""
        engine = create_async_engine("sqlite+aiosqlite:///:memory:", echo=False)
        storage = SQLAlchemyStorage(engine)

        # Initialize fresh database
        await storage.initialize()

        # Verify all expected tables exist
        expected_tables = {
            "schema_version",
            "workflow_definitions",
            "workflow_instances",
            "workflow_history",
            "workflow_history_archive",
            "workflow_compensations",
            "workflow_timer_subscriptions",
            "outbox_events",
            "workflow_group_memberships",
        }

        async with engine.begin() as conn:

            def get_tables(conn):
                inspector = inspect(conn)
                return set(inspector.get_table_names())

            actual_tables = await conn.run_sync(get_tables)
            assert expected_tables.issubset(
                actual_tables
            ), f"Missing tables: {expected_tables - actual_tables}"

        await engine.dispose()

    async def test_fresh_database_has_all_columns(self):
        """Test that workflow_instances has all expected columns."""
        engine = create_async_engine("sqlite+aiosqlite:///:memory:", echo=False)
        storage = SQLAlchemyStorage(engine)

        await storage.initialize()

        # Expected columns in workflow_instances
        expected_columns = {
            "instance_id",
            "workflow_name",
            "source_hash",
            "owner_service",
            "status",
            "current_activity_id",
            "continued_from",  # The column that was missing in PostgreSQL
            "started_at",
            "updated_at",
            "input_data",
            "output_data",
            "locked_by",
            "locked_at",
            "lock_timeout_seconds",
            "lock_expires_at",
        }

        async with engine.begin() as conn:

            def get_columns(conn):
                inspector = inspect(conn)
                return {col["name"] for col in inspector.get_columns("workflow_instances")}

            actual_columns = await conn.run_sync(get_columns)
            assert expected_columns.issubset(
                actual_columns
            ), f"Missing columns: {expected_columns - actual_columns}"

        await engine.dispose()
