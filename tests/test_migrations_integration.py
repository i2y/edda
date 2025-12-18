"""
Integration tests for database migrations using testcontainers.

These tests verify that migrations work correctly with real PostgreSQL and MySQL databases.
"""

import asyncio
from pathlib import Path

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

from edda.storage.migrations import apply_dbmate_migrations, get_applied_migrations

# Path to schema migrations (relative to project root)
SCHEMA_DIR = Path(__file__).parent.parent / "schema" / "db" / "migrations"


class TestMigrationsPostgreSQL:
    """Integration tests for PostgreSQL migrations."""

    @pytest.fixture
    def postgres_container(self):
        """Create a PostgreSQL container for testing."""
        try:
            import asyncpg  # noqa: F401
        except ModuleNotFoundError:
            pytest.skip("asyncpg not installed")

        try:
            from testcontainers.postgres import PostgresContainer
        except ModuleNotFoundError:
            pytest.skip("testcontainers not installed")

        with PostgresContainer(
            "postgres:17", username="edda", password="edda_test_password", dbname="edda_test"
        ) as container:
            yield container

    @pytest.mark.asyncio
    async def test_apply_migrations_postgresql(self, postgres_container):
        """Test applying migrations to PostgreSQL."""
        db_url = postgres_container.get_connection_url().replace("psycopg2", "asyncpg")
        engine = create_async_engine(db_url, echo=False)

        try:
            # Apply migrations
            applied = await apply_dbmate_migrations(engine, SCHEMA_DIR)

            # Should have applied at least the initial migration
            assert len(applied) >= 1
            assert "20251217000000" in applied

            # Verify schema_migrations table has the record
            recorded = await get_applied_migrations(engine)
            assert "20251217000000" in recorded

            # Apply again - should be idempotent
            applied_again = await apply_dbmate_migrations(engine, SCHEMA_DIR)
            assert len(applied_again) == 0  # No new migrations

            # Verify core tables exist
            async with engine.connect() as conn:
                result = await conn.execute(
                    text(
                        """
                        SELECT table_name FROM information_schema.tables
                        WHERE table_schema = 'public'
                        AND table_name = 'workflow_instances'
                        """
                    )
                )
                tables = [row[0] for row in result.fetchall()]
                assert "workflow_instances" in tables
        finally:
            await engine.dispose()

    @pytest.mark.asyncio
    async def test_multi_worker_postgresql(self, postgres_container):
        """Test concurrent migration from multiple workers."""
        db_url = postgres_container.get_connection_url().replace("psycopg2", "asyncpg")

        async def run_migration():
            engine = create_async_engine(db_url, echo=False)
            try:
                return await apply_dbmate_migrations(engine, SCHEMA_DIR)
            finally:
                await engine.dispose()

        # Simulate 5 workers starting simultaneously
        results = await asyncio.gather(
            run_migration(),
            run_migration(),
            run_migration(),
            run_migration(),
            run_migration(),
        )

        # Exactly one worker should have applied the migration
        total_applied = sum(len(r) for r in results)
        assert total_applied == 1, f"Expected 1 migration applied, got {total_applied}"

        # Verify only one record in schema_migrations
        engine = create_async_engine(db_url, echo=False)
        try:
            async with engine.connect() as conn:
                result = await conn.execute(
                    text("SELECT COUNT(*) FROM schema_migrations WHERE version = '20251217000000'")
                )
                count = result.scalar()
                assert count == 1, f"Expected 1 migration record, got {count}"
        finally:
            await engine.dispose()


class TestMigrationsMySQL:
    """Integration tests for MySQL migrations."""

    @pytest.fixture
    def mysql_container(self):
        """Create a MySQL container for testing."""
        try:
            import aiomysql  # noqa: F401
        except ModuleNotFoundError:
            pytest.skip("aiomysql not installed")

        try:
            from testcontainers.mysql import MySqlContainer
        except ModuleNotFoundError:
            pytest.skip("testcontainers not installed")

        with MySqlContainer(
            "mysql:9", username="edda", password="edda_test_password", dbname="edda_test"
        ) as container:
            yield container

    @pytest.mark.asyncio
    async def test_apply_migrations_mysql(self, mysql_container):
        """Test applying migrations to MySQL."""
        db_url = mysql_container.get_connection_url().replace("mysql://", "mysql+aiomysql://")
        engine = create_async_engine(db_url, echo=False)

        try:
            # Apply migrations
            applied = await apply_dbmate_migrations(engine, SCHEMA_DIR)

            # Should have applied at least the initial migration
            assert len(applied) >= 1
            assert "20251217000000" in applied

            # Verify schema_migrations table has the record
            recorded = await get_applied_migrations(engine)
            assert "20251217000000" in recorded

            # Apply again - should be idempotent
            applied_again = await apply_dbmate_migrations(engine, SCHEMA_DIR)
            assert len(applied_again) == 0  # No new migrations

            # Verify core tables exist
            async with engine.connect() as conn:
                result = await conn.execute(
                    text(
                        """
                        SELECT table_name FROM information_schema.tables
                        WHERE table_schema = 'edda_test'
                        AND table_name = 'workflow_instances'
                        """
                    )
                )
                tables = [row[0] for row in result.fetchall()]
                assert "workflow_instances" in tables
        finally:
            await engine.dispose()

    @pytest.mark.asyncio
    async def test_multi_worker_mysql(self, mysql_container):
        """Test concurrent migration from multiple workers on MySQL."""
        db_url = mysql_container.get_connection_url().replace("mysql://", "mysql+aiomysql://")

        async def run_migration():
            engine = create_async_engine(db_url, echo=False)
            try:
                return await apply_dbmate_migrations(engine, SCHEMA_DIR)
            finally:
                await engine.dispose()

        # Simulate 5 workers starting simultaneously
        results = await asyncio.gather(
            run_migration(),
            run_migration(),
            run_migration(),
            run_migration(),
            run_migration(),
        )

        # Exactly one worker should have applied the migration
        total_applied = sum(len(r) for r in results)
        assert total_applied == 1, f"Expected 1 migration applied, got {total_applied}"

        # Verify only one record in schema_migrations
        engine = create_async_engine(db_url, echo=False)
        try:
            async with engine.connect() as conn:
                result = await conn.execute(
                    text("SELECT COUNT(*) FROM schema_migrations WHERE version = '20251217000000'")
                )
                count = result.scalar()
                assert count == 1, f"Expected 1 migration record, got {count}"
        finally:
            await engine.dispose()
