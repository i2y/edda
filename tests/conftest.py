"""
Pytest configuration and fixtures for Edda tests.
"""

import os
import re
from pathlib import Path

import pytest
import pytest_asyncio
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.pool import StaticPool

from edda.serialization.json import JSONSerializer
from edda.storage.sqlalchemy_storage import SQLAlchemyStorage


# Path to schema migrations (relative to project root)
SCHEMA_DIR = Path(__file__).parent.parent / "schema" / "db" / "migrations"


async def apply_migrations(engine, db_type: str) -> None:
    """
    Apply database migrations from SQL files.

    Args:
        engine: SQLAlchemy async engine
        db_type: One of 'sqlite', 'postgresql', 'mysql'
    """
    migrations_dir = SCHEMA_DIR / db_type
    if not migrations_dir.exists():
        raise FileNotFoundError(f"Migration directory not found: {migrations_dir}")

    # Get all migration files sorted by name (timestamp order)
    migration_files = sorted(migrations_dir.glob("*.sql"))

    async with engine.begin() as conn:
        for migration_file in migration_files:
            content = migration_file.read_text()

            # Extract the "-- migrate:up" section
            up_match = re.search(r"-- migrate:up\s*(.*?)(?:-- migrate:down|$)", content, re.DOTALL)
            if not up_match:
                continue

            up_sql = up_match.group(1).strip()
            if not up_sql:
                continue

            # For MySQL, handle DELIMITER and stored procedures specially
            if db_type == "mysql" and "DELIMITER" in up_sql:
                # Skip procedure-based migrations for now, execute simpler parts
                # Remove DELIMITER blocks and procedures
                simple_sql = re.sub(
                    r"DROP PROCEDURE.*?;|DELIMITER.*?DELIMITER ;",
                    "",
                    up_sql,
                    flags=re.DOTALL
                )
                up_sql = simple_sql

            # Split by semicolons and execute each statement
            # Be careful with statements that might contain semicolons in strings
            statements = [s.strip() for s in up_sql.split(";") if s.strip()]

            for stmt in statements:
                # Skip empty statements
                if not stmt:
                    continue

                # Strip leading comment lines to get actual SQL
                lines = stmt.split("\n")
                sql_lines = []
                for line in lines:
                    stripped = line.strip()
                    # Skip empty lines and comment-only lines
                    if not stripped or stripped.startswith("--"):
                        continue
                    sql_lines.append(line)

                actual_sql = "\n".join(sql_lines).strip()
                if not actual_sql:
                    continue

                stmt = actual_sql
                try:
                    await conn.execute(text(stmt))
                except Exception as e:
                    # Log but continue - some statements might fail if objects exist
                    # (e.g., CREATE INDEX IF NOT EXISTS not supported in all DBs)
                    err_msg = str(e).lower()
                    if "already exists" not in err_msg and "duplicate column" not in err_msg:
                        raise


def get_database_urls() -> dict[str, str]:
    """Get database connection URLs from environment variables."""
    return {
        "sqlite": "sqlite+aiosqlite:///:memory:",
        "postgresql": os.getenv(
            "EDDA_TEST_POSTGRES_URL",
            "postgresql+asyncpg://edda:edda_test_password@localhost:5432/edda_test",
        ),
        "mysql": os.getenv(
            "EDDA_TEST_MYSQL_URL",
            "mysql+aiomysql://edda:edda_test_password@localhost:3306/edda_test",
        ),
    }


@pytest_asyncio.fixture(params=["sqlite", "postgresql", "mysql"])
async def db_storage(request):
    """
    Parametrized fixture that creates storage for SQLite, PostgreSQL, and MySQL.

    This fixture will run each test 3 times (once for each database).
    Uses Testcontainers for PostgreSQL and MySQL.
    Schema is created using dbmate migration files from schema/db/migrations/.
    """
    db_type = request.param

    if db_type == "sqlite":
        # SQLite: in-memory database
        # Use StaticPool to ensure all connections share the same in-memory database
        engine = create_async_engine(
            "sqlite+aiosqlite:///:memory:",
            echo=False,
            poolclass=StaticPool,
        )

        # Apply migrations from SQL files
        await apply_migrations(engine, "sqlite")

        storage = SQLAlchemyStorage(engine)

        # Create a sample workflow definition for tests
        await storage.upsert_workflow_definition(
            workflow_name="test_workflow",
            source_hash="abc123def456",
            source_code="async def test_workflow(ctx): pass",
        )

        yield storage
        await storage.close()

    elif db_type == "postgresql":
        # PostgreSQL with Testcontainers
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
        ) as postgres:
            db_url = postgres.get_connection_url().replace("psycopg2", "asyncpg")
            # Use READ COMMITTED to match production and avoid snapshot issues
            engine = create_async_engine(db_url, echo=False, isolation_level="READ COMMITTED")

            # Apply migrations from SQL files
            await apply_migrations(engine, "postgresql")

            storage = SQLAlchemyStorage(engine)

            await storage.upsert_workflow_definition(
                workflow_name="test_workflow",
                source_hash="abc123def456",
                source_code="async def test_workflow(ctx): pass",
            )

            yield storage

            # Cleanup (order matters due to foreign key constraints)
            async with AsyncSession(storage.engine) as session:
                await session.execute(text("DELETE FROM channel_message_claims"))
                await session.execute(text("DELETE FROM channel_delivery_cursors"))
                await session.execute(text("DELETE FROM channel_messages"))
                await session.execute(text("DELETE FROM channel_subscriptions"))
                await session.execute(text("DELETE FROM workflow_timer_subscriptions"))
                await session.execute(text("DELETE FROM workflow_group_memberships"))
                await session.execute(text("DELETE FROM workflow_compensations"))
                await session.execute(text("DELETE FROM workflow_history_archive"))
                await session.execute(text("DELETE FROM workflow_history"))
                await session.execute(text("DELETE FROM outbox_events"))
                await session.execute(text("DELETE FROM workflow_instances"))
                await session.execute(text("DELETE FROM workflow_definitions"))
                await session.commit()

            await storage.close()

    elif db_type == "mysql":
        # MySQL with Testcontainers
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
        ) as mysql:
            db_url = mysql.get_connection_url().replace("mysql://", "mysql+aiomysql://")
            # Use READ COMMITTED instead of REPEATABLE READ to avoid snapshot issues with SKIP LOCKED
            engine = create_async_engine(db_url, echo=False, isolation_level="READ COMMITTED")

            # Apply migrations from SQL files
            await apply_migrations(engine, "mysql")

            storage = SQLAlchemyStorage(engine)

            await storage.upsert_workflow_definition(
                workflow_name="test_workflow",
                source_hash="abc123def456",
                source_code="async def test_workflow(ctx): pass",
            )

            yield storage

            # Cleanup (order matters due to foreign key constraints)
            async with AsyncSession(storage.engine) as session:
                await session.execute(text("DELETE FROM channel_message_claims"))
                await session.execute(text("DELETE FROM channel_delivery_cursors"))
                await session.execute(text("DELETE FROM channel_messages"))
                await session.execute(text("DELETE FROM channel_subscriptions"))
                await session.execute(text("DELETE FROM workflow_timer_subscriptions"))
                await session.execute(text("DELETE FROM workflow_group_memberships"))
                await session.execute(text("DELETE FROM workflow_compensations"))
                await session.execute(text("DELETE FROM workflow_history_archive"))
                await session.execute(text("DELETE FROM workflow_history"))
                await session.execute(text("DELETE FROM outbox_events"))
                await session.execute(text("DELETE FROM workflow_instances"))
                await session.execute(text("DELETE FROM workflow_definitions"))
                await session.commit()

            await storage.close()


@pytest_asyncio.fixture
async def sqlite_storage():
    """Create an in-memory SQLite storage for testing."""
    # Use StaticPool to ensure all connections share the same in-memory database
    engine = create_async_engine(
        "sqlite+aiosqlite:///:memory:",
        echo=False,
        poolclass=StaticPool,
    )

    # Apply migrations from SQL files
    await apply_migrations(engine, "sqlite")

    storage = SQLAlchemyStorage(engine)

    # Create a sample workflow definition for tests
    await storage.upsert_workflow_definition(
        workflow_name="test_workflow",
        source_hash="abc123def456",  # Must match sample_workflow_data
        source_code="async def test_workflow(ctx): pass",
    )

    yield storage
    await storage.close()


@pytest_asyncio.fixture
async def postgresql_storage():
    """Create a PostgreSQL storage for testing with Testcontainers."""
    # Check if asyncpg driver is installed
    try:
        import asyncpg  # noqa: F401
    except ModuleNotFoundError:
        pytest.skip("asyncpg driver not installed. " "Install with: uv sync --extra postgresql")

    # Try to use Testcontainers
    try:
        from testcontainers.postgres import PostgresContainer
    except ModuleNotFoundError:
        pytest.skip("testcontainers not installed. " "Install with: uv sync --extra dev")

    # Start PostgreSQL container
    with PostgresContainer(
        "postgres:17", username="edda", password="edda_test_password", dbname="edda_test"
    ) as postgres:
        # Get connection URL and replace psycopg2 with asyncpg
        db_url = postgres.get_connection_url().replace("psycopg2", "asyncpg")
        engine = create_async_engine(db_url, echo=False, isolation_level="READ COMMITTED")

        # Apply migrations from SQL files
        await apply_migrations(engine, "postgresql")

        storage = SQLAlchemyStorage(engine)

        # Create a sample workflow definition for tests
        await storage.upsert_workflow_definition(
            workflow_name="test_workflow",
            source_hash="abc123def456",
            source_code="async def test_workflow(ctx): pass",
        )

        yield storage

        # Cleanup: Truncate all tables (order matters due to foreign key constraints)
        async with AsyncSession(storage.engine) as session:
            await session.execute(text("DELETE FROM channel_message_claims"))
            await session.execute(text("DELETE FROM channel_delivery_cursors"))
            await session.execute(text("DELETE FROM channel_messages"))
            await session.execute(text("DELETE FROM channel_subscriptions"))
            await session.execute(text("DELETE FROM workflow_timer_subscriptions"))
            await session.execute(text("DELETE FROM workflow_group_memberships"))
            await session.execute(text("DELETE FROM workflow_compensations"))
            await session.execute(text("DELETE FROM workflow_history_archive"))
            await session.execute(text("DELETE FROM workflow_history"))
            await session.execute(text("DELETE FROM outbox_events"))
            await session.execute(text("DELETE FROM workflow_instances"))
            await session.execute(text("DELETE FROM workflow_definitions"))
            await session.commit()

        await storage.close()
    # Container automatically stopped here


@pytest_asyncio.fixture
async def mysql_storage():
    """Create a MySQL storage for testing with Testcontainers."""
    # Check if aiomysql driver is installed
    try:
        import aiomysql  # noqa: F401
    except ModuleNotFoundError:
        pytest.skip("aiomysql driver not installed. " "Install with: uv sync --extra mysql")

    # Try to use Testcontainers
    try:
        from testcontainers.mysql import MySqlContainer
    except ModuleNotFoundError:
        pytest.skip("testcontainers not installed. " "Install with: uv sync --extra dev")

    # Start MySQL container
    with MySqlContainer(
        "mysql:9", username="edda", password="edda_test_password", dbname="edda_test"
    ) as mysql:
        # Get connection URL and add aiomysql driver
        db_url = mysql.get_connection_url().replace("mysql://", "mysql+aiomysql://")
        engine = create_async_engine(db_url, echo=False, isolation_level="READ COMMITTED")

        # Apply migrations from SQL files
        await apply_migrations(engine, "mysql")

        storage = SQLAlchemyStorage(engine)

        # Create a sample workflow definition for tests
        await storage.upsert_workflow_definition(
            workflow_name="test_workflow",
            source_hash="abc123def456",
            source_code="async def test_workflow(ctx): pass",
        )

        yield storage

        # Cleanup: Truncate all tables (order matters due to foreign key constraints)
        async with AsyncSession(storage.engine) as session:
            await session.execute(text("DELETE FROM channel_message_claims"))
            await session.execute(text("DELETE FROM channel_delivery_cursors"))
            await session.execute(text("DELETE FROM channel_messages"))
            await session.execute(text("DELETE FROM channel_subscriptions"))
            await session.execute(text("DELETE FROM workflow_timer_subscriptions"))
            await session.execute(text("DELETE FROM workflow_group_memberships"))
            await session.execute(text("DELETE FROM workflow_compensations"))
            await session.execute(text("DELETE FROM workflow_history_archive"))
            await session.execute(text("DELETE FROM workflow_history"))
            await session.execute(text("DELETE FROM outbox_events"))
            await session.execute(text("DELETE FROM workflow_instances"))
            await session.execute(text("DELETE FROM workflow_definitions"))
            await session.commit()

        await storage.close()
    # Container automatically stopped here


@pytest.fixture
def json_serializer():
    """Create a JSON serializer for testing."""
    return JSONSerializer()


@pytest.fixture
def sample_workflow_data():
    """Sample workflow instance data for testing."""
    return {
        "instance_id": "test-instance-123",
        "workflow_name": "test_workflow",
        "source_hash": "abc123def456",  # Mock hash for testing
        "owner_service": "test-service",
        "input_data": {"order_id": "order-123", "amount": 100},
    }


@pytest.fixture
def sample_event_data():
    """Sample event data for testing."""
    return {
        "event_type": "payment.completed",
        "event_source": "payment-service",
        "event_data": {"order_id": "order-123", "payment_id": "pay-456"},
    }


@pytest_asyncio.fixture
async def create_test_instance(sqlite_storage):
    """
    Helper fixture to create workflow instances with proper workflow definitions.

    Returns a function that can be called to create instances.
    Usage:
        instance_id = await create_test_instance(
            instance_id="test-123",
            workflow_name="my_workflow",
            input_data={"key": "value"}
        )
    """

    async def _create_instance(
        instance_id: str,
        workflow_name: str,
        input_data: dict,
        owner_service: str = "test-service",
        source_hash: str | None = None,
        source_code: str | None = None,
    ) -> str:
        """Create a test instance with workflow definition."""
        # Use default values if not provided
        if source_hash is None:
            source_hash = "test-hash-" + workflow_name
        if source_code is None:
            source_code = f"async def {workflow_name}(ctx): pass"

        # Ensure workflow definition exists
        await sqlite_storage.upsert_workflow_definition(
            workflow_name=workflow_name,
            source_hash=source_hash,
            source_code=source_code,
        )

        # Create instance
        await sqlite_storage.create_instance(
            instance_id=instance_id,
            workflow_name=workflow_name,
            source_hash=source_hash,
            owner_service=owner_service,
            input_data=input_data,
        )

        return instance_id

    return _create_instance
