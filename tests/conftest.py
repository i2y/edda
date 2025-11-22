"""
Pytest configuration and fixtures for Edda tests.
"""

import os

import pytest
import pytest_asyncio
from sqlalchemy import text
from sqlalchemy.exc import OperationalError
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine

from edda.serialization.json import JSONSerializer
from edda.storage.sqlalchemy_storage import SQLAlchemyStorage


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
    If PostgreSQL or MySQL is not available, those tests will be skipped.
    """
    db_type = request.param
    db_urls = get_database_urls()
    db_url = db_urls[db_type]

    # Check if required driver is installed
    if db_type == "postgresql":
        try:
            import asyncpg  # noqa: F401
        except ModuleNotFoundError:
            pytest.skip(
                "asyncpg driver not installed. "
                "Install with: uv sync --extra postgresql"
            )
    elif db_type == "mysql":
        try:
            import aiomysql  # noqa: F401
        except ModuleNotFoundError:
            pytest.skip(
                "aiomysql driver not installed. "
                "Install with: uv sync --extra mysql"
            )

    # Try to connect
    engine = create_async_engine(db_url, echo=False)
    storage = SQLAlchemyStorage(engine)

    try:
        await storage.initialize()
    except OperationalError as e:
        pytest.skip(
            f"{db_type.upper()} is not available. "
            f"Please ensure {db_type.upper()} is running and accessible.\n"
            f"Error: {e}"
        )

    # Create a sample workflow definition for tests
    await storage.upsert_workflow_definition(
        workflow_name="test_workflow",
        source_hash="abc123def456",  # Must match sample_workflow_data
        source_code="async def test_workflow(ctx): pass",
    )

    yield storage

    # Cleanup: For persistent databases (PostgreSQL, MySQL), truncate all tables
    if db_type in ["postgresql", "mysql"]:
        async with AsyncSession(storage.engine) as session:
            # Truncate all tables in reverse order of dependencies
            await session.execute(text("DELETE FROM workflow_timer_subscriptions"))
            await session.execute(text("DELETE FROM workflow_event_subscriptions"))
            await session.execute(text("DELETE FROM workflow_history"))
            await session.execute(text("DELETE FROM outbox_events"))
            await session.execute(text("DELETE FROM workflow_instances"))
            await session.execute(text("DELETE FROM workflow_definitions"))
            await session.commit()

    await storage.close()


@pytest_asyncio.fixture
async def sqlite_storage():
    """Create an in-memory SQLite storage for testing."""
    engine = create_async_engine("sqlite+aiosqlite:///:memory:", echo=False)
    storage = SQLAlchemyStorage(engine)
    await storage.initialize()

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
    """Create a PostgreSQL storage for testing (requires Docker)."""
    # Check if asyncpg driver is installed
    try:
        import asyncpg  # noqa: F401
    except ModuleNotFoundError:
        pytest.skip(
            "asyncpg driver not installed. "
            "Install with: uv sync --extra postgresql"
        )

    db_urls = get_database_urls()
    engine = create_async_engine(db_urls["postgresql"], echo=False)
    storage = SQLAlchemyStorage(engine)

    try:
        await storage.initialize()
    except OperationalError as e:
        pytest.skip(
            f"PostgreSQL is not available. "
            f"Please ensure PostgreSQL is running and accessible.\n"
            f"Error: {e}"
        )

    # Create a sample workflow definition for tests
    await storage.upsert_workflow_definition(
        workflow_name="test_workflow",
        source_hash="abc123def456",
        source_code="async def test_workflow(ctx): pass",
    )

    yield storage

    # Cleanup: Truncate all tables
    async with AsyncSession(storage.engine) as session:
        await session.execute(text("DELETE FROM workflow_timer_subscriptions"))
        await session.execute(text("DELETE FROM workflow_event_subscriptions"))
        await session.execute(text("DELETE FROM workflow_compensations"))
        await session.execute(text("DELETE FROM workflow_history"))
        await session.execute(text("DELETE FROM outbox_events"))
        await session.execute(text("DELETE FROM workflow_instances"))
        await session.execute(text("DELETE FROM workflow_definitions"))
        await session.commit()

    await storage.close()


@pytest_asyncio.fixture
async def mysql_storage():
    """Create a MySQL storage for testing (requires Docker)."""
    # Check if aiomysql driver is installed
    try:
        import aiomysql  # noqa: F401
    except ModuleNotFoundError:
        pytest.skip(
            "aiomysql driver not installed. "
            "Install with: uv sync --extra mysql"
        )

    db_urls = get_database_urls()
    engine = create_async_engine(db_urls["mysql"], echo=False)
    storage = SQLAlchemyStorage(engine)

    try:
        await storage.initialize()
    except OperationalError as e:
        pytest.skip(
            f"MySQL is not available. "
            f"Please ensure MySQL is running and accessible.\n"
            f"Error: {e}"
        )

    # Create a sample workflow definition for tests
    await storage.upsert_workflow_definition(
        workflow_name="test_workflow",
        source_hash="abc123def456",
        source_code="async def test_workflow(ctx): pass",
    )

    yield storage

    # Cleanup: Truncate all tables
    async with AsyncSession(storage.engine) as session:
        await session.execute(text("DELETE FROM workflow_timer_subscriptions"))
        await session.execute(text("DELETE FROM workflow_event_subscriptions"))
        await session.execute(text("DELETE FROM workflow_compensations"))
        await session.execute(text("DELETE FROM workflow_history"))
        await session.execute(text("DELETE FROM outbox_events"))
        await session.execute(text("DELETE FROM workflow_instances"))
        await session.execute(text("DELETE FROM workflow_definitions"))
        await session.commit()

    await storage.close()


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
