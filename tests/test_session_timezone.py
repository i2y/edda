"""Database sessions are pinned to UTC.

The shared schema stores TIMESTAMP/DATETIME values without a zone and
compares them with NOW(); every framework on it writes and reads UTC. A
server or database whose default zone is not UTC must not shift leases,
timers or deadlines: east of UTC a fresh lease would look stale at once.
"""

import itertools
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
from sqlalchemy import make_url, text
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.pool import NullPool

from edda.app import EddaApp
from edda.locking import new_lock_identity
from edda.storage.migrations import apply_dbmate_migrations
from edda.storage.sqlalchemy_storage import SQLAlchemyStorage

SCHEMA_DIR = Path(__file__).parent.parent / "schema" / "db" / "migrations"
_seq = itertools.count()

pytestmark = pytest.mark.asyncio


async def _create_database(base_url: str, *setup: str) -> str:
    name = f"edda_tz{next(_seq)}"
    admin = create_async_engine(base_url, poolclass=NullPool, isolation_level="AUTOCOMMIT")
    async with admin.connect() as conn:
        await conn.execute(text(f"CREATE DATABASE {name}"))
        for statement in setup:
            await conn.execute(text(statement.format(name=name)))
    await admin.dispose()
    return make_url(base_url).set(database=name).render_as_string(hide_password=False)


async def _assert_fresh_lease_is_utc_and_kept(storage) -> None:
    await apply_dbmate_migrations(storage.engine, SCHEMA_DIR)
    await storage.upsert_workflow_definition(
        workflow_name="tz_wf", source_hash="h", source_code="pass"
    )
    await storage.create_instance(
        instance_id="tz-1",
        workflow_name="tz_wf",
        source_hash="h",
        owner_service="t",
        input_data={},
    )
    lock_id = new_lock_identity("worker-tz")
    assert await storage.try_acquire_lock("tz-1", lock_id, timeout_seconds=300)

    # The stale-lock sweep does not take a fresh lease back...
    assert await storage.cleanup_stale_locks() == []
    assert (await storage.get_instance("tz-1"))["locked_by"] == lock_id
    # ...and the lease is stored as UTC, as every framework reads it.
    async with storage.engine.connect() as conn:
        expires = (
            await conn.execute(
                text("SELECT lock_expires_at FROM workflow_instances WHERE instance_id = 'tz-1'")
            )
        ).scalar_one()
    expected = datetime.now(UTC).replace(tzinfo=None) + timedelta(seconds=300)
    assert abs((expires.replace(tzinfo=None) - expected).total_seconds()) < 60


async def test_postgresql_sessions_are_pinned_to_utc(multidb_postgresql_url):
    url = await _create_database(
        multidb_postgresql_url, "ALTER DATABASE {name} SET timezone TO 'Asia/Tokyo'"
    )
    plain = create_async_engine(url, poolclass=NullPool)
    async with plain.connect() as conn:
        assert (await conn.execute(text("SHOW TIME ZONE"))).scalar_one() == "Asia/Tokyo"
    await plain.dispose()

    storage = EddaApp(service_name="tz-test", db_url=url).storage
    try:
        async with storage.engine.connect() as conn:
            assert (await conn.execute(text("SHOW TIME ZONE"))).scalar_one() == "UTC"
        await _assert_fresh_lease_is_utc_and_kept(storage)
    finally:
        await storage.close()


async def test_mysql_sessions_are_pinned_to_utc(multidb_mysql_url):
    url = await _create_database(multidb_mysql_url)
    admin = create_async_engine(multidb_mysql_url, poolclass=NullPool, isolation_level="AUTOCOMMIT")
    async with admin.connect() as conn:
        original = (await conn.execute(text("SELECT @@global.time_zone"))).scalar_one()
        await conn.execute(text("SET GLOBAL time_zone = '+09:00'"))
    try:
        storage = EddaApp(service_name="tz-test", db_url=url).storage
        try:
            async with storage.engine.connect() as conn:
                zone = (await conn.execute(text("SELECT @@session.time_zone"))).scalar_one()
                isolation = await _isolation(conn)
            assert zone == "+00:00"
            assert isolation == "READ-COMMITTED"
            await _assert_fresh_lease_is_utc_and_kept(storage)
        finally:
            await storage.close()
    finally:
        async with admin.connect() as conn:
            await conn.execute(text("SET GLOBAL time_zone = :z"), {"z": original})
        await admin.dispose()


async def _isolation(conn) -> str:
    return (await conn.execute(text("SELECT @@transaction_isolation"))).scalar_one()


async def test_postgresql_caller_engine_is_pinned_to_utc(multidb_postgresql_url):
    url = await _create_database(
        multidb_postgresql_url, "ALTER DATABASE {name} SET timezone TO 'Asia/Tokyo'"
    )
    # One pooled connection: the second connect below reuses the first one
    storage = SQLAlchemyStorage(create_async_engine(url, pool_size=1, max_overflow=4))
    try:
        async with storage.engine.connect() as conn:
            await conn.execute(text("SELECT 1"))
            await conn.rollback()  # must not undo the session setting
        async with storage.engine.connect() as conn:
            assert (await conn.execute(text("SHOW TIME ZONE"))).scalar_one() == "UTC"
        await _assert_fresh_lease_is_utc_and_kept(storage)
    finally:
        await storage.close()


async def test_mysql_caller_engine_is_pinned_to_utc_and_read_committed(multidb_mysql_url):
    url = await _create_database(multidb_mysql_url)
    admin = create_async_engine(multidb_mysql_url, poolclass=NullPool, isolation_level="AUTOCOMMIT")
    async with admin.connect() as conn:
        original = (await conn.execute(text("SELECT @@global.time_zone"))).scalar_one()
        await conn.execute(text("SET GLOBAL time_zone = '+09:00'"))
    try:
        storage = SQLAlchemyStorage(create_async_engine(url))
        try:
            async with storage.engine.connect() as conn:
                zone = (await conn.execute(text("SELECT @@session.time_zone"))).scalar_one()
                isolation = await _isolation(conn)
            async with storage.engine.begin() as conn:
                isolation_in_transaction = await _isolation(conn)
            assert zone == "+00:00"
            assert isolation == isolation_in_transaction == "READ-COMMITTED"
            await _assert_fresh_lease_is_utc_and_kept(storage)
        finally:
            await storage.close()
    finally:
        async with admin.connect() as conn:
            await conn.execute(text("SET GLOBAL time_zone = :z"), {"z": original})
        await admin.dispose()


async def test_mysql_explicit_isolation_level_is_kept(multidb_mysql_url):
    url = await _create_database(multidb_mysql_url)
    storage = SQLAlchemyStorage(create_async_engine(url, isolation_level="SERIALIZABLE"))
    try:
        async with storage.engine.connect() as conn:
            zone = (await conn.execute(text("SELECT @@session.time_zone"))).scalar_one()
            isolation = await _isolation(conn)
        assert zone == "+00:00"
        assert isolation == "SERIALIZABLE"
    finally:
        await storage.close()


async def test_postgresql_connection_opened_before_the_storage_is_pinned(
    multidb_postgresql_url,
):
    url = await _create_database(
        multidb_postgresql_url, "ALTER DATABASE {name} SET timezone TO 'Asia/Tokyo'"
    )
    # The caller uses its engine first (migrations, its own queries): the one
    # pooled connection predates the storage and its connect listener.
    engine = create_async_engine(url, pool_size=1, max_overflow=4)
    async with engine.connect() as conn:
        assert (await conn.execute(text("SHOW TIME ZONE"))).scalar_one() == "Asia/Tokyo"
    storage = SQLAlchemyStorage(engine)
    try:
        async with storage.engine.connect() as conn:
            assert (await conn.execute(text("SHOW TIME ZONE"))).scalar_one() == "UTC"
        await _assert_fresh_lease_is_utc_and_kept(storage)
    finally:
        await storage.close()


async def test_mysql_connection_opened_before_the_storage_is_pinned(multidb_mysql_url):
    url = await _create_database(multidb_mysql_url)
    admin = create_async_engine(multidb_mysql_url, poolclass=NullPool, isolation_level="AUTOCOMMIT")
    async with admin.connect() as conn:
        original = (await conn.execute(text("SELECT @@global.time_zone"))).scalar_one()
        await conn.execute(text("SET GLOBAL time_zone = '+09:00'"))
    try:
        engine = create_async_engine(url, pool_size=1, max_overflow=4)
        async with engine.connect() as conn:
            zone = (await conn.execute(text("SELECT @@session.time_zone"))).scalar_one()
            assert zone == "+09:00"
        storage = SQLAlchemyStorage(engine)
        try:
            async with storage.engine.connect() as conn:
                zone = (await conn.execute(text("SELECT @@session.time_zone"))).scalar_one()
                isolation = await _isolation(conn)
            assert zone == "+00:00"
            assert isolation == "READ-COMMITTED"
            await _assert_fresh_lease_is_utc_and_kept(storage)
        finally:
            await storage.close()
    finally:
        async with admin.connect() as conn:
            await conn.execute(text("SET GLOBAL time_zone = :z"), {"z": original})
        await admin.dispose()
