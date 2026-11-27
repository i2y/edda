"""
SQLAlchemy storage implementation for Edda framework.

This module provides a SQLAlchemy-based implementation of the StorageProtocol,
supporting SQLite, PostgreSQL, and MySQL with database-based exclusive control
and transactional outbox pattern.
"""

import json
import logging
import re
import sqlite3
import uuid
from collections.abc import AsyncIterator, Awaitable, Callable, Sequence
from contextlib import asynccontextmanager
from contextvars import ContextVar
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import Any

from sqlalchemy import (
    CheckConstraint,
    Column,
    DateTime,
    ForeignKey,
    ForeignKeyConstraint,
    Index,
    Integer,
    LargeBinary,
    String,
    Text,
    UniqueConstraint,
    and_,
    delete,
    event,
    func,
    inspect,
    or_,
    select,
    text,
    update,
)
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

logger = logging.getLogger(__name__)

# How old a channel-message claim must be before another competing consumer
# may take it over. Aligned with the workflow lock lease (300s).
CHANNEL_CLAIM_STALE_SECONDS = 300

# How old an outbox 'processing' claim must be before another relayer may
# reclaim it. Aligned with the workflow lock lease (300s).
OUTBOX_CLAIM_STALE_SECONDS = 300

# How old an actor_tasks 'claimed' row must be before the leader sweep
# returns it to 'open' (orphaned by a crashed claimant).
TASK_CLAIM_STALE_SECONDS = 300

# Terminal workflow statuses. They are absorbing: no fenced write moves an
# instance out of them.
_TERMINAL_STATUSES = ("completed", "failed", "cancelled")


# Key in a pooled connection's info: the session settings are applied.
_SESSION_PINNED = "edda_session_pinned"


def _pin_postgresql_session(dbapi_connection: Any, record: Any) -> None:
    # A SET inside a transaction that is later rolled back is undone, so it
    # runs in autocommit (SQLAlchemy's search_path recipe).
    previous = dbapi_connection.autocommit
    dbapi_connection.autocommit = True
    cursor = dbapi_connection.cursor()
    try:
        cursor.execute("SET TIME ZONE 'UTC'")
    finally:
        cursor.close()
        dbapi_connection.autocommit = previous
    record.info[_SESSION_PINNED] = True


def _pin_mysql_time_zone(dbapi_connection: Any, record: Any) -> None:
    cursor = dbapi_connection.cursor()
    try:
        cursor.execute("SET time_zone = '+00:00'")
    finally:
        cursor.close()
    record.info[_SESSION_PINNED] = True


def _pin_mysql_session(dbapi_connection: Any, record: Any) -> None:
    _pin_mysql_time_zone(dbapi_connection, record)
    cursor = dbapi_connection.cursor()
    try:
        cursor.execute("SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED")
    finally:
        cursor.close()


_CHECKOUT_PINS: dict[Callable[[Any, Any], None], Callable[[Any, Any, Any], None]] = {}


def _pin_at_checkout(listener: Callable[[Any, Any], None]) -> Callable[[Any, Any, Any], None]:
    """The checkout listener that applies ``listener`` to a pooled connection
    it has not run on: one the engine opened before the storage existed."""
    pin = _CHECKOUT_PINS.get(listener)
    if pin is None:

        def pin(dbapi_connection: Any, record: Any, _proxy: Any) -> None:
            if not record.info.get(_SESSION_PINNED):
                listener(dbapi_connection, record)

        _CHECKOUT_PINS[listener] = pin
    return pin


def is_write_conflict(error: BaseException | None) -> bool:
    """Whether the error is SQLite's SQLITE_BUSY / SQLITE_BUSY_SNAPSHOT.

    A transaction that read before it wrote fails at once with it when another
    connection wrote in between, and any write fails with it once
    busy_timeout is spent. The attempt can simply run again.
    """
    seen: set[int] = set()
    while error is not None and id(error) not in seen:
        seen.add(id(error))
        if isinstance(error, sqlite3.OperationalError):
            code = getattr(error, "sqlite_errorcode", None)
            if code is not None:
                return bool((code & 0xFF) == sqlite3.SQLITE_BUSY)
            return "database is locked" in str(error)
        error = getattr(error, "orig", None) or error.__cause__ or error.__context__
    return False


def _use_sqlite_wal(dbapi_connection: Any, record: Any) -> None:
    cursor = dbapi_connection.cursor()
    try:
        # busy_timeout first: switching the journal mode needs a lock.
        cursor.execute("PRAGMA busy_timeout = 5000")
        cursor.execute("PRAGMA journal_mode = WAL")
    finally:
        cursor.close()
    record.info[_SESSION_PINNED] = True


def _is_sqlite_file_database(engine: AsyncEngine) -> bool:
    url = engine.url
    in_memory = (
        url.database in (None, "", ":memory:")
        or url.query.get("mode") == "memory"
        or "mode=memory" in (url.database or "")
    )
    return url.get_backend_name() == "sqlite" and not in_memory


def _install_session_settings(engine: AsyncEngine) -> None:
    """
    Pin every new connection of the engine to UTC and, on MySQL, to READ
    COMMITTED -- also for an engine the caller built. File-backed SQLite
    databases run in WAL mode (readers and a writer no longer block each
    other); in-memory ones keep their default.

    The schema stores TIMESTAMP without a zone and compares it with NOW(),
    and edda writes UTC (deadlines, timer expiry, leases): in any other
    session zone a fresh lease looks expired, or timers fire hours early or
    late. The kernel's protocols assume READ COMMITTED, not MySQL's default
    REPEATABLE READ (stale snapshots, gap locks). An engine created with an
    explicit ``isolation_level`` keeps it. A connection the engine opened
    before the storage was created (the caller ran migrations or its own
    queries first) gets the settings when it is first checked out.
    """
    backend = engine.dialect.name
    if backend == "postgresql":
        listener = _pin_postgresql_session
    elif backend == "mysql":
        explicit = getattr(engine.dialect, "_on_connect_isolation_level", None)
        listener = _pin_mysql_time_zone if explicit else _pin_mysql_session
    elif _is_sqlite_file_database(engine):
        listener = _use_sqlite_wal
    else:
        return
    target = engine.sync_engine
    if not event.contains(target, "connect", listener):
        # First, so that an engine-level isolation_level, which SQLAlchemy
        # applies in its own connect handler, still wins.
        event.listen(target, "connect", listener, insert=True)
    checkout = _pin_at_checkout(listener)
    if not event.contains(target, "checkout", checkout):
        event.listen(target, "checkout", checkout)


def _like_escape(s: str) -> str:
    """Escape LIKE wildcards (and the escape char itself) for ESCAPE '\\'."""
    return s.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")


# Declarative base for ORM models
class Base(DeclarativeBase):
    pass


# ============================================================================
# SQLAlchemy ORM Models
# ============================================================================


def _canonical_ts(dt: datetime) -> str:
    """Fixed-width UTC timestamp, lexically comparable across dialects."""
    return dt.astimezone(UTC).strftime("%Y-%m-%d %H:%M:%S")


def _normalize_expiry(expires_at: str | None) -> datetime | None:
    """Parse a caller-supplied expiry (ISO 8601; naive means UTC) as aware UTC."""
    if expires_at is None:
        return None
    dt = datetime.fromisoformat(str(expires_at).replace("Z", "+00:00"))
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)
    return dt.astimezone(UTC)


class SchemaVersion(Base):
    """Schema version tracking."""

    __tablename__ = "schema_version"

    version: Mapped[int] = mapped_column(Integer, primary_key=True)
    applied_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    description: Mapped[str] = mapped_column(Text)


class WorkflowDefinition(Base):
    """Workflow definition (source code storage)."""

    __tablename__ = "workflow_definitions"

    workflow_name: Mapped[str] = mapped_column(String(255), primary_key=True)
    source_hash: Mapped[str] = mapped_column(String(64), primary_key=True)
    source_code: Mapped[str] = mapped_column(Text)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())

    __table_args__ = (
        Index("idx_definitions_name", "workflow_name"),
        Index("idx_definitions_hash", "source_hash"),
    )


class WorkflowInstance(Base):
    """Workflow instance with distributed locking support."""

    __tablename__ = "workflow_instances"

    instance_id: Mapped[str] = mapped_column(String(255), primary_key=True)
    workflow_name: Mapped[str] = mapped_column(String(255))
    source_hash: Mapped[str] = mapped_column(String(64))
    owner_service: Mapped[str] = mapped_column(String(255))
    framework: Mapped[str] = mapped_column(String(50), server_default=text("'python'"))
    status: Mapped[str] = mapped_column(String(50), server_default=text("'running'"))
    current_activity_id: Mapped[str | None] = mapped_column(String(255), nullable=True)
    continued_from: Mapped[str | None] = mapped_column(
        String(255), ForeignKey("workflow_instances.instance_id"), nullable=True
    )
    sandboxed: Mapped[int] = mapped_column(Integer, nullable=False, server_default=text("0"))
    started_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now()
    )
    input_data: Mapped[str] = mapped_column(Text)  # JSON
    output_data: Mapped[str | None] = mapped_column(Text, nullable=True)  # JSON
    locked_by: Mapped[str | None] = mapped_column(String(255), nullable=True)
    locked_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    lock_timeout_seconds: Mapped[int | None] = mapped_column(
        Integer, nullable=True
    )  # None = use global default (300s)
    lock_expires_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )  # Absolute expiry time

    __table_args__ = (
        ForeignKeyConstraint(
            ["workflow_name", "source_hash"],
            ["workflow_definitions.workflow_name", "workflow_definitions.source_hash"],
        ),
        CheckConstraint(
            "status IN ('running', 'completed', 'failed', 'waiting_for_event', "
            "'waiting_for_timer', 'waiting_for_message', 'compensating', 'cancelled', 'recurred')",
            name="valid_status",
        ),
        Index("idx_instances_status", "status"),
        Index("idx_instances_workflow", "workflow_name"),
        Index("idx_instances_owner", "owner_service"),
        Index("idx_instances_framework", "framework"),
        Index("idx_instances_locked", "locked_by", "locked_at"),
        Index("idx_instances_lock_expires", "lock_expires_at"),
        Index("idx_instances_updated", "updated_at"),
        Index("idx_instances_hash", "source_hash"),
        Index("idx_instances_continued_from", "continued_from"),
        # Composite index for find_resumable_workflows(): WHERE status='running' AND locked_by IS NULL
        Index("idx_instances_resumable", "status", "locked_by"),
    )


class WorkflowHistory(Base):
    """Workflow execution history (for deterministic replay)."""

    __tablename__ = "workflow_history"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    instance_id: Mapped[str] = mapped_column(String(255))
    activity_id: Mapped[str] = mapped_column(String(255))
    event_type: Mapped[str] = mapped_column(String(100))
    data_type: Mapped[str] = mapped_column(String(10))  # 'json' or 'binary'
    event_data: Mapped[str | None] = mapped_column(
        Text, nullable=True
    )  # JSON (when data_type='json')
    event_data_binary: Mapped[bytes | None] = mapped_column(
        LargeBinary, nullable=True
    )  # Binary (when data_type='binary')
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())

    __table_args__ = (
        ForeignKeyConstraint(
            ["instance_id"],
            ["workflow_instances.instance_id"],
            ondelete="CASCADE",
        ),
        CheckConstraint(
            "data_type IN ('json', 'binary')",
            name="valid_data_type",
        ),
        CheckConstraint(
            "(data_type = 'json' AND event_data IS NOT NULL AND event_data_binary IS NULL) OR "
            "(data_type = 'binary' AND event_data IS NULL AND event_data_binary IS NOT NULL)",
            name="data_type_consistency",
        ),
        UniqueConstraint("instance_id", "activity_id", name="unique_instance_activity"),
        Index("idx_history_instance", "instance_id", "activity_id"),
        Index("idx_history_created", "created_at"),
    )


class WorkflowHistoryArchive(Base):
    """Archived workflow execution history (for recur pattern)."""

    __tablename__ = "workflow_history_archive"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    instance_id: Mapped[str] = mapped_column(String(255))
    activity_id: Mapped[str] = mapped_column(String(255))
    event_type: Mapped[str] = mapped_column(String(100))
    event_data: Mapped[str] = mapped_column(Text)  # JSON (includes both types for archive)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    archived_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )

    __table_args__ = (
        ForeignKeyConstraint(
            ["instance_id"],
            ["workflow_instances.instance_id"],
            ondelete="CASCADE",
        ),
        Index("idx_history_archive_instance", "instance_id"),
        Index("idx_history_archive_archived", "archived_at"),
    )


class WorkflowCompensation(Base):
    """Compensation transactions (LIFO stack for Saga pattern)."""

    __tablename__ = "workflow_compensations"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    instance_id: Mapped[str] = mapped_column(String(255))
    activity_id: Mapped[str] = mapped_column(String(255))
    activity_name: Mapped[str] = mapped_column(String(255))
    args: Mapped[str] = mapped_column(Text)  # JSON
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())

    __table_args__ = (
        ForeignKeyConstraint(
            ["instance_id"],
            ["workflow_instances.instance_id"],
            ondelete="CASCADE",
        ),
        Index("idx_compensations_instance", "instance_id", "created_at"),
    )


class WorkflowTimerSubscription(Base):
    """Timer subscriptions (for wait_timer)."""

    __tablename__ = "workflow_timer_subscriptions"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    instance_id: Mapped[str] = mapped_column(String(255))
    timer_id: Mapped[str] = mapped_column(String(255))
    expires_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    activity_id: Mapped[str | None] = mapped_column(String(255), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())

    __table_args__ = (
        ForeignKeyConstraint(
            ["instance_id"],
            ["workflow_instances.instance_id"],
            ondelete="CASCADE",
        ),
        UniqueConstraint("instance_id", "timer_id", name="unique_instance_timer"),
        Index("idx_timer_subscriptions_expires", "expires_at"),
        Index("idx_timer_subscriptions_instance", "instance_id"),
    )


class OutboxEvent(Base):
    """Transactional outbox pattern events."""

    __tablename__ = "outbox_events"

    event_id: Mapped[str] = mapped_column(String(255), primary_key=True)
    event_type: Mapped[str] = mapped_column(String(255))
    event_source: Mapped[str] = mapped_column(String(255))
    data_type: Mapped[str] = mapped_column(String(10))  # 'json' or 'binary'
    event_data: Mapped[str | None] = mapped_column(
        Text, nullable=True
    )  # JSON (when data_type='json')
    event_data_binary: Mapped[bytes | None] = mapped_column(
        LargeBinary, nullable=True
    )  # Binary (when data_type='binary')
    content_type: Mapped[str] = mapped_column(
        String(100), server_default=text("'application/json'")
    )
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    published_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    claimed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    status: Mapped[str] = mapped_column(String(50), server_default=text("'pending'"))
    retry_count: Mapped[int] = mapped_column(Integer, server_default=text("0"))
    last_error: Mapped[str | None] = mapped_column(Text, nullable=True)

    __table_args__ = (
        CheckConstraint(
            "status IN ('pending', 'processing', 'published', 'failed', 'invalid', 'expired')",
            name="valid_outbox_status",
        ),
        CheckConstraint(
            "data_type IN ('json', 'binary')",
            name="valid_outbox_data_type",
        ),
        CheckConstraint(
            "(data_type = 'json' AND event_data IS NOT NULL AND event_data_binary IS NULL) OR "
            "(data_type = 'binary' AND event_data IS NULL AND event_data_binary IS NOT NULL)",
            name="outbox_data_type_consistency",
        ),
        Index("idx_outbox_status", "status", "created_at"),
        Index("idx_outbox_retry", "status", "retry_count"),
        Index("idx_outbox_published", "published_at"),
    )


class WorkflowGroupMembership(Base):
    """Group memberships (Erlang pg style for broadcast messaging)."""

    __tablename__ = "workflow_group_memberships"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    instance_id: Mapped[str] = mapped_column(String(255))
    group_name: Mapped[str] = mapped_column(String(255))
    joined_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())

    __table_args__ = (
        ForeignKeyConstraint(
            ["instance_id"],
            ["workflow_instances.instance_id"],
            ondelete="CASCADE",
        ),
        UniqueConstraint("instance_id", "group_name", name="unique_instance_group"),
        Index("idx_group_memberships_group", "group_name"),
        Index("idx_group_memberships_instance", "instance_id"),
    )


# =============================================================================
# Channel-based Message Queue Models
# =============================================================================


class ChannelMessage(Base):
    """Channel message queue (persistent message storage)."""

    __tablename__ = "channel_messages"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    channel: Mapped[str] = mapped_column(String(255))
    message_id: Mapped[str] = mapped_column(String(255), unique=True)
    data_type: Mapped[str] = mapped_column(String(10))  # 'json' or 'binary'
    data: Mapped[str | None] = mapped_column(Text, nullable=True)  # JSON (when data_type='json')
    data_binary: Mapped[bytes | None] = mapped_column(
        LargeBinary, nullable=True
    )  # Binary (when data_type='binary')
    message_metadata: Mapped[str | None] = mapped_column(
        "metadata", Text, nullable=True
    )  # JSON - renamed to avoid SQLAlchemy reserved name
    published_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )

    __table_args__ = (
        CheckConstraint(
            "data_type IN ('json', 'binary')",
            name="channel_valid_data_type",
        ),
        CheckConstraint(
            "(data_type = 'json' AND data IS NOT NULL AND data_binary IS NULL) OR "
            "(data_type = 'binary' AND data IS NULL AND data_binary IS NOT NULL)",
            name="channel_data_type_consistency",
        ),
        Index("idx_channel_messages_channel", "channel", "published_at"),
        Index("idx_channel_messages_id", "id"),
    )


class ChannelSubscription(Base):
    """Channel subscriptions for workflow instances."""

    __tablename__ = "channel_subscriptions"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    instance_id: Mapped[str] = mapped_column(String(255))
    channel: Mapped[str] = mapped_column(String(255))
    mode: Mapped[str] = mapped_column(String(20))  # 'broadcast' or 'competing'
    activity_id: Mapped[str | None] = mapped_column(
        String(255), nullable=True
    )  # Set when waiting for message
    cursor_message_id: Mapped[int | None] = mapped_column(
        Integer, nullable=True
    )  # Last received message id (broadcast)
    timeout_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )  # Timeout deadline
    subscribed_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )

    __table_args__ = (
        ForeignKeyConstraint(
            ["instance_id"],
            ["workflow_instances.instance_id"],
            ondelete="CASCADE",
        ),
        CheckConstraint(
            "mode IN ('broadcast', 'competing')",
            name="channel_valid_mode",
        ),
        UniqueConstraint("instance_id", "channel", name="unique_channel_instance_channel"),
        Index("idx_channel_subscriptions_channel", "channel"),
        Index("idx_channel_subscriptions_instance", "instance_id"),
        Index("idx_channel_subscriptions_waiting", "channel", "activity_id"),
        Index("idx_channel_subscriptions_timeout", "timeout_at"),
    )


class ChannelDeliveryCursor(Base):
    """Channel delivery cursors (broadcast mode: track who read what)."""

    __tablename__ = "channel_delivery_cursors"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    channel: Mapped[str] = mapped_column(String(255))
    instance_id: Mapped[str] = mapped_column(String(255))
    last_delivered_id: Mapped[int] = mapped_column(Integer)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())

    __table_args__ = (
        ForeignKeyConstraint(
            ["instance_id"],
            ["workflow_instances.instance_id"],
            ondelete="CASCADE",
        ),
        UniqueConstraint("channel", "instance_id", name="unique_channel_delivery_cursor"),
        Index("idx_channel_delivery_cursors_channel", "channel"),
    )


class ChannelMessageClaim(Base):
    """Channel message claims (competing mode: who is processing what)."""

    __tablename__ = "channel_message_claims"

    message_id: Mapped[str] = mapped_column(String(255), primary_key=True)
    instance_id: Mapped[str] = mapped_column(String(255))
    claimed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())

    __table_args__ = (
        ForeignKeyConstraint(
            ["message_id"],
            ["channel_messages.message_id"],
            ondelete="CASCADE",
        ),
        ForeignKeyConstraint(
            ["instance_id"],
            ["workflow_instances.instance_id"],
            ondelete="CASCADE",
        ),
        Index("idx_channel_message_claims_instance", "instance_id"),
    )


# =============================================================================
# System-level Lock Models (for background task coordination)
# =============================================================================


class SystemLock(Base):
    """System-level locks for coordinating background tasks across pods.

    Used to prevent duplicate execution of operational tasks like:
    - cleanup_stale_locks_periodically()
    - auto_resume_stale_workflows_periodically()
    - _cleanup_old_messages_periodically()
    """

    __tablename__ = "system_locks"

    lock_name: Mapped[str] = mapped_column(String(255), primary_key=True)
    locked_by: Mapped[str | None] = mapped_column(String(255), nullable=True)
    locked_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    lock_expires_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    __table_args__ = (Index("idx_system_locks_expires", "lock_expires_at"),)


# Current schema version
CURRENT_SCHEMA_VERSION = 1


# ============================================================================
# Transaction Context
# ============================================================================


@dataclass
class TransactionContext:
    """
    Transaction context for managing nested transactions.

    Uses savepoints for nested transaction support across all databases.
    """

    depth: int = 0
    """Current transaction depth (0 = not in transaction, 1+ = in transaction)"""

    savepoint_stack: list[Any] = field(default_factory=list)
    """Stack of nested transaction objects for savepoint support"""

    session: "AsyncSession | None" = None
    """The actual session for this transaction"""

    post_commit_callbacks: list[Callable[[], Awaitable[None]]] = field(default_factory=list)
    """Callbacks to execute after successful top-level commit"""


# Context variable for transaction state (asyncio-safe)
_transaction_context: ContextVar[TransactionContext | None] = ContextVar(
    "_transaction_context", default=None
)


# ============================================================================
# SQLAlchemyStorage
# ============================================================================


class SQLAlchemyStorage:
    """
    SQLAlchemy implementation of StorageProtocol.

    Supports SQLite, PostgreSQL, and MySQL with database-based exclusive control
    and transactional outbox pattern.

    Transaction Architecture:
    - Lock operations: Always use separate session (isolated transactions)
    - History/outbox operations: Use transaction context session when available
    - Automatic transaction management via @activity decorator
    """

    def __init__(
        self,
        engine: AsyncEngine,
        notify_listener: Any | None = None,
        migrations_dir: str | None = None,
    ):
        """
        Initialize SQLAlchemy storage.

        Args:
            engine: SQLAlchemy AsyncEngine instance
            notify_listener: Optional notify listener for PostgreSQL LISTEN/NOTIFY.
                            If provided and PostgreSQL is used, NOTIFY messages
                            will be sent after key operations.
            migrations_dir: Optional path to migrations directory. If None,
                           auto-detects from package or schema/ submodule.
        """
        self.engine = engine
        self._notify_listener = notify_listener
        self._migrations_dir = migrations_dir
        _install_session_settings(engine)

    @property
    def _is_postgresql(self) -> bool:
        """Check if the database is PostgreSQL."""
        return self.engine.dialect.name == "postgresql"

    def _ts_param(self, dt: datetime | None = None, *, tz: bool = False) -> Any:
        """Bind value for a timestamp parameter of a raw text() statement (UTC).

        text() binds carry no column type, so each driver needs its own form:
        SQLite gets the canonical text of its datetime('now') defaults,
        asyncpg a naive UTC datetime for TIMESTAMP and an aware one for
        TIMESTAMPTZ (tz=True; only actor_tasks.claimed_at), MySQL a naive UTC
        datetime. dt defaults to now; a naive dt is taken as UTC.
        """
        if dt is None:
            dt = datetime.now(UTC)
        elif dt.tzinfo is None:
            dt = dt.replace(tzinfo=UTC)
        else:
            dt = dt.astimezone(UTC)
        if self.engine.dialect.name == "sqlite":
            return _canonical_ts(dt)
        if tz and self._is_postgresql:
            return dt
        return dt.replace(tzinfo=None)

    async def _insert_unless_exists(
        self, session: AsyncSession, sql: str, conflict: str, params: dict[str, Any]
    ) -> bool:
        """Run a one-row INSERT; return False when it hits the unique key.

        PostgreSQL and SQLite skip the row with ON CONFLICT DO NOTHING: a
        unique violation would abort an enclosing PostgreSQL transaction.
        MySQL keeps the transaction usable after a duplicate-key error, so it
        is caught there (INSERT IGNORE would also hide truncation errors).
        """
        if self.engine.dialect.name == "mysql":
            try:
                await session.execute(text(sql), params)
            except IntegrityError as exc:
                if getattr(exc.orig, "args", (None,))[0] != 1062:  # ER_DUP_ENTRY
                    raise
                return False
            return True
        result = await session.execute(text(f"{sql} ON CONFLICT ({conflict}) DO NOTHING"), params)
        return bool(result.rowcount)  # type: ignore[attr-defined]

    @property
    def _notify_enabled(self) -> bool:
        """Check if NOTIFY is enabled (PostgreSQL with listener)."""
        return self._is_postgresql and self._notify_listener is not None

    def set_notify_listener(self, listener: Any) -> None:
        """Set the notify listener after initialization.

        This allows setting the listener after EddaApp creates the storage,
        useful for dependency injection patterns.

        Args:
            listener: NotifyProtocol implementation (PostgresNotifyListener or NoopNotifyListener)
        """
        self._notify_listener = listener

    async def initialize(self) -> None:
        """Initialize database connection and apply migrations.

        This method automatically applies dbmate migration files to create
        tables and update schema. It tracks applied migrations in the
        schema_migrations table (compatible with dbmate CLI).
        """
        from pathlib import Path

        from .migrations import apply_dbmate_migrations

        # Apply dbmate migrations
        migrations_dir = Path(self._migrations_dir) if self._migrations_dir else None
        await apply_dbmate_migrations(self.engine, migrations_dir)

        # Auto-migrate CHECK constraints (for existing tables)
        await self._auto_migrate_check_constraints()

        # Initialize schema version
        await self._initialize_schema_version()

    async def close(self) -> None:
        """Close database connection."""
        await self.engine.dispose()

    async def _send_notify(
        self,
        channel: str,
        payload: dict[str, Any],
    ) -> None:
        """Send PostgreSQL NOTIFY message.

        This method sends a notification on the specified channel with the given
        payload. It's a no-op if NOTIFY is not enabled (non-PostgreSQL or no listener).

        Args:
            channel: PostgreSQL NOTIFY channel name (max 63 chars).
            payload: Dictionary to serialize as JSON payload (max ~7500 bytes).
        """
        if not self._notify_enabled:
            return

        if self.in_transaction():
            # Notify once the data is visible (and not at all on rollback):
            # a listener woken before the commit would miss it.
            async def notify_after_commit() -> None:
                await self._send_notify(channel, payload)

            self.register_post_commit_callback(notify_after_commit)
            return

        try:
            import json as json_module

            payload_str = json_module.dumps(payload, separators=(",", ":"))

            # Use a separate connection for NOTIFY to avoid transaction issues
            async with self.engine.connect() as conn:
                await conn.execute(
                    text("SELECT pg_notify(:channel, :payload)"),
                    {"channel": channel, "payload": payload_str},
                )
                await conn.commit()
        except Exception as e:
            # Log but don't fail - polling will catch it as backup
            logger.warning(f"Failed to send NOTIFY on channel {channel}: {e}")

    async def _initialize_schema_version(self) -> None:
        """Initialize schema version for a fresh database."""
        async with AsyncSession(self.engine) as session:
            # Check if schema_version table is empty
            result = await session.execute(select(func.count()).select_from(SchemaVersion))
            count = result.scalar()

            # If empty, insert current version
            if count == 0:
                version = SchemaVersion(
                    version=CURRENT_SCHEMA_VERSION,
                    description="Initial schema with workflow_definitions",
                )
                session.add(version)
                await session.commit()
                logger.info(f"Initialized schema version to {CURRENT_SCHEMA_VERSION}")

    async def _auto_migrate_schema(self) -> None:
        """
        Automatically add missing columns to existing tables.

        This method compares the ORM model definitions with the actual database
        schema and adds any missing columns using ALTER TABLE ADD COLUMN.

        Note: This only handles column additions, not removals or type changes.
        For complex migrations, use Alembic.
        """

        def _get_column_type_sql(column: Column, dialect_name: str) -> str:  # type: ignore[type-arg]
            """Get SQL type string for a column based on dialect."""
            col_type = column.type

            # Map SQLAlchemy types to SQL types
            if isinstance(col_type, String):
                length = col_type.length or 255
                return f"VARCHAR({length})"
            elif isinstance(col_type, Text):
                return "TEXT"
            elif isinstance(col_type, Integer):
                return "INTEGER"
            elif isinstance(col_type, DateTime):
                if dialect_name == "postgresql":
                    return "TIMESTAMP WITH TIME ZONE" if col_type.timezone else "TIMESTAMP"
                elif dialect_name == "mysql":
                    return "DATETIME" if not col_type.timezone else "DATETIME"
                else:  # sqlite
                    return "DATETIME"
            elif isinstance(col_type, LargeBinary):
                if dialect_name == "postgresql":
                    return "BYTEA"
                elif dialect_name == "mysql":
                    return "LONGBLOB"
                else:  # sqlite
                    return "BLOB"
            else:
                # Fallback to compiled type
                return str(col_type.compile(dialect=self.engine.dialect))

        def _get_default_sql(column: Column, _dialect_name: str) -> str | None:  # type: ignore[type-arg]
            """Get DEFAULT clause for a column if applicable."""
            if column.server_default is not None:
                # Handle text() server defaults - try to get the arg attribute
                server_default = column.server_default
                if hasattr(server_default, "arg"):
                    default_val = server_default.arg
                    if hasattr(default_val, "text"):
                        return f"DEFAULT {default_val.text}"
                    return f"DEFAULT {default_val}"
            return None

        def _run_migration(conn: Any) -> None:
            """Run migration in sync context."""
            dialect_name = self.engine.dialect.name
            inspector = inspect(conn)

            # Iterate through all ORM tables
            for table in Base.metadata.tables.values():
                table_name = table.name

                # Check if table exists
                if not inspector.has_table(table_name):
                    logger.debug(f"Table {table_name} does not exist, skipping migration")
                    continue

                # Get existing columns
                existing_columns = {col["name"] for col in inspector.get_columns(table_name)}

                # Check each column in the ORM model
                for column in table.columns:
                    if column.name not in existing_columns:
                        # Column is missing, generate ALTER TABLE
                        col_type_sql = _get_column_type_sql(column, dialect_name)
                        nullable = "NULL" if column.nullable else "NOT NULL"

                        # Build ALTER TABLE statement
                        alter_sql = (
                            f'ALTER TABLE "{table_name}" ADD COLUMN "{column.name}" {col_type_sql}'
                        )

                        # Add nullable constraint (only if NOT NULL and has default)
                        default_sql = _get_default_sql(column, dialect_name)
                        if not column.nullable and default_sql:
                            alter_sql += f" {default_sql} {nullable}"
                        elif column.nullable:
                            alter_sql += f" {nullable}"
                        elif default_sql:
                            alter_sql += f" {default_sql}"
                        # For NOT NULL without default, just add the column as nullable
                        # (PostgreSQL requires default or nullable for existing rows)
                        else:
                            alter_sql += " NULL"

                        logger.info(f"Auto-migrating: Adding column {column.name} to {table_name}")
                        logger.debug(f"Executing: {alter_sql}")

                        try:
                            conn.execute(text(alter_sql))
                        except Exception as e:
                            logger.warning(
                                f"Failed to add column {column.name} to {table_name}: {e}"
                            )

        async with self.engine.begin() as conn:
            await conn.run_sync(_run_migration)

    async def _auto_migrate_check_constraints(self) -> None:
        """
        Automatically update CHECK constraints for workflow status.

        This method ensures the valid_status CHECK constraint includes all
        required status values (including 'waiting_for_message').
        """
        dialect_name = self.engine.dialect.name

        # SQLite doesn't support ALTER CONSTRAINT easily, and SQLAlchemy create_all
        # handles it correctly for new databases. For existing SQLite databases,
        # the constraint is more lenient (CHECK is not enforced in many SQLite versions).
        if dialect_name == "sqlite":
            return

        # Expected status values (must match WorkflowInstance model)
        expected_statuses = (
            "'running', 'completed', 'failed', 'waiting_for_event', "
            "'waiting_for_timer', 'waiting_for_message', 'compensating', 'cancelled', 'recurred'"
        )

        def _run_constraint_migration(conn: Any) -> None:
            """Run CHECK constraint migration in sync context."""
            inspector = inspect(conn)

            # Check if workflow_instances table exists
            if not inspector.has_table("workflow_instances"):
                return

            # Get existing CHECK constraints
            try:
                constraints = inspector.get_check_constraints("workflow_instances")
            except NotImplementedError:
                # Some databases don't support get_check_constraints
                logger.debug("Database doesn't support get_check_constraints inspection")
                constraints = []

            # Find the valid_status constraint
            valid_status_constraint = None
            for constraint in constraints:
                if constraint.get("name") == "valid_status":
                    valid_status_constraint = constraint
                    break

            # Check if constraint exists and needs updating
            if valid_status_constraint:
                sqltext = valid_status_constraint.get("sqltext", "")
                # Check if 'waiting_for_message' is already in the constraint
                if "waiting_for_message" in sqltext:
                    logger.debug("valid_status constraint already includes waiting_for_message")
                    return

                # Need to update the constraint
                logger.info("Updating valid_status CHECK constraint to include waiting_for_message")
                try:
                    if dialect_name == "postgresql":
                        conn.execute(
                            text("ALTER TABLE workflow_instances DROP CONSTRAINT valid_status")
                        )
                        conn.execute(
                            text(
                                f"ALTER TABLE workflow_instances ADD CONSTRAINT valid_status "
                                f"CHECK (status IN ({expected_statuses}))"
                            )
                        )
                    elif dialect_name == "mysql":
                        # MySQL uses DROP CHECK and ADD CONSTRAINT CHECK syntax
                        conn.execute(text("ALTER TABLE workflow_instances DROP CHECK valid_status"))
                        conn.execute(
                            text(
                                f"ALTER TABLE workflow_instances ADD CONSTRAINT valid_status "
                                f"CHECK (status IN ({expected_statuses}))"
                            )
                        )
                    logger.info("Successfully updated valid_status CHECK constraint")
                except Exception as e:
                    logger.warning(f"Failed to update valid_status CHECK constraint: {e}")
            else:
                # Constraint doesn't exist (shouldn't happen with create_all, but handle it)
                logger.debug("valid_status constraint not found, will be created by create_all")

        async with self.engine.begin() as conn:
            await conn.run_sync(_run_constraint_migration)

    def _get_session_for_operation(self, is_lock_operation: bool = False) -> AsyncSession:
        """
        Get the appropriate session for an operation.

        Lock operations ALWAYS use a new session (separate transactions).
        Other operations prefer: transaction session > new session.

        Args:
            is_lock_operation: True if this is a lock acquisition/release operation

        Returns:
            AsyncSession to use for the operation
        """
        if is_lock_operation:
            # Lock operations always use new session
            return AsyncSession(self.engine, expire_on_commit=False)

        # Check for transaction context session
        ctx = _transaction_context.get()
        if ctx is not None and ctx.session is not None:
            return ctx.session

        # Otherwise create new session
        return AsyncSession(self.engine, expire_on_commit=False)

    def _is_managed_session(self, session: AsyncSession) -> bool:
        """Check if session is managed by transaction context."""
        ctx = _transaction_context.get()
        return ctx is not None and ctx.session == session

    @asynccontextmanager
    async def _session_scope(self, session: AsyncSession) -> AsyncIterator[AsyncSession]:
        """
        Context manager for session usage.

        If session is managed (transaction context), use it directly without closing.
        If session is new, manage its lifecycle (commit/rollback/close).
        """
        if self._is_managed_session(session):
            # Managed session: yield without lifecycle management
            yield session
        else:
            # New session: full lifecycle management
            try:
                yield session
                await self._commit_if_not_in_transaction(session)
            except Exception:
                await session.rollback()
                raise
            finally:
                await session.close()

    def _get_current_time_expr(self) -> Any:
        """
        Get database-specific current time SQL expression.

        Returns:
            SQLAlchemy function for current time in SQL queries.
            - SQLite: datetime('now') - returns UTC datetime string
            - PostgreSQL/MySQL: NOW() - returns timezone-aware datetime

        This method enables cross-database datetime comparisons in SQL queries.
        """
        if self.engine.dialect.name == "sqlite":
            # SQLite: datetime('now') returns UTC datetime as string
            return func.datetime("now")
        else:
            # PostgreSQL/MySQL: NOW() returns timezone-aware datetime
            return func.now()

    def _make_datetime_comparable(self, column: Any) -> Any:
        """
        Make datetime column comparable with current time in SQL queries.

        For SQLite, wraps column in datetime() function to ensure proper comparison.
        For PostgreSQL/MySQL, returns column as-is (already timezone-aware).

        Args:
            column: SQLAlchemy Column expression representing a datetime field

        Returns:
            SQLAlchemy expression suitable for datetime comparison

        Example:
            >>> # SQLite: datetime(timeout_at) <= datetime('now')
            >>> # PostgreSQL/MySQL: timeout_at <= NOW()
            >>> self._make_datetime_comparable(ChannelSubscription.timeout_at)
            >>>     <= self._get_current_time_expr()
        """
        if self.engine.dialect.name == "sqlite":
            # SQLite: wrap in datetime() for proper comparison
            return func.datetime(column)
        else:
            # PostgreSQL/MySQL: column is already timezone-aware
            return column

    def _validate_json_path(self, json_path: str) -> bool:
        """
        Validate JSON path to prevent SQL injection.

        Args:
            json_path: JSON path string (e.g., "order_id" or "customer.email")

        Returns:
            True if valid, False otherwise
        """
        # Only allow alphanumeric characters, dots, and underscores
        # Must start with letter or underscore
        return bool(re.match(r"^[a-zA-Z_][a-zA-Z0-9_.]*$", json_path))

    def _build_json_extract_expr(self, column: Any, json_path: str) -> Any:
        """
        Build a database-agnostic JSON extraction expression.

        Args:
            column: SQLAlchemy column containing JSON text
            json_path: Dot-notation path (e.g., "order_id" or "customer.email")

        Returns:
            SQLAlchemy expression that extracts the value at json_path

        Raises:
            ValueError: If json_path is invalid or dialect is unsupported
        """
        if not self._validate_json_path(json_path):
            raise ValueError(f"Invalid JSON path: {json_path}")

        full_path = f"$.{json_path}"
        dialect = self.engine.dialect.name

        if dialect == "sqlite":
            # SQLite: json_extract(column, '$.key')
            return func.json_extract(column, full_path)
        elif dialect == "postgresql":
            # PostgreSQL: For nested paths, use #>> operator with array path
            # For simple paths, use ->> operator
            # Since we're dealing with Text column, we need to cast first
            if "." in json_path:
                # Nested: (column)::json #>> '{customer,email}'
                path_array = "{" + json_path.replace(".", ",") + "}"
                return text(f"(input_data)::json #>> '{path_array}'")
            else:
                # Simple: (column)::json->>'key'
                return text(f"(input_data)::json->>'{json_path}'")
        elif dialect == "mysql":
            # MySQL: JSON_UNQUOTE(JSON_EXTRACT(column, '$.key'))
            return func.JSON_UNQUOTE(func.JSON_EXTRACT(column, full_path))
        else:
            raise ValueError(f"Unsupported database dialect: {dialect}")

    # -------------------------------------------------------------------------
    # Transaction Management Methods
    # -------------------------------------------------------------------------

    async def begin_transaction(self) -> None:
        """
        Begin a new transaction.

        If a transaction is already in progress, creates a nested transaction
        using savepoints. This is asyncio-safe using ContextVar.
        """
        ctx = _transaction_context.get()

        if ctx is None:
            # First transaction - create new context with session
            session = AsyncSession(self.engine, expire_on_commit=False)
            ctx = TransactionContext(session=session)
            _transaction_context.set(ctx)

        ctx.depth += 1

        if ctx.depth == 1:
            # Top-level transaction - begin the session transaction
            logger.debug("Beginning top-level transaction")
            await ctx.session.begin()  # type: ignore[union-attr]
        else:
            # Nested transaction - use SQLAlchemy's begin_nested() (creates SAVEPOINT)
            await self._begin_sqlite_transaction_before_savepoint(ctx.session)  # type: ignore[arg-type]
            nested_tx = await ctx.session.begin_nested()  # type: ignore[union-attr]
            ctx.savepoint_stack.append(nested_tx)
            logger.debug(f"Created nested transaction (savepoint) at depth={ctx.depth}")

    async def _begin_sqlite_transaction_before_savepoint(self, session: AsyncSession) -> None:
        """
        Make SQLite begin the enclosing transaction before a SAVEPOINT.

        The sqlite3 driver (aiosqlite too) sends BEGIN only before the first
        INSERT, UPDATE or DELETE, so a transaction that only read so far has
        not begun in SQLite. A SAVEPOINT sent then begins a transaction of its
        own, and releasing it commits: the enclosing transaction's rollback
        would not undo what the savepoint wrote. Begin it here instead. This
        pins nothing earlier than the SAVEPOINT itself would.
        """
        if self.engine.dialect.name != "sqlite":
            return
        connection = await session.connection()
        raw = await connection.get_raw_connection()
        driver = raw.driver_connection
        if driver is not None and not driver.in_transaction:
            await connection.exec_driver_sql("BEGIN")

    async def commit_transaction(self) -> None:
        """
        Commit the current transaction.

        For nested transactions, releases the savepoint.
        For top-level transactions, commits to the database.
        """
        ctx = _transaction_context.get()
        if ctx is None or ctx.depth == 0:
            raise RuntimeError("Not in a transaction")

        # Capture callbacks before any state changes
        callbacks_to_execute: list[Callable[[], Awaitable[None]]] = []

        if ctx.depth == 1:
            # Top-level transaction - commit the session
            logger.debug("Committing top-level transaction")
            await ctx.session.commit()  # type: ignore[union-attr]
            await ctx.session.close()  # type: ignore[union-attr]
            # Capture callbacks to execute after clearing context
            callbacks_to_execute = ctx.post_commit_callbacks.copy()
        else:
            # Nested transaction - commit the savepoint
            nested_tx = ctx.savepoint_stack.pop()
            await nested_tx.commit()
            logger.debug(f"Committed nested transaction (savepoint) at depth={ctx.depth}")

        ctx.depth -= 1

        if ctx.depth == 0:
            # All transactions completed - clear context
            _transaction_context.set(None)
            # Execute post-commit callbacks after successful top-level commit
            for callback in callbacks_to_execute:
                try:
                    await callback()
                except Exception as e:
                    logger.error(f"Post-commit callback failed: {e}")

    async def rollback_transaction(self) -> None:
        """
        Rollback the current transaction.

        For nested transactions, rolls back to the savepoint.
        For top-level transactions, rolls back all changes.
        """
        ctx = _transaction_context.get()
        if ctx is None or ctx.depth == 0:
            raise RuntimeError("Not in a transaction")

        if ctx.depth == 1:
            # Top-level transaction - rollback the session
            logger.debug("Rolling back top-level transaction")
            await ctx.session.rollback()  # type: ignore[union-attr]
            await ctx.session.close()  # type: ignore[union-attr]
        else:
            # Nested transaction - rollback the savepoint
            nested_tx = ctx.savepoint_stack.pop()
            await nested_tx.rollback()
            logger.debug(f"Rolled back nested transaction (savepoint) at depth={ctx.depth}")

        ctx.depth -= 1

        if ctx.depth == 0:
            # All transactions rolled back - clear context
            _transaction_context.set(None)

    def in_transaction(self) -> bool:
        """
        Check if currently in a transaction.

        Returns:
            True if in a transaction, False otherwise.
        """
        ctx = _transaction_context.get()
        return ctx is not None and ctx.depth > 0

    def register_post_commit_callback(self, callback: Callable[[], Awaitable[None]]) -> None:
        """
        Register a callback to be executed after the current transaction commits.

        The callback will be executed after the top-level transaction commits successfully.
        If the transaction is rolled back, the callback will NOT be executed.
        If not in a transaction, the callback will be executed immediately.

        Args:
            callback: An async function to call after commit.

        Raises:
            RuntimeError: If not in a transaction.
        """
        ctx = _transaction_context.get()
        if ctx is None or ctx.depth == 0:
            raise RuntimeError("Cannot register post-commit callback: not in a transaction")
        ctx.post_commit_callbacks.append(callback)
        logger.debug(f"Registered post-commit callback: {callback}")

    async def _commit_if_not_in_transaction(self, session: AsyncSession) -> None:
        """
        Commit session if not in a transaction (auto-commit mode).

        This helper method ensures that operations outside of explicit transactions
        are still committed, while operations inside transactions are deferred
        until the transaction is committed.

        Args:
            session: Database session
        """
        # If this is a transaction context session, don't commit (will be done by commit_transaction)
        ctx = _transaction_context.get()
        if ctx is not None and ctx.session == session:
            return

        # If not in transaction, commit
        if not self.in_transaction():
            await session.commit()

    # -------------------------------------------------------------------------
    # Workflow Definition Methods
    # -------------------------------------------------------------------------

    async def upsert_workflow_definition(
        self,
        workflow_name: str,
        source_hash: str,
        source_code: str,
    ) -> None:
        """Insert or update a workflow definition."""
        session = self._get_session_for_operation()
        async with self._session_scope(session) as session:
            # Check if exists
            result = await session.execute(
                select(WorkflowDefinition).where(
                    and_(
                        WorkflowDefinition.workflow_name == workflow_name,
                        WorkflowDefinition.source_hash == source_hash,
                    )
                )
            )
            existing = result.scalar_one_or_none()

            if existing:
                # Update
                existing.source_code = source_code
            else:
                # Insert
                definition = WorkflowDefinition(
                    workflow_name=workflow_name,
                    source_hash=source_hash,
                    source_code=source_code,
                )
                session.add(definition)

            await self._commit_if_not_in_transaction(session)

    async def get_workflow_definition(
        self,
        workflow_name: str,
        source_hash: str,
    ) -> dict[str, Any] | None:
        """Get a workflow definition by name and hash."""
        session = self._get_session_for_operation()
        async with self._session_scope(session) as session:
            result = await session.execute(
                select(WorkflowDefinition).where(
                    and_(
                        WorkflowDefinition.workflow_name == workflow_name,
                        WorkflowDefinition.source_hash == source_hash,
                    )
                )
            )
            definition = result.scalar_one_or_none()

            if definition is None:
                return None

            return {
                "workflow_name": definition.workflow_name,
                "source_hash": definition.source_hash,
                "source_code": definition.source_code,
                "created_at": definition.created_at.isoformat(),
            }

    async def get_current_workflow_definition(
        self,
        workflow_name: str,
    ) -> dict[str, Any] | None:
        """Get the most recent workflow definition by name."""
        session = self._get_session_for_operation()
        async with self._session_scope(session) as session:
            result = await session.execute(
                select(WorkflowDefinition)
                .where(WorkflowDefinition.workflow_name == workflow_name)
                .order_by(WorkflowDefinition.created_at.desc())
                .limit(1)
            )
            definition = result.scalar_one_or_none()

            if definition is None:
                return None

            return {
                "workflow_name": definition.workflow_name,
                "source_hash": definition.source_hash,
                "source_code": definition.source_code,
                "created_at": definition.created_at.isoformat(),
            }

    # -------------------------------------------------------------------------
    # Workflow Instance Methods
    # -------------------------------------------------------------------------

    async def create_instance(
        self,
        instance_id: str,
        workflow_name: str,
        source_hash: str,
        owner_service: str,
        input_data: dict[str, Any],
        lock_timeout_seconds: int | None = None,
        continued_from: str | None = None,
        sandboxed: bool = False,
        parent_instance_id: str | None = None,
    ) -> None:
        """Create a new workflow instance."""
        session = self._get_session_for_operation()
        async with self._session_scope(session) as session:
            if not sandboxed:
                seen: set[str] = set()
                for parent_id in (parent_instance_id, continued_from):
                    if not parent_id or parent_id in seen:
                        continue
                    seen.add(parent_id)
                    result = await session.execute(
                        select(WorkflowInstance.sandboxed).where(
                            WorkflowInstance.instance_id == parent_id
                        )
                    )
                    parent_sandboxed = result.scalar_one_or_none()
                    if parent_sandboxed is None:
                        raise ValueError(f"parent instance {parent_id} not found")
                    if parent_sandboxed != 0:
                        sandboxed = True
                        break

            instance = WorkflowInstance(
                instance_id=instance_id,
                workflow_name=workflow_name,
                source_hash=source_hash,
                owner_service=owner_service,
                framework="python",
                input_data=json.dumps(input_data),
                lock_timeout_seconds=lock_timeout_seconds,
                continued_from=continued_from,
                sandboxed=1 if sandboxed else 0,
            )
            session.add(instance)

    async def get_instance(self, instance_id: str) -> dict[str, Any] | None:
        """Get workflow instance metadata with its definition."""
        session = self._get_session_for_operation()
        async with self._session_scope(session) as session:
            # Join with workflow_definitions to get source_code
            result = await session.execute(
                select(WorkflowInstance, WorkflowDefinition.source_code)
                .join(
                    WorkflowDefinition,
                    and_(
                        WorkflowInstance.workflow_name == WorkflowDefinition.workflow_name,
                        WorkflowInstance.source_hash == WorkflowDefinition.source_hash,
                    ),
                )
                .where(WorkflowInstance.instance_id == instance_id)
            )
            row = result.one_or_none()

            if row is None:
                return None

            instance, source_code = row

            return {
                "instance_id": instance.instance_id,
                "workflow_name": instance.workflow_name,
                "source_hash": instance.source_hash,
                "owner_service": instance.owner_service,
                "status": instance.status,
                "current_activity_id": instance.current_activity_id,
                "started_at": instance.started_at.isoformat(),
                "updated_at": instance.updated_at.isoformat(),
                "input_data": json.loads(instance.input_data),
                "source_code": source_code,
                "output_data": json.loads(instance.output_data) if instance.output_data else None,
                "locked_by": instance.locked_by,
                "locked_at": instance.locked_at.isoformat() if instance.locked_at else None,
                "lock_timeout_seconds": instance.lock_timeout_seconds,
                "sandboxed": bool(instance.sandboxed),
            }

    async def update_instance_status(
        self,
        instance_id: str,
        status: str,
        output_data: dict[str, Any] | None = None,
        lock_id: str | None = None,
    ) -> None:
        """Update workflow instance status.

        When lock_id is given the update is fenced: it only applies while
        ``locked_by == lock_id`` still holds, and raises LockLostError
        otherwise.
        """
        session = self._get_session_for_operation()
        async with self._session_scope(session) as session:
            conditions = [WorkflowInstance.instance_id == instance_id]
            if lock_id is not None:
                conditions.append(WorkflowInstance.locked_by == lock_id)
                # Terminal statuses are absorbing; rewriting the same status
                # (output first, then status) stays a no-op success.
                conditions.append(
                    or_(
                        WorkflowInstance.status == status,
                        WorkflowInstance.status.notin_(_TERMINAL_STATUSES),
                    )
                )
            stmt = (
                update(WorkflowInstance)
                .where(and_(*conditions))
                .values(
                    status=status,
                    updated_at=func.now(),
                )
            )

            if output_data is not None:
                stmt = stmt.values(output_data=json.dumps(output_data))

            result = await session.execute(stmt)
            if lock_id is not None and (result.rowcount or 0) == 0:  # type: ignore[attr-defined]
                # The refused write changed nothing; a caller's transaction
                # is left for the caller to decide on (_session_scope rolls
                # back a session of our own).
                raise await self._fenced_write_refused(session, instance_id, lock_id)
            await self._commit_if_not_in_transaction(session)

    async def _fenced_write_refused(
        self, session: AsyncSession, instance_id: str, lock_id: str
    ) -> Exception:
        """Explain a fenced write that matched no row.

        InstanceTerminatedError when lock_id still holds an instance that
        already terminated, LockLostError otherwise.
        """
        from edda.locking import InstanceTerminatedError, LockLostError

        row = (
            await session.execute(
                select(WorkflowInstance.locked_by, WorkflowInstance.status).where(
                    WorkflowInstance.instance_id == instance_id
                )
            )
        ).one_or_none()
        if row is not None and row[0] == lock_id and row[1] in _TERMINAL_STATUSES:
            return InstanceTerminatedError(f"Workflow instance {instance_id} already {row[1]}")
        return LockLostError(f"Lock lost for instance {instance_id}")

    async def update_instance_activity(
        self, instance_id: str, activity_id: str, lock_id: str | None = None
    ) -> None:
        """Update the current activity ID for a workflow instance.

        Fenced like update_instance_status when lock_id is given.
        """
        from edda.locking import LockLostError

        session = self._get_session_for_operation()
        async with self._session_scope(session) as session:
            conditions = [WorkflowInstance.instance_id == instance_id]
            if lock_id is not None:
                conditions.append(WorkflowInstance.locked_by == lock_id)
            result = await session.execute(
                update(WorkflowInstance)
                .where(and_(*conditions))
                .values(current_activity_id=activity_id, updated_at=func.now())
            )
            if lock_id is not None and (result.rowcount or 0) == 0:  # type: ignore[attr-defined]
                raise LockLostError(f"Lock lost for instance {instance_id}")
            await self._commit_if_not_in_transaction(session)

    async def list_instances(
        self,
        limit: int = 50,
        page_token: str | None = None,
        status_filter: str | None = None,
        workflow_name_filter: str | None = None,
        instance_id_filter: str | None = None,
        started_after: datetime | None = None,
        started_before: datetime | None = None,
        input_filters: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """List workflow instances with cursor-based pagination and filtering."""
        session = self._get_session_for_operation()
        async with self._session_scope(session) as session:
            # Base query with JOIN
            stmt = (
                select(WorkflowInstance, WorkflowDefinition.source_code)
                .join(
                    WorkflowDefinition,
                    and_(
                        WorkflowInstance.workflow_name == WorkflowDefinition.workflow_name,
                        WorkflowInstance.source_hash == WorkflowDefinition.source_hash,
                    ),
                )
                .where(WorkflowInstance.framework == "python")
                .order_by(
                    WorkflowInstance.started_at.desc(),
                    WorkflowInstance.instance_id.desc(),
                )
            )

            # Apply cursor-based pagination (page_token format: "ISO_DATETIME||INSTANCE_ID")
            if page_token:
                # Parse page_token: || separates datetime and instance_id
                separator = "||"
                if separator in page_token:
                    cursor_time_str, cursor_id = page_token.split(separator, 1)
                    cursor_time = datetime.fromisoformat(cursor_time_str)
                    # Use _make_datetime_comparable for SQLite compatibility
                    started_at_comparable = self._make_datetime_comparable(
                        WorkflowInstance.started_at
                    )
                    # For SQLite, also wrap the cursor_time in func.datetime()
                    cursor_time_comparable: Any
                    if self.engine.dialect.name == "sqlite":
                        cursor_time_comparable = func.datetime(cursor_time_str)
                    else:
                        cursor_time_comparable = cursor_time
                    # For DESC order, we want rows where (started_at, instance_id) < cursor
                    stmt = stmt.where(
                        or_(
                            started_at_comparable < cursor_time_comparable,
                            and_(
                                started_at_comparable == cursor_time_comparable,
                                WorkflowInstance.instance_id < cursor_id,
                            ),
                        )
                    )

            # Apply status filter
            if status_filter:
                stmt = stmt.where(WorkflowInstance.status == status_filter)

            # Apply workflow name and/or instance ID filter (partial match, case-insensitive)
            # When both filters have the same value (unified search), use OR logic
            if workflow_name_filter and instance_id_filter:
                if workflow_name_filter == instance_id_filter:
                    # Unified search: match either workflow name OR instance ID
                    stmt = stmt.where(
                        or_(
                            WorkflowInstance.workflow_name.ilike(f"%{workflow_name_filter}%"),
                            WorkflowInstance.instance_id.ilike(f"%{instance_id_filter}%"),
                        )
                    )
                else:
                    # Separate filters: match both (AND logic)
                    stmt = stmt.where(
                        WorkflowInstance.workflow_name.ilike(f"%{workflow_name_filter}%")
                    )
                    stmt = stmt.where(WorkflowInstance.instance_id.ilike(f"%{instance_id_filter}%"))
            elif workflow_name_filter:
                stmt = stmt.where(WorkflowInstance.workflow_name.ilike(f"%{workflow_name_filter}%"))
            elif instance_id_filter:
                stmt = stmt.where(WorkflowInstance.instance_id.ilike(f"%{instance_id_filter}%"))

            # Apply date range filters (use _make_datetime_comparable for SQLite)
            if started_after or started_before:
                started_at_comparable = self._make_datetime_comparable(WorkflowInstance.started_at)
                if started_after:
                    started_after_comparable: Any
                    if self.engine.dialect.name == "sqlite":
                        started_after_comparable = func.datetime(started_after.isoformat())
                    else:
                        started_after_comparable = started_after
                    stmt = stmt.where(started_at_comparable >= started_after_comparable)
                if started_before:
                    started_before_comparable: Any
                    if self.engine.dialect.name == "sqlite":
                        started_before_comparable = func.datetime(started_before.isoformat())
                    else:
                        started_before_comparable = started_before
                    stmt = stmt.where(started_at_comparable <= started_before_comparable)

            # Apply input data filters (JSON field matching)
            if input_filters:
                for json_path, expected_value in input_filters.items():
                    dialect = self.engine.dialect.name
                    if dialect == "postgresql":
                        # PostgreSQL: use text() for the entire condition
                        if not self._validate_json_path(json_path):
                            raise ValueError(f"Invalid JSON path: {json_path}")
                        if "." in json_path:
                            path_array = "{" + json_path.replace(".", ",") + "}"
                            json_sql = f"(input_data)::json #>> '{path_array}'"
                        else:
                            json_sql = f"(input_data)::json->>'{json_path}'"
                        if expected_value is None:
                            stmt = stmt.where(text(f"({json_sql} IS NULL OR {json_sql} = 'null')"))
                        else:
                            # Escape single quotes in value
                            safe_value = str(expected_value).replace("'", "''")
                            stmt = stmt.where(text(f"{json_sql} = '{safe_value}'"))
                    else:
                        # SQLite and MySQL: use func-based approach
                        json_expr = self._build_json_extract_expr(
                            WorkflowInstance.input_data, json_path
                        )
                        if expected_value is None:
                            stmt = stmt.where(or_(json_expr.is_(None), json_expr == "null"))
                        elif isinstance(expected_value, bool):
                            stmt = stmt.where(json_expr == str(expected_value).lower())
                        elif isinstance(expected_value, (int, float)):
                            if dialect == "sqlite":
                                stmt = stmt.where(json_expr == expected_value)
                            else:
                                stmt = stmt.where(json_expr == str(expected_value))
                        else:
                            stmt = stmt.where(json_expr == str(expected_value))

            # Fetch limit+1 to determine if there are more pages
            stmt = stmt.limit(limit + 1)

            result = await session.execute(stmt)
            rows = result.all()

            # Determine has_more and next_page_token
            has_more = len(rows) > limit
            if has_more:
                rows = rows[:limit]  # Trim to actual limit

            # Generate next_page_token from last row
            next_page_token: str | None = None
            if has_more and rows:
                last_instance = rows[-1][0]
                # Format: ISO_DATETIME||INSTANCE_ID (using || as separator)
                next_page_token = (
                    f"{last_instance.started_at.isoformat()}||{last_instance.instance_id}"
                )

            instances = [
                {
                    "instance_id": instance.instance_id,
                    "workflow_name": instance.workflow_name,
                    "source_hash": instance.source_hash,
                    "owner_service": instance.owner_service,
                    "status": instance.status,
                    "current_activity_id": instance.current_activity_id,
                    "started_at": instance.started_at.isoformat(),
                    "updated_at": instance.updated_at.isoformat(),
                    "input_data": json.loads(instance.input_data),
                    "source_code": source_code,
                    "output_data": (
                        json.loads(instance.output_data) if instance.output_data else None
                    ),
                    "locked_by": instance.locked_by,
                    "locked_at": instance.locked_at.isoformat() if instance.locked_at else None,
                    "lock_timeout_seconds": instance.lock_timeout_seconds,
                }
                for instance, source_code in rows
            ]

            return {
                "instances": instances,
                "next_page_token": next_page_token,
                "has_more": has_more,
            }

    # -------------------------------------------------------------------------
    # Distributed Locking Methods (ALWAYS use separate session/transaction)
    # -------------------------------------------------------------------------

    async def try_acquire_lock(
        self,
        instance_id: str,
        worker_id: str,
        timeout_seconds: int = 300,
    ) -> bool:
        """
        Try to acquire lock with a single conditional UPDATE.

        Atomically claims the row only when it is unlocked or its lease has
        expired (same-worker reclaims create a new epoch on the caller side).
        Returns True if the lock was acquired.

        Note: ALWAYS uses separate session (not external session).
        """
        session = self._get_session_for_operation(is_lock_operation=True)
        async with self._session_scope(session) as session:
            current_time = datetime.now(UTC)

            # Read custom timeout (plain SELECT; the UPDATE below is atomic)
            result = await session.execute(
                select(WorkflowInstance.lock_timeout_seconds).where(
                    WorkflowInstance.instance_id == instance_id
                )
            )
            row = result.one_or_none()
            if row is None:
                await session.commit()
                return False
            actual_timeout = int(row[0] if row[0] is not None else timeout_seconds)

            lock_expires_at = current_time + timedelta(seconds=actual_timeout)
            result = await session.execute(
                update(WorkflowInstance)
                .where(
                    and_(
                        WorkflowInstance.instance_id == instance_id,
                        or_(
                            WorkflowInstance.locked_by.is_(None),
                            WorkflowInstance.lock_expires_at < current_time,
                        ),
                    )
                )
                .values(
                    locked_by=worker_id,
                    locked_at=current_time,
                    lock_expires_at=lock_expires_at,
                    updated_at=current_time,
                )
            )
            await session.commit()
            return bool(result.rowcount and result.rowcount > 0)  # type: ignore[attr-defined]

    async def release_lock(self, instance_id: str, worker_id: str) -> None:
        """
        Release lock only if we own it.

        Note: ALWAYS uses separate session (not external session).
        """
        session = self._get_session_for_operation(is_lock_operation=True)
        async with self._session_scope(session) as session:
            # Use Python datetime for consistency (timezone-aware)
            current_time = datetime.now(UTC)

            await session.execute(
                update(WorkflowInstance)
                .where(
                    and_(
                        WorkflowInstance.instance_id == instance_id,
                        WorkflowInstance.locked_by == worker_id,
                    )
                )
                .values(
                    locked_by=None,
                    locked_at=None,
                    lock_expires_at=None,
                    updated_at=current_time,
                )
            )
            await session.commit()

    async def refresh_lock(
        self, instance_id: str, worker_id: str, timeout_seconds: int = 300
    ) -> bool:
        """
        Refresh lock timestamp and expiry time.

        Args:
            instance_id: Workflow instance ID
            worker_id: Worker ID that currently owns the lock
            timeout_seconds: Default timeout (used if instance doesn't have custom timeout)

        Note: ALWAYS uses separate session (not external session).
        """
        session = self._get_session_for_operation(is_lock_operation=True)
        async with self._session_scope(session) as session:
            # Use Python datetime for consistency with try_acquire_lock() (timezone-aware)
            current_time = datetime.now(UTC)

            # First, get the instance to determine actual timeout
            result = await session.execute(
                select(WorkflowInstance).where(
                    and_(
                        WorkflowInstance.instance_id == instance_id,
                        WorkflowInstance.locked_by == worker_id,
                    )
                )
            )
            instance = result.scalar_one_or_none()

            if instance is None:
                # Instance doesn't exist or not locked by us
                await session.commit()
                return False

            # Determine actual timeout (priority: instance > parameter > default)
            actual_timeout = int(
                instance.lock_timeout_seconds
                if instance.lock_timeout_seconds is not None
                else timeout_seconds
            )

            # Calculate new expiry time
            lock_expires_at = current_time + timedelta(seconds=actual_timeout)

            # Update lock timestamp and expiry
            result = await session.execute(
                update(WorkflowInstance)
                .where(
                    and_(
                        WorkflowInstance.instance_id == instance_id,
                        WorkflowInstance.locked_by == worker_id,
                    )
                )
                .values(
                    locked_at=current_time,
                    lock_expires_at=lock_expires_at,
                    updated_at=current_time,
                )
            )
            await session.commit()
            return bool(result.rowcount and result.rowcount > 0)  # type: ignore[attr-defined]

    async def prove_lock(self, instance_id: str, lock_id: str) -> None:
        """
        Prove that lock_id still holds the instance lock, in the current
        transaction (unlike the lock operations, this joins it).

        The conditional write only matches while ``locked_by == lock_id``.
        It also orders the transaction after any lease steal: PostgreSQL
        and MySQL keep the instance row locked until commit, SQLite holds
        the write lock.

        Raises:
            LockLostError: If lock_id does not hold the lock
        """
        session = self._get_session_for_operation()
        async with self._session_scope(session) as session:
            result = await session.execute(
                update(WorkflowInstance)
                .where(
                    and_(
                        WorkflowInstance.instance_id == instance_id,
                        WorkflowInstance.locked_by == lock_id,
                    )
                )
                .values(updated_at=func.now())
            )
            if (result.rowcount or 0) == 0:  # type: ignore[attr-defined]
                from edda.locking import LockLostError

                raise LockLostError(f"Lock lost for instance {instance_id}")
            await self._commit_if_not_in_transaction(session)

    async def cleanup_stale_locks(self) -> list[dict[str, str]]:
        """
        Clean up locks that have expired (based on lock_expires_at column).

        Returns list of workflows with status='running' or 'compensating' that need auto-resume.

        Workflows with status='compensating' crashed during compensation execution
        and need special handling to complete compensations.

        Note: Uses lock_expires_at column for efficient SQL-side filtering.
        Note: ALWAYS uses separate session (not external session).
        """
        session = self._get_session_for_operation(is_lock_operation=True)
        async with self._session_scope(session) as session:
            # Use timezone-aware datetime to match DateTime(timezone=True) columns
            current_time = datetime.now(UTC)

            # SQL-side filtering: Find all instances with expired locks
            # Use database abstraction layer for cross-database compatibility
            result = await session.execute(
                select(WorkflowInstance).where(
                    and_(
                        WorkflowInstance.locked_by.isnot(None),
                        WorkflowInstance.lock_expires_at.isnot(None),
                        self._make_datetime_comparable(WorkflowInstance.lock_expires_at)
                        < self._get_current_time_expr(),
                        WorkflowInstance.framework == "python",
                    )
                )
            )
            instances = result.scalars().all()

            stale_instance_ids = []
            workflows_to_resume = []

            # Collect instance IDs and workflows to resume
            for instance in instances:
                stale_instance_ids.append(instance.instance_id)

                # Add to resume list if status is 'running' or 'compensating'
                if instance.status in ["running", "compensating", "recurred"]:
                    workflows_to_resume.append(
                        {
                            "instance_id": str(instance.instance_id),
                            "workflow_name": str(instance.workflow_name),
                            "source_hash": str(instance.source_hash),
                            "status": str(instance.status),
                        }
                    )

            # Release all stale locks in one UPDATE statement
            if stale_instance_ids:
                await session.execute(
                    update(WorkflowInstance)
                    .where(WorkflowInstance.instance_id.in_(stale_instance_ids))
                    .values(
                        locked_by=None,
                        locked_at=None,
                        lock_expires_at=None,
                        updated_at=current_time,
                    )
                )

            await session.commit()
            return workflows_to_resume

    # -------------------------------------------------------------------------
    # System-level Locking Methods (for background task coordination)
    # -------------------------------------------------------------------------

    async def try_acquire_system_lock(
        self,
        lock_name: str,
        worker_id: str,
        timeout_seconds: int = 60,
    ) -> bool:
        """
        Try to acquire a system-level lock for coordinating background tasks.

        Uses atomic UPDATE pattern to avoid race conditions:
        1. Ensure row exists (INSERT OR IGNORE / ON CONFLICT DO NOTHING)
        2. Atomic UPDATE with WHERE condition (rowcount determines success)

        Note: ALWAYS uses separate session (not external session).
        """
        session = self._get_session_for_operation(is_lock_operation=True)
        async with self._session_scope(session) as session:
            current_time = datetime.now(UTC)
            lock_expires_at = current_time + timedelta(seconds=timeout_seconds)

            # Get dialect name
            dialect_name = self.engine.dialect.name

            # 1. Ensure row exists (idempotent INSERT)
            if dialect_name == "sqlite":
                from sqlalchemy.dialects.sqlite import insert as sqlite_insert

                stmt: Any = (
                    sqlite_insert(SystemLock)
                    .values(
                        lock_name=lock_name,
                        locked_by=None,
                        locked_at=None,
                        lock_expires_at=None,
                    )
                    .on_conflict_do_nothing(index_elements=["lock_name"])
                )
            elif dialect_name == "postgresql":
                from sqlalchemy.dialects.postgresql import insert as pg_insert

                stmt = (
                    pg_insert(SystemLock)
                    .values(
                        lock_name=lock_name,
                        locked_by=None,
                        locked_at=None,
                        lock_expires_at=None,
                    )
                    .on_conflict_do_nothing(index_elements=["lock_name"])
                )
            else:  # mysql
                from sqlalchemy.dialects.mysql import insert as mysql_insert

                stmt = (
                    mysql_insert(SystemLock)
                    .values(
                        lock_name=lock_name,
                        locked_by=None,
                        locked_at=None,
                        lock_expires_at=None,
                    )
                    .on_duplicate_key_update(lock_name=lock_name)
                )  # No-op update

            await session.execute(stmt)
            await session.commit()

            # 2. Atomic UPDATE to acquire lock (rowcount == 1 means success)
            # Use SQL-side datetime comparison for cross-DB compatibility
            current_time_expr = self._get_current_time_expr()
            result = await session.execute(
                update(SystemLock)
                .where(
                    and_(
                        SystemLock.lock_name == lock_name,
                        or_(
                            SystemLock.locked_by == None,  # noqa: E711
                            SystemLock.locked_by == worker_id,  # Allow renewal by same worker
                            self._make_datetime_comparable(SystemLock.lock_expires_at)
                            <= current_time_expr,
                        ),
                    )
                )
                .values(
                    locked_by=worker_id,
                    locked_at=current_time,
                    lock_expires_at=lock_expires_at,
                )
            )
            await session.commit()

            return bool(result.rowcount == 1)  # type: ignore[attr-defined]

    async def release_system_lock(self, lock_name: str, worker_id: str) -> None:
        """
        Release a system-level lock.

        Only releases the lock if it's held by the specified worker.

        Note: ALWAYS uses separate session (not external session).
        """
        session = self._get_session_for_operation(is_lock_operation=True)
        async with self._session_scope(session) as session:
            await session.execute(
                update(SystemLock)
                .where(
                    and_(
                        SystemLock.lock_name == lock_name,
                        SystemLock.locked_by == worker_id,
                    )
                )
                .values(
                    locked_by=None,
                    locked_at=None,
                    lock_expires_at=None,
                )
            )
            await session.commit()

    # -------------------------------------------------------------------------
    # History Methods (prefer external session)
    # -------------------------------------------------------------------------

    async def append_history(
        self,
        instance_id: str,
        activity_id: str,
        event_type: str,
        event_data: dict[str, Any] | bytes,
        lock_id: str | None = None,
    ) -> None:
        """Append an event to workflow execution history.

        Fenced when lock_id is given: a guard update proves the caller still
        holds ``locked_by == lock_id`` before the row is inserted.
        """
        session = self._get_session_for_operation()
        async with self._session_scope(session) as session:
            # Determine data type and storage columns
            if isinstance(event_data, bytes):
                data_type = "binary"
                event_data_json = None
                event_data_bin = event_data
            else:
                data_type = "json"
                event_data_json = json.dumps(event_data)
                event_data_bin = None

            if lock_id is not None:
                # Fenced write: prove we still hold the lock epoch before the
                # history row becomes visible.
                guard = await session.execute(
                    update(WorkflowInstance)
                    .where(
                        and_(
                            WorkflowInstance.instance_id == instance_id,
                            WorkflowInstance.locked_by == lock_id,
                        )
                    )
                    .values(updated_at=func.now())
                )
                if (guard.rowcount or 0) == 0:  # type: ignore[attr-defined]
                    from edda.locking import LockLostError

                    raise LockLostError(f"Lock lost for instance {instance_id}")

            history = WorkflowHistory(
                instance_id=instance_id,
                activity_id=activity_id,
                event_type=event_type,
                data_type=data_type,
                event_data=event_data_json,
                event_data_binary=event_data_bin,
            )
            session.add(history)
            await self._commit_if_not_in_transaction(session)

    async def get_history(self, instance_id: str) -> list[dict[str, Any]]:
        """
        Get workflow execution history in order.

        Returns history events ordered by creation time.
        """
        session = self._get_session_for_operation()
        async with self._session_scope(session) as session:
            result = await session.execute(
                select(WorkflowHistory)
                .where(WorkflowHistory.instance_id == instance_id)
                .order_by(WorkflowHistory.created_at.asc())
            )
            rows = result.scalars().all()

            return [
                {
                    "id": row.id,
                    "instance_id": row.instance_id,
                    "activity_id": row.activity_id,
                    "event_type": row.event_type,
                    "event_data": (
                        row.event_data_binary
                        if row.data_type == "binary"
                        else json.loads(row.event_data)  # type: ignore[arg-type]
                    ),
                    "created_at": row.created_at.isoformat(),
                }
                for row in rows
            ]

    async def archive_history(self, instance_id: str) -> int:
        """
        Archive workflow history for the recur pattern.

        Moves all history entries from workflow_history to workflow_history_archive.
        Binary data is converted to base64 for JSON storage in the archive.

        Returns:
            Number of history entries archived
        """
        import base64

        session = self._get_session_for_operation()
        async with self._session_scope(session) as session:
            # Get all history entries for this instance
            result = await session.execute(
                select(WorkflowHistory)
                .where(WorkflowHistory.instance_id == instance_id)
                .order_by(WorkflowHistory.created_at.asc())
            )
            history_rows = result.scalars().all()

            if not history_rows:
                return 0

            # Archive each history entry
            for row in history_rows:
                # Convert event_data to JSON string for archive
                event_data_json: str | None
                if row.data_type == "binary" and row.event_data_binary is not None:
                    # Convert binary to base64 for JSON storage
                    event_data_json = json.dumps(
                        {
                            "_binary": True,
                            "data": base64.b64encode(row.event_data_binary).decode("ascii"),
                        }
                    )
                else:
                    # Already JSON, use as-is
                    event_data_json = row.event_data

                archive_entry = WorkflowHistoryArchive(
                    instance_id=row.instance_id,
                    activity_id=row.activity_id,
                    event_type=row.event_type,
                    event_data=event_data_json,
                    created_at=row.created_at,
                )
                session.add(archive_entry)

            # Delete original history entries
            await session.execute(
                delete(WorkflowHistory).where(WorkflowHistory.instance_id == instance_id)
            )

            await self._commit_if_not_in_transaction(session)
            return len(history_rows)

    async def reset_instance_for_recur(
        self, instance_id: str, input_data: dict[str, Any], lock_id: str
    ) -> None:
        """
        Reset one instance for its next recur iteration in a single fenced
        transaction.

        The lock proof and all mutations share one session/transaction: either
        the whole iteration boundary commits or nothing does. Instance-scoped
        actor state (names, monitors, capabilities, tasks, FS ownership,
        sandboxing) and channel delivery cursors deliberately survive.
        """
        import base64

        from edda.locking import LockLostError

        session = self._get_session_for_operation(is_lock_operation=True)
        async with self._session_scope(session) as session:
            result = await session.execute(
                update(WorkflowInstance)
                .where(
                    and_(
                        WorkflowInstance.instance_id == instance_id,
                        WorkflowInstance.locked_by == lock_id,
                    )
                )
                .values(updated_at=func.now())
            )
            if (result.rowcount or 0) == 0:  # type: ignore[attr-defined]
                raise LockLostError(f"Lock lost for instance {instance_id}")

            history_rows = (
                (
                    await session.execute(
                        select(WorkflowHistory)
                        .where(WorkflowHistory.instance_id == instance_id)
                        .order_by(WorkflowHistory.created_at.asc())
                    )
                )
                .scalars()
                .all()
            )
            for row in history_rows:
                event_data_json: str | None
                if row.data_type == "binary" and row.event_data_binary is not None:
                    event_data_json = json.dumps(
                        {
                            "_binary": True,
                            "data": base64.b64encode(row.event_data_binary).decode("ascii"),
                        }
                    )
                else:
                    event_data_json = row.event_data
                session.add(
                    WorkflowHistoryArchive(
                        instance_id=row.instance_id,
                        activity_id=row.activity_id,
                        event_type=row.event_type,
                        event_data=event_data_json,
                        created_at=row.created_at,
                    )
                )

            await session.execute(
                delete(WorkflowHistory).where(WorkflowHistory.instance_id == instance_id)
            )
            await session.execute(
                delete(WorkflowTimerSubscription).where(
                    WorkflowTimerSubscription.instance_id == instance_id
                )
            )
            await session.execute(
                delete(ChannelSubscription).where(ChannelSubscription.instance_id == instance_id)
            )
            await session.execute(
                delete(ChannelMessageClaim).where(ChannelMessageClaim.instance_id == instance_id)
            )
            await session.execute(
                delete(WorkflowGroupMembership).where(
                    WorkflowGroupMembership.instance_id == instance_id
                )
            )
            await session.execute(
                delete(WorkflowCompensation).where(WorkflowCompensation.instance_id == instance_id)
            )
            await session.execute(
                update(WorkflowInstance)
                .where(
                    and_(
                        WorkflowInstance.instance_id == instance_id,
                        WorkflowInstance.locked_by == lock_id,
                    )
                )
                .values(
                    status="recurred",
                    input_data=json.dumps(input_data),
                    output_data=None,
                    current_activity_id=None,
                    updated_at=func.now(),
                )
            )

    async def find_first_cancellation_event(self, instance_id: str) -> dict[str, Any] | None:
        """
        Find the first cancellation event in workflow history.

        Uses LIMIT 1 optimization to avoid loading all history events.
        """
        session = self._get_session_for_operation()
        async with self._session_scope(session) as session:
            # Query for cancellation events using LIMIT 1
            result = await session.execute(
                select(WorkflowHistory)
                .where(
                    and_(
                        WorkflowHistory.instance_id == instance_id,
                        or_(
                            WorkflowHistory.event_type == "WorkflowCancelled",
                            func.lower(WorkflowHistory.event_type).contains("cancel"),
                        ),
                    )
                )
                .order_by(WorkflowHistory.created_at.asc())
                .limit(1)
            )
            row = result.scalars().first()

            if row is None:
                return None

            # Parse event_data based on data_type
            if row.data_type == "binary" and row.event_data_binary is not None:
                event_data: dict[str, Any] | bytes = row.event_data_binary
            else:
                event_data = json.loads(row.event_data) if row.event_data else {}

            return {
                "id": row.id,
                "instance_id": row.instance_id,
                "activity_id": row.activity_id,
                "event_type": row.event_type,
                "event_data": event_data,
                "created_at": row.created_at,
            }

    # -------------------------------------------------------------------------
    # Compensation Methods (prefer external session)
    # -------------------------------------------------------------------------

    async def push_compensation(
        self,
        instance_id: str,
        activity_id: str,
        activity_name: str,
        args: dict[str, Any],
    ) -> None:
        """Push a compensation to the stack."""
        session = self._get_session_for_operation()
        async with self._session_scope(session) as session:
            compensation = WorkflowCompensation(
                instance_id=instance_id,
                activity_id=activity_id,
                activity_name=activity_name,
                args=json.dumps(args),
            )
            session.add(compensation)
            await self._commit_if_not_in_transaction(session)

    async def get_compensations(self, instance_id: str) -> list[dict[str, Any]]:
        """Get compensations in LIFO order (most recent first)."""
        session = self._get_session_for_operation()
        async with self._session_scope(session) as session:
            result = await session.execute(
                select(WorkflowCompensation)
                .where(WorkflowCompensation.instance_id == instance_id)
                .order_by(WorkflowCompensation.created_at.desc(), WorkflowCompensation.id.desc())
            )
            rows = result.scalars().all()

            return [
                {
                    "id": row.id,
                    "instance_id": row.instance_id,
                    "activity_id": row.activity_id,
                    "activity_name": row.activity_name,
                    "args": json.loads(row.args) if row.args else [],
                    "created_at": row.created_at.isoformat(),
                }
                for row in rows
            ]

    async def clear_compensations(self, instance_id: str) -> None:
        """Clear all compensations for a workflow instance."""
        session = self._get_session_for_operation()
        async with self._session_scope(session) as session:
            await session.execute(
                delete(WorkflowCompensation).where(WorkflowCompensation.instance_id == instance_id)
            )
            await self._commit_if_not_in_transaction(session)

    # -------------------------------------------------------------------------
    # Timer Subscription Methods
    # -------------------------------------------------------------------------

    async def register_timer_subscription_and_release_lock(
        self,
        instance_id: str,
        worker_id: str,
        timer_id: str,
        expires_at: datetime,
        activity_id: str | None = None,
    ) -> None:
        """
        Atomically register timer subscription and release workflow lock.

        This performs FOUR operations in a SINGLE transaction:
        1. Register timer subscription
        2. Update current activity
        3. Update status to 'waiting_for_timer'
        4. Release lock

        This ensures distributed coroutines work correctly - when a workflow
        calls wait_timer(), the subscription is registered and lock is released
        atomically, so ANY worker can resume the workflow when the timer expires.

        Note: Uses LOCK operation session (separate from external session).
        """
        session = self._get_session_for_operation(is_lock_operation=True)
        async with self._session_scope(session) as session, session.begin():
            # 1. Verify we hold the lock (sanity check)
            result = await session.execute(
                select(WorkflowInstance.locked_by).where(
                    WorkflowInstance.instance_id == instance_id
                )
            )
            row = result.one_or_none()

            if row is None:
                raise RuntimeError(f"Workflow instance {instance_id} not found")

            current_lock_holder = row[0]
            if current_lock_holder != worker_id:
                raise RuntimeError(
                    f"Cannot release lock: worker {worker_id} does not hold lock "
                    f"for {instance_id} (held by: {current_lock_holder})"
                )

            # 2. Register timer subscription (with conflict handling)
            # Check if exists
            result = await session.execute(
                select(WorkflowTimerSubscription).where(
                    and_(
                        WorkflowTimerSubscription.instance_id == instance_id,
                        WorkflowTimerSubscription.timer_id == timer_id,
                    )
                )
            )
            existing = result.scalar_one_or_none()

            if not existing:
                # Insert new subscription
                subscription = WorkflowTimerSubscription(
                    instance_id=instance_id,
                    timer_id=timer_id,
                    expires_at=expires_at,
                    activity_id=activity_id,
                )
                session.add(subscription)

            # 3. Update current activity (if provided)
            if activity_id is not None:
                await session.execute(
                    update(WorkflowInstance)
                    .where(WorkflowInstance.instance_id == instance_id)
                    .values(current_activity_id=activity_id, updated_at=func.now())
                )

            # 4. Update status to 'waiting_for_timer' and release lock. A
            # terminated instance stays terminated: raising rolls back the
            # timer registered above.
            result = await session.execute(
                update(WorkflowInstance)
                .where(
                    and_(
                        WorkflowInstance.instance_id == instance_id,
                        WorkflowInstance.locked_by == worker_id,
                        WorkflowInstance.status.notin_(_TERMINAL_STATUSES),
                    )
                )
                .values(
                    status="waiting_for_timer",
                    locked_by=None,
                    locked_at=None,
                    updated_at=func.now(),
                )
            )
            if (result.rowcount or 0) == 0:  # type: ignore[attr-defined]
                raise await self._fenced_write_refused(session, instance_id, worker_id)

    async def find_expired_timers(self) -> list[dict[str, Any]]:
        """Find timer subscriptions that have expired.

        Returns timer info including workflow status to avoid N+1 queries.
        The SQL query already filters by status='waiting_for_timer'.
        """
        session = self._get_session_for_operation()
        async with self._session_scope(session) as session:
            result = await session.execute(
                select(
                    WorkflowTimerSubscription.instance_id,
                    WorkflowTimerSubscription.timer_id,
                    WorkflowTimerSubscription.expires_at,
                    WorkflowTimerSubscription.activity_id,
                    WorkflowInstance.workflow_name,
                    WorkflowInstance.status,  # Include status to avoid N+1 query
                )
                .join(
                    WorkflowInstance,
                    WorkflowTimerSubscription.instance_id == WorkflowInstance.instance_id,
                )
                .where(
                    and_(
                        self._make_datetime_comparable(WorkflowTimerSubscription.expires_at)
                        <= self._get_current_time_expr(),
                        WorkflowInstance.status == "waiting_for_timer",
                        WorkflowInstance.framework == "python",
                    )
                )
            )
            rows = result.all()

            return [
                {
                    "instance_id": row[0],
                    "timer_id": row[1],
                    "expires_at": row[2].isoformat(),
                    "activity_id": row[3],
                    "workflow_name": row[4],
                    "status": row[5],  # Always 'waiting_for_timer' due to WHERE clause
                }
                for row in rows
            ]

    async def remove_timer_subscription(
        self,
        instance_id: str,
        timer_id: str,
    ) -> None:
        """Remove timer subscription after the timer expires."""
        session = self._get_session_for_operation()
        async with self._session_scope(session) as session:
            await session.execute(
                delete(WorkflowTimerSubscription).where(
                    and_(
                        WorkflowTimerSubscription.instance_id == instance_id,
                        WorkflowTimerSubscription.timer_id == timer_id,
                    )
                )
            )
            await self._commit_if_not_in_transaction(session)

    # -------------------------------------------------------------------------
    # Transactional Outbox Methods (prefer external session)
    # -------------------------------------------------------------------------

    async def add_outbox_event(
        self,
        event_id: str,
        event_type: str,
        event_source: str,
        event_data: dict[str, Any] | bytes,
        content_type: str = "application/json",
    ) -> None:
        """Add an event to the transactional outbox."""
        session = self._get_session_for_operation()
        async with self._session_scope(session) as session:
            # Determine data type and storage columns
            if isinstance(event_data, bytes):
                data_type = "binary"
                event_data_json = None
                event_data_bin = event_data
            else:
                data_type = "json"
                event_data_json = json.dumps(event_data)
                event_data_bin = None

            event = OutboxEvent(
                event_id=event_id,
                event_type=event_type,
                event_source=event_source,
                data_type=data_type,
                event_data=event_data_json,
                event_data_binary=event_data_bin,
                content_type=content_type,
            )
            session.add(event)
            await self._commit_if_not_in_transaction(session)

        # Send NOTIFY for new outbox event
        await self._send_notify(
            "workflow_outbox_pending",
            {"evt_id": event_id, "evt_type": event_type},
        )

    async def get_pending_outbox_events(self, limit: int = 10) -> list[dict[str, Any]]:
        """
        Get pending/failed outbox events for publishing (with row-level locking).

        This method uses SELECT FOR UPDATE SKIP LOCKED to safely fetch events
        in a multi-worker environment. It fetches both 'pending' and 'failed'
        events (for retry). Fetched events are immediately marked as 'processing'
        to prevent duplicate processing by other workers.

        Args:
            limit: Maximum number of events to fetch

        Returns:
            List of event dictionaries with 'processing' status
        """
        # Use new session for lock operation (SKIP LOCKED requires separate transactions)
        session = self._get_session_for_operation(is_lock_operation=True)
        # Explicitly begin transaction before SELECT FOR UPDATE
        # This ensures proper transaction isolation for SKIP LOCKED
        async with self._session_scope(session) as session, session.begin():
            # 1. SELECT FOR UPDATE to lock rows. Claims 'pending' and 'failed'
            # (retry), plus 'processing' rows whose claim went stale — a
            # crashed relayer must not strand events forever.
            stale_before = datetime.now(UTC) - timedelta(seconds=OUTBOX_CLAIM_STALE_SECONDS)
            result = await session.execute(
                select(OutboxEvent)
                .where(
                    or_(
                        OutboxEvent.status.in_(["pending", "failed"]),
                        and_(
                            OutboxEvent.status == "processing",
                            func.coalesce(OutboxEvent.claimed_at, OutboxEvent.published_at)
                            < stale_before,
                        ),
                    )
                )
                .order_by(OutboxEvent.created_at.asc())
                .limit(limit)
                .with_for_update(skip_locked=True)
            )
            rows = result.scalars().all()

            # 2. Mark as 'processing' and stamp the claim time
            if rows:
                event_ids = [row.event_id for row in rows]
                await session.execute(
                    update(OutboxEvent)
                    .where(OutboxEvent.event_id.in_(event_ids))
                    .values(status="processing", claimed_at=datetime.now(UTC))
                )

            # 3. Return events (now with status='processing')
            return [
                {
                    "event_id": row.event_id,
                    "event_type": row.event_type,
                    "event_source": row.event_source,
                    "event_data": (
                        row.event_data_binary
                        if row.data_type == "binary"
                        else json.loads(row.event_data)  # type: ignore[arg-type]
                    ),
                    "content_type": row.content_type,
                    "created_at": row.created_at.isoformat(),
                    "status": "processing",  # Always 'processing' after update
                    "retry_count": row.retry_count,
                    "last_error": row.last_error,
                }
                for row in rows
            ]

    async def mark_outbox_published(self, event_id: str) -> None:
        """Mark outbox event as successfully published."""
        session = self._get_session_for_operation()
        async with self._session_scope(session) as session:
            await session.execute(
                update(OutboxEvent)
                .where(OutboxEvent.event_id == event_id)
                .values(status="published", published_at=func.now(), claimed_at=None)
            )
            await self._commit_if_not_in_transaction(session)

    async def mark_outbox_failed(self, event_id: str, error: str) -> None:
        """
        Mark event as failed and increment retry count.

        The event status is changed to 'failed' so it can be retried later.
        get_pending_outbox_events() will fetch both 'pending' and 'failed' events.
        """
        session = self._get_session_for_operation()
        async with self._session_scope(session) as session:
            await session.execute(
                update(OutboxEvent)
                .where(OutboxEvent.event_id == event_id)
                .values(
                    status="failed",
                    retry_count=OutboxEvent.retry_count + 1,
                    last_error=error,
                    claimed_at=None,
                )
            )
            await self._commit_if_not_in_transaction(session)

    async def mark_outbox_permanently_failed(self, event_id: str, error: str) -> None:
        """Mark outbox event as permanently failed (sets status to 'failed')."""
        session = self._get_session_for_operation()
        async with self._session_scope(session) as session:
            await session.execute(
                update(OutboxEvent)
                .where(OutboxEvent.event_id == event_id)
                .values(
                    status="failed",
                    last_error=error,
                    claimed_at=None,
                )
            )
            await self._commit_if_not_in_transaction(session)

    async def mark_outbox_invalid(self, event_id: str, error: str) -> None:
        """Mark outbox event as invalid (sets status to 'invalid')."""
        session = self._get_session_for_operation()
        async with self._session_scope(session) as session:
            await session.execute(
                update(OutboxEvent)
                .where(OutboxEvent.event_id == event_id)
                .values(
                    status="invalid",
                    last_error=error,
                    claimed_at=None,
                )
            )
            await self._commit_if_not_in_transaction(session)

    async def mark_outbox_expired(self, event_id: str, error: str) -> None:
        """Mark outbox event as expired (sets status to 'expired')."""
        session = self._get_session_for_operation()
        async with self._session_scope(session) as session:
            await session.execute(
                update(OutboxEvent)
                .where(OutboxEvent.event_id == event_id)
                .values(
                    status="expired",
                    last_error=error,
                    claimed_at=None,
                )
            )
            await self._commit_if_not_in_transaction(session)

    async def cleanup_published_events(self, older_than_hours: int = 24) -> int:
        """Clean up successfully published events older than threshold."""
        session = self._get_session_for_operation()
        async with self._session_scope(session) as session:
            threshold = datetime.now(UTC) - timedelta(hours=older_than_hours)

            result = await session.execute(
                delete(OutboxEvent).where(
                    and_(
                        OutboxEvent.status == "published",
                        OutboxEvent.published_at < threshold,
                    )
                )
            )
            await self._commit_if_not_in_transaction(session)
            return int(getattr(result, "rowcount", 0) or 0)

    # -------------------------------------------------------------------------
    # Workflow Cancellation Methods
    # -------------------------------------------------------------------------

    async def cancel_instance(
        self, instance_id: str, cancelled_by: str, *, allow_compensating: bool = True
    ) -> bool:
        """
        Cancel a workflow instance.

        Only running, waiting_for_* and recurred workflows can be cancelled;
        compensating ones never are (their compensations run to the end).
        This method atomically:
        1. Checks current status
        2. Updates status to 'cancelled' if allowed (with atomic status check)
        3. Clears locks
        4. Records cancellation metadata
        5. Removes event subscriptions (if waiting for event)
        6. Removes timer subscriptions (if waiting for timer)

        The UPDATE includes a status condition in WHERE clause to prevent
        TOCTOU (time-of-check to time-of-use) race conditions. If the status
        changes between SELECT and UPDATE, the UPDATE will affect 0 rows
        and the cancellation will fail safely.

        Args:
            instance_id: Workflow instance to cancel
            cancelled_by: Who/what triggered the cancellation
            allow_compensating: Deprecated compatibility argument; ignored.
                Compensating instances are never cancellable.

        Returns:
            True if successfully cancelled, False otherwise

        Note: Uses LOCK operation session (separate from external session).
        """
        # ``allow_compensating`` is kept for source compatibility but ignored:
        # compensation is kernel recovery and is never externally cancellable.
        # ``recurred`` is a live between-iterations state and is cancellable.
        _ = allow_compensating
        cancellable_statuses: tuple[str, ...] = (
            "running",
            "waiting_for_event",
            "waiting_for_timer",
            "waiting_for_message",
            "recurred",
        )

        session = self._get_session_for_operation(is_lock_operation=True)
        async with self._session_scope(session) as session, session.begin():
            # Get current instance status
            result = await session.execute(
                select(WorkflowInstance.status).where(WorkflowInstance.instance_id == instance_id)
            )
            row = result.one_or_none()

            if row is None:
                # Instance not found
                return False

            current_status = row[0]

            # Only live non-compensating workflows are externally cancellable.
            if current_status not in cancellable_statuses:
                # Already completed, failed, or cancelled
                return False

            # Update status to cancelled and record metadata
            # IMPORTANT: Include status condition in WHERE clause to prevent TOCTOU race
            # If another worker changed the status between SELECT and UPDATE,
            # this UPDATE will affect 0 rows and we'll return False
            cancellation_metadata = {
                "cancelled_by": cancelled_by,
                "cancelled_at": datetime.now(UTC).isoformat(),
                "previous_status": current_status,
            }

            update_result = await session.execute(
                update(WorkflowInstance)
                .where(
                    and_(
                        WorkflowInstance.instance_id == instance_id,
                        WorkflowInstance.status == current_status,  # Atomic check
                    )
                )
                .values(
                    status="cancelled",
                    output_data=json.dumps(cancellation_metadata),
                    locked_by=None,
                    locked_at=None,
                    lock_expires_at=None,
                    updated_at=func.now(),
                )
            )

            if update_result.rowcount == 0:  # type: ignore[attr-defined]
                # Status changed between SELECT and UPDATE (race condition)
                # Another worker may have resumed/modified the workflow
                return False

            # Drop the instance's timers and channel waits whatever it was
            # doing: a timer or delivery that fires later must not resume a
            # cancelled instance (e.g. one registered by a run that held the
            # lock while it was cancelled).
            await session.execute(
                delete(WorkflowTimerSubscription).where(
                    WorkflowTimerSubscription.instance_id == instance_id
                )
            )
            await session.execute(
                update(ChannelSubscription)
                .where(ChannelSubscription.instance_id == instance_id)
                .values(activity_id=None, timeout_at=None)
            )

            return True

    async def finalize_cancelled_instance(
        self, instance_id: str, cancelled_by: str, lock_id: str
    ) -> bool:
        """
        Finish a user cancellation after its compensations completed.

        This is an internal recovery transition, not a cancel operation: the
        row must already be ``compensating`` and the caller must still hold
        the fenced lock. External cancellation of ``compensating`` remains
        impossible.
        """
        session = self._get_session_for_operation(is_lock_operation=True)
        async with self._session_scope(session) as session:
            cancellation_metadata = {
                "cancelled_by": cancelled_by,
                "cancelled_at": datetime.now(UTC).isoformat(),
                "previous_status": "compensating",
            }
            result = await session.execute(
                update(WorkflowInstance)
                .where(
                    and_(
                        WorkflowInstance.instance_id == instance_id,
                        WorkflowInstance.status == "compensating",
                        WorkflowInstance.locked_by == lock_id,
                    )
                )
                .values(
                    status="cancelled",
                    output_data=json.dumps(cancellation_metadata),
                    locked_by=None,
                    locked_at=None,
                    lock_expires_at=None,
                    updated_at=func.now(),
                )
            )
            if not result.rowcount:  # type: ignore[attr-defined]
                return False
            await session.execute(
                delete(WorkflowTimerSubscription).where(
                    WorkflowTimerSubscription.instance_id == instance_id
                )
            )
            await session.execute(
                update(ChannelSubscription)
                .where(ChannelSubscription.instance_id == instance_id)
                .values(activity_id=None, timeout_at=None)
            )
            return True

    # -------------------------------------------------------------------------
    # Message Subscription Methods
    # -------------------------------------------------------------------------

    async def find_waiting_instances_by_channel(self, channel: str) -> list[dict[str, Any]]:
        """
        Find all workflow instances waiting on a specific channel.

        Args:
            channel: Channel name to search for

        Returns:
            List of subscription info dicts with instance_id, channel, activity_id, timeout_at
        """
        session = self._get_session_for_operation()
        async with self._session_scope(session) as session:
            # Query ChannelSubscription table for waiting instances
            result = await session.execute(
                select(ChannelSubscription).where(
                    and_(
                        ChannelSubscription.channel == channel,
                        ChannelSubscription.activity_id.isnot(None),  # Only waiting subscriptions
                    )
                )
            )
            subscriptions = result.scalars().all()
            return [
                {
                    "instance_id": sub.instance_id,
                    "channel": sub.channel,
                    "activity_id": sub.activity_id,
                    "timeout_at": sub.timeout_at.isoformat() if sub.timeout_at else None,
                    "created_at": sub.subscribed_at.isoformat() if sub.subscribed_at else None,
                }
                for sub in subscriptions
            ]

    async def remove_message_subscription(
        self,
        instance_id: str,
        channel: str,
    ) -> None:
        """
        Remove a message subscription.

        This method clears waiting state from the ChannelSubscription table.

        Args:
            instance_id: Workflow instance ID
            channel: Channel name
        """
        session = self._get_session_for_operation()
        async with self._session_scope(session) as session:
            # Clear waiting state from ChannelSubscription table
            # Don't delete the subscription - just clear the waiting state
            await session.execute(
                update(ChannelSubscription)
                .where(
                    and_(
                        ChannelSubscription.instance_id == instance_id,
                        ChannelSubscription.channel == channel,
                    )
                )
                .values(activity_id=None, timeout_at=None)
            )
            await self._commit_if_not_in_transaction(session)

    async def deliver_message(
        self,
        instance_id: str,
        channel: str,
        data: dict[str, Any] | bytes,
        metadata: dict[str, Any],
        worker_id: str | None = None,
    ) -> dict[str, Any] | None:
        """
        Deliver a message to a waiting workflow using Lock-First pattern.

        This method:
        1. Checks if there's a subscription for this instance/channel
        2. Acquires lock (Lock-First pattern) - if worker_id provided
        3. Records the message in history
        4. Removes the subscription
        5. Updates status to 'running'
        6. Releases lock

        Args:
            instance_id: Target workflow instance ID
            channel: Channel name
            data: Message payload (dict or bytes)
            metadata: Message metadata
            worker_id: Worker ID for locking. If None, skip locking (legacy mode).

        Returns:
            Dict with delivery info if successful, None otherwise
        """

        # Step 1: Check if subscription exists (without lock)
        session = self._get_session_for_operation()
        async with self._session_scope(session) as session:
            result = await session.execute(
                select(ChannelSubscription).where(
                    and_(
                        ChannelSubscription.instance_id == instance_id,
                        ChannelSubscription.channel == channel,
                        ChannelSubscription.activity_id.isnot(None),  # Only waiting subscriptions
                    )
                )
            )
            subscription = result.scalar_one_or_none()

            if subscription is None:
                return None

            activity_id = subscription.activity_id

        # Step 2: Acquire lock (Lock-First pattern) if worker_id provided
        lock_acquired = False
        if worker_id is not None:
            lock_acquired = await self.try_acquire_lock(instance_id, worker_id)
            if not lock_acquired:
                # Another worker is processing this workflow
                return None

        try:
            # Step 3-5: Deliver message atomically
            session = self._get_session_for_operation()
            async with self._session_scope(session) as session:
                # Re-check subscription (may have been removed by another worker)
                result = await session.execute(
                    select(ChannelSubscription).where(
                        and_(
                            ChannelSubscription.instance_id == instance_id,
                            ChannelSubscription.channel == channel,
                            ChannelSubscription.activity_id.isnot(
                                None
                            ),  # Only waiting subscriptions
                        )
                    )
                )
                subscription = result.scalar_one_or_none()

                if subscription is None:
                    # Already delivered by another worker
                    return None

                activity_id = subscription.activity_id

                # Get workflow info for return value
                instance_result = await session.execute(
                    select(WorkflowInstance).where(WorkflowInstance.instance_id == instance_id)
                )
                instance = instance_result.scalar_one_or_none()
                workflow_name = instance.workflow_name if instance else "unknown"

                # Build message data for history (JSON; bytes base64-encoded)
                from edda.channels import _message_history_fields

                message_id = str(uuid.uuid4())
                message_data = {
                    "id": message_id,
                    "channel": channel,
                    **_message_history_fields(data),
                    "metadata": metadata,
                }
                data_type = "json"
                event_data_json: str | None = json.dumps(message_data)
                event_data_binary = None

                # Record in history
                history_entry = WorkflowHistory(
                    instance_id=instance_id,
                    activity_id=activity_id,
                    event_type="ChannelMessageReceived",
                    data_type=data_type,
                    event_data=event_data_json,
                    event_data_binary=event_data_binary,
                )
                session.add(history_entry)

                # Clear waiting state from subscription (don't delete)
                await session.execute(
                    update(ChannelSubscription)
                    .where(
                        and_(
                            ChannelSubscription.instance_id == instance_id,
                            ChannelSubscription.channel == channel,
                        )
                    )
                    .values(activity_id=None, timeout_at=None)
                )

                # Update status to 'running' (ready for resumption)
                await session.execute(
                    update(WorkflowInstance)
                    .where(WorkflowInstance.instance_id == instance_id)
                    .values(status="running", updated_at=func.now())
                )

                await self._commit_if_not_in_transaction(session)

                return {
                    "instance_id": instance_id,
                    "workflow_name": workflow_name,
                    "activity_id": activity_id,
                }

        finally:
            # Step 6: Release lock if we acquired it
            if lock_acquired and worker_id is not None:
                await self.release_lock(instance_id, worker_id)

    async def find_expired_message_subscriptions(self) -> list[dict[str, Any]]:
        """
        Find all message subscriptions that have timed out.

        JOINs with WorkflowInstance to ensure instance exists and avoid N+1 queries.

        Returns:
            List of dicts with instance_id, channel, activity_id, timeout_at, created_at, workflow_name
        """
        session = self._get_session_for_operation()
        async with self._session_scope(session) as session:
            # Query ChannelSubscription table with JOIN
            result = await session.execute(
                select(
                    ChannelSubscription.instance_id,
                    ChannelSubscription.channel,
                    ChannelSubscription.activity_id,
                    ChannelSubscription.timeout_at,
                    ChannelSubscription.subscribed_at,
                    WorkflowInstance.workflow_name,
                )
                .join(
                    WorkflowInstance,
                    ChannelSubscription.instance_id == WorkflowInstance.instance_id,
                )
                .where(
                    and_(
                        ChannelSubscription.timeout_at.isnot(None),
                        ChannelSubscription.activity_id.isnot(None),  # Only waiting subscriptions
                        # Strictly '<': timestamps are kept to the second, so
                        # '<=' would fire up to a second before the deadline.
                        self._make_datetime_comparable(ChannelSubscription.timeout_at)
                        < self._get_current_time_expr(),
                        WorkflowInstance.framework == "python",
                    )
                )
            )
            rows = result.all()
            return [
                {
                    "instance_id": row[0],
                    "channel": row[1],
                    "activity_id": row[2],
                    "timeout_at": row[3],
                    "created_at": row[4],  # subscribed_at as created_at for compatibility
                    "workflow_name": row[5],
                }
                for row in rows
            ]

    # -------------------------------------------------------------------------
    # Group Membership Methods (Erlang pg style)
    # -------------------------------------------------------------------------

    async def join_group(self, instance_id: str, group_name: str) -> None:
        """
        Add a workflow instance to a group.

        Groups provide loose coupling for broadcast messaging.
        Idempotent - joining a group the instance is already in is a no-op.

        Args:
            instance_id: Workflow instance ID
            group_name: Group to join
        """
        session = self._get_session_for_operation()
        async with self._session_scope(session) as session:
            # Check if already a member (for idempotency)
            result = await session.execute(
                select(WorkflowGroupMembership).where(
                    and_(
                        WorkflowGroupMembership.instance_id == instance_id,
                        WorkflowGroupMembership.group_name == group_name,
                    )
                )
            )
            if result.scalar_one_or_none() is not None:
                # Already a member, nothing to do
                return

            # Add membership
            membership = WorkflowGroupMembership(
                instance_id=instance_id,
                group_name=group_name,
            )
            session.add(membership)
            await self._commit_if_not_in_transaction(session)

    async def leave_group(self, instance_id: str, group_name: str) -> None:
        """
        Remove a workflow instance from a group.

        Args:
            instance_id: Workflow instance ID
            group_name: Group to leave
        """
        session = self._get_session_for_operation()
        async with self._session_scope(session) as session:
            await session.execute(
                delete(WorkflowGroupMembership).where(
                    and_(
                        WorkflowGroupMembership.instance_id == instance_id,
                        WorkflowGroupMembership.group_name == group_name,
                    )
                )
            )
            await self._commit_if_not_in_transaction(session)

    async def get_group_members(self, group_name: str) -> list[str]:
        """
        Get all workflow instances in a group.

        Args:
            group_name: Group name

        Returns:
            List of instance IDs in the group
        """
        session = self._get_session_for_operation()
        async with self._session_scope(session) as session:
            result = await session.execute(
                select(WorkflowGroupMembership.instance_id).where(
                    WorkflowGroupMembership.group_name == group_name
                )
            )
            return [row[0] for row in result.fetchall()]

    async def leave_all_groups(self, instance_id: str) -> None:
        """
        Remove a workflow instance from all groups.

        Called automatically when a workflow completes or fails.

        Args:
            instance_id: Workflow instance ID
        """
        session = self._get_session_for_operation()
        async with self._session_scope(session) as session:
            await session.execute(
                delete(WorkflowGroupMembership).where(
                    WorkflowGroupMembership.instance_id == instance_id
                )
            )
            await self._commit_if_not_in_transaction(session)

    # -------------------------------------------------------------------------
    # Workflow Resumption Methods
    # -------------------------------------------------------------------------

    async def find_resumable_workflows(self, limit: int | None = None) -> list[dict[str, Any]]:
        """
        Find workflows that are ready to be resumed.

        Returns workflows with status='running' that don't have an active lock.
        Used for immediate resumption after message delivery.

        Args:
            limit: Optional maximum number of workflows to return.
                   If None, returns all resumable workflows.

        Returns:
            List of resumable workflows with instance_id and workflow_name.
        """
        session = self._get_session_for_operation()
        async with self._session_scope(session) as session:
            query = select(
                WorkflowInstance.instance_id,
                WorkflowInstance.workflow_name,
            ).where(
                and_(
                    WorkflowInstance.status.in_(("running", "recurred")),
                    WorkflowInstance.locked_by.is_(None),
                    WorkflowInstance.framework == "python",
                )
            )
            if limit is not None:
                query = query.limit(limit)
            result = await session.execute(query)
            return [
                {
                    "instance_id": row.instance_id,
                    "workflow_name": row.workflow_name,
                }
                for row in result.fetchall()
            ]

    async def claim_resumable_workflows(
        self,
        worker_id: str,
        limit: int | None = None,
        timeout_seconds: int = 300,
        workflow_names: Sequence[str] | None = None,
    ) -> list[dict[str, Any]]:
        """
        Atomically claim resumable workflows (running/recurred, unlocked).

        find_resumable_workflows + per-instance try_acquire_lock in one step:
        candidates are selected with FOR UPDATE SKIP LOCKED (PostgreSQL/MySQL)
        so concurrent resumption loops take disjoint batches, then claimed
        with the caller's lock identity. Each returned row is already locked
        by worker_id — the resumer must use that same identity for its writes
        and release.

        Args:
            worker_id: Lock identity to stamp on claimed rows (use an
                epoch-bearing identity so downstream writes are fenced)
            limit: Max workflows to claim (default 10)
            timeout_seconds: Default lock lease in seconds (an instance's
                own lock_timeout_seconds takes precedence, as in
                try_acquire_lock)
            workflow_names: Only instances of these workflows are claimed (the
                ones this worker has registered, so it never claims and
                releases instances it cannot run); None claims any. Edda
                resumes by name and does not compare source hashes.

        Returns:
            List of claimed workflows (instance_id, workflow_name,
            source_hash, status)
        """
        if limit is None:
            limit = 10
        if workflow_names is not None and not workflow_names:
            return []

        conditions = [
            WorkflowInstance.status.in_(("running", "recurred")),
            WorkflowInstance.locked_by.is_(None),
            WorkflowInstance.framework == "python",
        ]
        if workflow_names is not None:
            conditions.append(WorkflowInstance.workflow_name.in_(list(workflow_names)))

        session = self._get_session_for_operation(is_lock_operation=True)
        async with self._session_scope(session) as session:
            now = datetime.now(UTC)

            result = await session.execute(
                select(WorkflowInstance)
                .where(and_(*conditions))
                .limit(limit)
                .with_for_update(skip_locked=True)
            )
            rows = result.scalars().all()
            if not rows:
                await session.commit()
                return []

            # Claim row by row, re-checking the selection: SQLite's SELECT
            # locks nothing, so a lock taken since then must not be
            # overwritten. Only rows actually claimed are returned.
            claimed = []
            for r in rows:
                lease = int(
                    r.lock_timeout_seconds
                    if r.lock_timeout_seconds is not None
                    else timeout_seconds
                )
                res = await session.execute(
                    update(WorkflowInstance)
                    .where(
                        and_(
                            WorkflowInstance.instance_id == r.instance_id,
                            WorkflowInstance.status.in_(("running", "recurred")),
                            WorkflowInstance.locked_by.is_(None),
                        )
                    )
                    .values(
                        status="running",
                        locked_by=worker_id,
                        locked_at=now,
                        lock_expires_at=now + timedelta(seconds=lease),
                        updated_at=now,
                    )
                )
                if res.rowcount:  # type: ignore[attr-defined]
                    claimed.append(r)
            await session.commit()

            return [
                {
                    "instance_id": r.instance_id,
                    "workflow_name": r.workflow_name,
                    "source_hash": r.source_hash,
                    "status": r.status,
                }
                for r in claimed
            ]

    # -------------------------------------------------------------------------
    # Actor Registry / Monitor (Erlang register/whereis/monitor equivalents)
    # -------------------------------------------------------------------------

    async def register_actor_name(
        self, name: str, instance_id: str, framework: str = "python"
    ) -> None:
        """Bind a unique actor name to an instance.

        Idempotent for the holder; raises ValueError when the name is bound
        to another instance. The insert skips an existing name instead of
        raising a unique violation (which would abort an enclosing
        PostgreSQL transaction), and the binding is read back.
        """
        insert_sql = "INSERT INTO actor_names (name, instance_id, framework) VALUES (:n, :i, :f)"
        session = self._get_session_for_operation()
        async with self._session_scope(session) as session:
            params = {"n": name, "i": instance_id, "f": framework}
            if self.engine.dialect.name == "mysql":
                await session.execute(
                    text(f"{insert_sql} ON DUPLICATE KEY UPDATE name = name"), params
                )
            else:
                await session.execute(text(f"{insert_sql} ON CONFLICT (name) DO NOTHING"), params)
            holder = (
                await session.execute(
                    text("SELECT instance_id FROM actor_names WHERE name = :n"), {"n": name}
                )
            ).scalar_one_or_none()
            if holder != instance_id:
                from edda.exceptions import ActorNameTakenError

                raise ActorNameTakenError(f"actor name {name!r} is already registered to {holder}")
            await self._commit_if_not_in_transaction(session)

    async def unregister_actor_name(self, name: str) -> None:
        """Remove an actor name binding (idempotent)."""
        session = self._get_session_for_operation()
        async with self._session_scope(session) as session:
            await session.execute(text("DELETE FROM actor_names WHERE name = :n"), {"n": name})
            await self._commit_if_not_in_transaction(session)

    async def unregister_own_actor_name(self, name: str, owner: str) -> None:
        """Remove a name binding held by owner (idempotent).

        A name held by another instance raises NotOwnerError; an unbound
        name is a no-op.
        """
        session = self._get_session_for_operation()
        async with self._session_scope(session) as session:
            deleted = await session.execute(
                text("DELETE FROM actor_names WHERE name = :n AND instance_id = :o"),
                {"n": name, "o": owner},
            )
            if not getattr(deleted, "rowcount", 0):
                holder = (
                    await session.execute(
                        text("SELECT instance_id FROM actor_names WHERE name = :n"), {"n": name}
                    )
                ).scalar_one_or_none()
                if holder is not None:
                    from edda.exceptions import NotOwnerError

                    raise NotOwnerError(f"actor name {name!r} is held by {holder}")
            await self._commit_if_not_in_transaction(session)

    async def whereis_actor(self, name: str) -> str | None:
        """Resolve an actor name to an instance ID, or None if unbound."""
        session = self._get_session_for_operation()
        async with self._session_scope(session) as session:
            result = await session.execute(
                text("SELECT instance_id FROM actor_names WHERE name = :n"), {"n": name}
            )
            row = result.first()
            return row[0] if row else None

    async def monitor_actor(
        self, watcher_instance_id: str, target_instance_id: str, channel: str
    ) -> None:
        """Register that watcher is notified on channel when target terminates."""
        session = self._get_session_for_operation()
        async with self._session_scope(session) as session:
            await self._insert_unless_exists(
                session,
                "INSERT INTO actor_monitors (watcher_instance_id, target_instance_id, channel) "
                "VALUES (:w, :t, :c)",
                "watcher_instance_id, target_instance_id",
                {"w": watcher_instance_id, "t": target_instance_id, "c": channel},
            )
            await self._commit_if_not_in_transaction(session)

    async def unmonitor_actor(self, watcher_instance_id: str, target_instance_id: str) -> None:
        """Remove a monitor registration (idempotent)."""
        session = self._get_session_for_operation()
        async with self._session_scope(session) as session:
            await session.execute(
                text(
                    "DELETE FROM actor_monitors "
                    "WHERE watcher_instance_id = :w AND target_instance_id = :t"
                ),
                {"w": watcher_instance_id, "t": target_instance_id},
            )
            await self._commit_if_not_in_transaction(session)

    async def emit_actor_down_for_terminated(self) -> int:
        """
        Publish a DOWN message to each monitor channel whose target is in a
        terminal status and remove the monitor rows, then free the
        actor_names bindings of every terminated instance, monitored or not
        -- all in one transaction (leader sweep, safe to run repeatedly).

        A monitor row is deleted before its DOWN is published and only the
        delete that removed it publishes, so overlapping sweeps emit each
        DOWN exactly once. Returns the number of notifications emitted.
        """
        session = self._get_session_for_operation(is_lock_operation=True)
        async with self._session_scope(session) as session:
            result = await session.execute(
                text(
                    "SELECT m.id, m.target_instance_id, m.channel, w.status "
                    "FROM actor_monitors m "
                    "JOIN workflow_instances w ON w.instance_id = m.target_instance_id "
                    "WHERE w.status IN ('completed', 'failed', 'cancelled')"
                )
            )
            monitors = result.all()

            emitted = 0
            for m_id, target_id, channel, status in monitors:
                deleted = await session.execute(
                    text("DELETE FROM actor_monitors WHERE id = :i"), {"i": m_id}
                )
                if deleted.rowcount != 1:  # type: ignore[attr-defined]
                    continue  # another sweep emitted this DOWN
                payload = json.dumps({"type": "DOWN", "actor": target_id, "status": status})
                await session.execute(
                    text(
                        "INSERT INTO channel_messages (channel, message_id, data_type, data) "
                        "VALUES (:c, :m, 'json', :d)"
                    ),
                    {"c": channel, "m": str(uuid.uuid4()), "d": payload},
                )
                emitted += 1
            await session.execute(
                text(
                    "DELETE FROM actor_names WHERE instance_id IN ("
                    "SELECT instance_id FROM workflow_instances "
                    "WHERE status IN ('completed', 'failed', 'cancelled'))"
                )
            )
            await session.commit()
            return emitted

    # -------------------------------------------------------------------------
    # Capability Methods
    # -------------------------------------------------------------------------

    async def grant_capability(
        self,
        capability_id: str,
        kind: str,
        target: str,
        secret_hash: str,
        granted_by: str | None = None,
        expires_at: str | None = None,
    ) -> None:
        """Persist a capability grant (secret is stored only as a hash)."""
        expiry = _normalize_expiry(expires_at)
        session = self._get_session_for_operation()
        async with self._session_scope(session) as session:
            await session.execute(
                text(
                    "INSERT INTO actor_capabilities "
                    "(id, kind, target, secret_hash, granted_by, expires_at) "
                    "VALUES (:i, :k, :t, :h, :g, :e)"
                ),
                {
                    "i": capability_id,
                    "k": kind,
                    "t": target,
                    "h": secret_hash,
                    "g": granted_by,
                    "e": None if expiry is None else self._ts_param(expiry),
                },
            )
            await self._commit_if_not_in_transaction(session)

    async def revoke_capability(self, capability_id: str) -> None:
        """Remove a capability grant by ID (idempotent)."""
        session = self._get_session_for_operation()
        async with self._session_scope(session) as session:
            await session.execute(
                text("DELETE FROM actor_capabilities WHERE id = :i"), {"i": capability_id}
            )
            await self._commit_if_not_in_transaction(session)

    async def revoke_own_capability(self, capability_id: str, owner: str) -> None:
        """Remove a grant issued by owner (idempotent).

        A grant issued by another instance raises NotOwnerError; an unknown
        id is a no-op.
        """
        session = self._get_session_for_operation()
        async with self._session_scope(session) as session:
            deleted = await session.execute(
                text("DELETE FROM actor_capabilities WHERE id = :i AND granted_by = :o"),
                {"i": capability_id, "o": owner},
            )
            if not getattr(deleted, "rowcount", 0):
                exists = (
                    await session.execute(
                        text("SELECT 1 FROM actor_capabilities WHERE id = :i"), {"i": capability_id}
                    )
                ).first()
                if exists is not None:
                    from edda.exceptions import NotOwnerError

                    raise NotOwnerError(
                        f"capability {capability_id!r} was issued by another instance"
                    )
            await self._commit_if_not_in_transaction(session)

    async def has_capability_grants(self, kind: str, target: str) -> bool:
        """Return True if any grant exists for (kind, target)."""
        session = self._get_session_for_operation()
        async with self._session_scope(session) as session:
            result = await session.execute(
                text("SELECT COUNT(*) FROM actor_capabilities WHERE kind = :k AND target = :t"),
                {"k": kind, "t": target},
            )
            return bool(result.scalar_one() > 0)

    async def check_capability(self, kind: str, target: str, secret_hash: str) -> bool:
        """Return True if a non-expired grant exists for (kind, target, secret_hash)."""
        session = self._get_session_for_operation()
        async with self._session_scope(session) as session:
            result = await session.execute(
                text(
                    "SELECT expires_at FROM actor_capabilities "
                    "WHERE kind = :k AND target = :t AND secret_hash = :h"
                ),
                {"k": kind, "t": target, "h": secret_hash},
            )
            row = result.first()
            if not row:
                return False
            expires_at = row[0]
            if expires_at is None:
                return True
            if isinstance(expires_at, datetime):
                if expires_at.tzinfo is None:  # TIMESTAMP columns hold UTC
                    expires_at = expires_at.replace(tzinfo=UTC)
                expires_at = _canonical_ts(expires_at)
            return bool(str(expires_at) > _canonical_ts(datetime.now(UTC)))

    async def put_blob(self, hash: str, data: bytes, created_by: str | None = None) -> None:
        """Store data under its content hash (idempotent CAS write)."""
        session = self._get_session_for_operation()
        async with self._session_scope(session) as session:
            await self._insert_unless_exists(
                session,
                "INSERT INTO payload_blobs (hash, data, size_bytes, created_by) "
                "VALUES (:h, :d, :s, :c)",
                "hash",
                {"h": hash, "d": data, "s": len(data), "c": created_by},
            )
            await self._commit_if_not_in_transaction(session)

    async def get_blob(self, hash: str) -> bytes | None:
        """Return the blob for hash, or None if absent."""
        session = self._get_session_for_operation()
        async with self._session_scope(session) as session:
            result = await session.execute(
                text("SELECT data FROM payload_blobs WHERE hash = :h"), {"h": hash}
            )
            row = result.first()
            return bytes(row[0]) if row else None

    async def fs_get(self, path: str) -> dict[str, Any] | None:
        """Return the FS object at path (dict with data/version/owner), or None."""
        session = self._get_session_for_operation()
        async with self._session_scope(session) as session:
            result = await session.execute(
                text("SELECT data, version, owner FROM actor_fs WHERE path = :p"),
                {"p": path},
            )
            row = result.first()
            if not row:
                return None
            return {"data": bytes(row[0]), "version": row[1], "owner": row[2]}

    async def fs_put(self, path: str, data: bytes, owner: str, expected_version: int) -> None:
        """CAS write: expected 0 = create, >0 = update iff version matches.

        Raises FSConflictError on version mismatch or existing path.
        """
        from edda.exceptions import FSConflictError

        session = self._get_session_for_operation()
        async with self._session_scope(session) as session:
            if expected_version == 0:
                written = await self._insert_unless_exists(
                    session,
                    "INSERT INTO actor_fs (path, owner, data, version) VALUES (:p, :o, :d, 1)",
                    "path",
                    {"p": path, "o": owner, "d": data},
                )
            else:
                result = await session.execute(
                    text(
                        "UPDATE actor_fs SET data = :d, owner = :o, "
                        "version = version + 1, updated_at = :u "
                        "WHERE path = :p AND version = :v"
                    ),
                    {
                        "d": data,
                        "o": owner,
                        "p": path,
                        "v": expected_version,
                        "u": self._ts_param(),
                    },
                )
                written = getattr(result, "rowcount", 0) != 0
            if not written:
                raise FSConflictError(f"actor_fs version conflict: {path}")
            await self._commit_if_not_in_transaction(session)

    async def fs_delete(self, path: str, expected_version: int) -> None:
        """Delete path (expected 0 = unconditional, >0 = version-checked)."""
        from edda.exceptions import FSConflictError

        session = self._get_session_for_operation()
        async with self._session_scope(session) as session:
            if expected_version == 0:
                result = await session.execute(
                    text("DELETE FROM actor_fs WHERE path = :p"), {"p": path}
                )
            else:
                result = await session.execute(
                    text("DELETE FROM actor_fs WHERE path = :p AND version = :v"),
                    {"p": path, "v": expected_version},
                )
            if expected_version > 0 and getattr(result, "rowcount", 0) == 0:
                raise FSConflictError(f"actor_fs version conflict: {path}")
            await self._commit_if_not_in_transaction(session)

    async def fs_list(self, prefix: str) -> list[dict[str, Any]]:
        """Return FS objects (path/owner/version — no data) under prefix."""
        # MySQL string literals treat the backslash as an escape character.
        escape = "'\\\\'" if self.engine.dialect.name == "mysql" else "'\\'"
        session = self._get_session_for_operation()
        async with self._session_scope(session) as session:
            rows = (
                await session.execute(
                    text(
                        "SELECT path, owner, version FROM actor_fs "
                        f"WHERE path LIKE :p ESCAPE {escape} ORDER BY path"
                    ),
                    {"p": _like_escape(prefix) + "%"},
                )
            ).all()
            return [{"path": r[0], "owner": r[1], "version": r[2]} for r in rows]

    async def get_blob_info(self, hash: str) -> dict[str, Any] | None:
        """Return blob metadata (size_bytes/created_by/created_at), or None."""
        session = self._get_session_for_operation()
        async with self._session_scope(session) as session:
            result = await session.execute(
                text(
                    "SELECT size_bytes, created_by, created_at "
                    "FROM payload_blobs WHERE hash = :h"
                ),
                {"h": hash},
            )
            row = result.first()
            if not row:
                return None
            return {
                "size_bytes": row[0],
                "created_by": row[1],
                "created_at": row[2],
            }

    async def delete_expired_capabilities(self) -> int:
        """Remove expired capability grants, never unsealing a channel.

        A channel is sealed while any grant of the kind exists, so an expired
        grant goes only when another grant of the same (kind, target) is live
        or newer (created_at, then id); the newest expired grant stays. The
        derived table keeps MySQL from rejecting the self-referencing DELETE.
        """
        session = self._get_session_for_operation()
        async with self._session_scope(session) as session:
            result = await session.execute(
                text(
                    "DELETE FROM actor_capabilities WHERE id IN ("
                    "SELECT id FROM ("
                    "SELECT g.id AS id FROM actor_capabilities g "
                    "WHERE g.expires_at IS NOT NULL AND g.expires_at <= :now "
                    "AND EXISTS (SELECT 1 FROM actor_capabilities h "
                    "WHERE h.kind = g.kind AND h.target = g.target AND h.id <> g.id "
                    "AND (h.expires_at IS NULL OR h.expires_at > :now "
                    "OR h.created_at > g.created_at "
                    "OR (h.created_at = g.created_at AND h.id > g.id)))"
                    ") AS doomed)"
                ),
                {"now": self._ts_param()},
            )
            await self._commit_if_not_in_transaction(session)
            return int(getattr(result, "rowcount", 0) or 0)

    # -------------------------------------------------------------------------
    # Subscription Cleanup Methods (for recur())
    # -------------------------------------------------------------------------

    async def cleanup_instance_subscriptions(self, instance_id: str) -> None:
        """
        Remove all subscriptions for a workflow instance.

        Called during recur() to clean up event/timer/message subscriptions
        before archiving the history.

        Args:
            instance_id: Workflow instance ID to clean up
        """
        session = self._get_session_for_operation()
        async with self._session_scope(session) as session:
            # Remove timer subscriptions
            await session.execute(
                delete(WorkflowTimerSubscription).where(
                    WorkflowTimerSubscription.instance_id == instance_id
                )
            )

            # Remove channel subscriptions
            await session.execute(
                delete(ChannelSubscription).where(ChannelSubscription.instance_id == instance_id)
            )

            # Remove channel message claims
            await session.execute(
                delete(ChannelMessageClaim).where(ChannelMessageClaim.instance_id == instance_id)
            )

            await self._commit_if_not_in_transaction(session)

    # -------------------------------------------------------------------------
    # Channel-based Message Queue Methods
    # -------------------------------------------------------------------------

    async def publish_to_channel(
        self,
        channel: str,
        data: dict[str, Any] | bytes,
        metadata: dict[str, Any] | None = None,
    ) -> str:
        """
        Publish a message to a channel.

        Messages are persisted to channel_messages and available for subscribers.

        Args:
            channel: Channel name
            data: Message payload (dict or bytes)
            metadata: Optional message metadata

        Returns:
            Generated message_id (UUID)
        """

        message_id = str(uuid.uuid4())

        # Determine data type and serialize
        if isinstance(data, bytes):
            data_type = "binary"
            data_json = None
            data_binary = data
        else:
            data_type = "json"
            data_json = json.dumps(data)
            data_binary = None

        metadata_json = json.dumps(metadata) if metadata else None

        session = self._get_session_for_operation()
        async with self._session_scope(session) as session:
            msg = ChannelMessage(
                channel=channel,
                message_id=message_id,
                data_type=data_type,
                data=data_json,
                data_binary=data_binary,
                message_metadata=metadata_json,
            )
            session.add(msg)
            await self._commit_if_not_in_transaction(session)

        # Send NOTIFY for message published (unified channel name)
        await self._send_notify(
            "workflow_channel_message",
            {"ch": channel, "msg_id": message_id},
        )

        return message_id

    async def subscribe_to_channel(
        self,
        instance_id: str,
        channel: str,
        mode: str,
    ) -> None:
        """
        Subscribe a workflow instance to a channel.

        Args:
            instance_id: Workflow instance ID
            channel: Channel name
            mode: 'broadcast' or 'competing'

        Raises:
            ValueError: If mode is invalid
        """
        if mode not in ("broadcast", "competing"):
            raise ValueError(f"Invalid mode: {mode}. Must be 'broadcast' or 'competing'")

        session = self._get_session_for_operation()
        async with self._session_scope(session) as session:
            # Check if already subscribed
            result = await session.execute(
                select(ChannelSubscription).where(
                    and_(
                        ChannelSubscription.instance_id == instance_id,
                        ChannelSubscription.channel == channel,
                    )
                )
            )
            existing = result.scalar_one_or_none()

            if existing is not None:
                # Already subscribed, update mode if different
                if existing.mode != mode:
                    existing.mode = mode
                    await self._commit_if_not_in_transaction(session)
                return

            # For broadcast mode, set cursor to current max message id
            # So subscriber only sees messages published after subscription
            cursor_message_id = None
            if mode == "broadcast":
                result = await session.execute(
                    select(func.max(ChannelMessage.id)).where(ChannelMessage.channel == channel)
                )
                max_id = result.scalar()
                cursor_message_id = max_id if max_id is not None else 0

            # Create subscription
            subscription = ChannelSubscription(
                instance_id=instance_id,
                channel=channel,
                mode=mode,
                cursor_message_id=cursor_message_id,
            )
            session.add(subscription)
            await self._commit_if_not_in_transaction(session)

    async def unsubscribe_from_channel(
        self,
        instance_id: str,
        channel: str,
    ) -> None:
        """
        Unsubscribe a workflow instance from a channel.

        Args:
            instance_id: Workflow instance ID
            channel: Channel name
        """
        session = self._get_session_for_operation()
        async with self._session_scope(session) as session:
            await session.execute(
                delete(ChannelSubscription).where(
                    and_(
                        ChannelSubscription.instance_id == instance_id,
                        ChannelSubscription.channel == channel,
                    )
                )
            )
            await self._commit_if_not_in_transaction(session)

    async def get_channel_subscription(
        self,
        instance_id: str,
        channel: str,
    ) -> dict[str, Any] | None:
        """
        Get the subscription info for a workflow instance on a channel.

        Args:
            instance_id: Workflow instance ID
            channel: Channel name

        Returns:
            Subscription info dict with: mode, activity_id, cursor_message_id,
            timeout_at, or None if not subscribed
        """
        session = self._get_session_for_operation()
        async with self._session_scope(session) as session:
            result = await session.execute(
                select(ChannelSubscription).where(
                    and_(
                        ChannelSubscription.instance_id == instance_id,
                        ChannelSubscription.channel == channel,
                    )
                )
            )
            subscription = result.scalar_one_or_none()

            if subscription is None:
                return None

            return {
                "mode": subscription.mode,
                "activity_id": subscription.activity_id,
                "cursor_message_id": subscription.cursor_message_id,
                "timeout_at": subscription.timeout_at,
            }

    async def get_channel_mode(self, channel: str) -> str | None:
        """
        Get the mode for a channel (from any existing subscription).

        Args:
            channel: Channel name

        Returns:
            The mode ('broadcast' or 'competing') or None if no subscriptions exist
        """
        session = self._get_session_for_operation()
        async with self._session_scope(session) as session:
            result = await session.execute(
                select(ChannelSubscription.mode)
                .where(ChannelSubscription.channel == channel)
                .limit(1)
            )
            row = result.scalar_one_or_none()
            return row

    async def register_channel_receive_and_release_lock(
        self,
        instance_id: str,
        worker_id: str,
        channel: str,
        activity_id: str | None = None,
        timeout_seconds: float | None = None,
    ) -> bool:
        """
        Atomically register that workflow is waiting for channel message and release lock.

        Re-checks for pending messages inside the registration transaction:
        if a message is already deliverable, the wait is NOT registered and
        the instance stays running. This closes the missed-wakeup window
        between the caller pending check and the wait registration.

        Args:
            instance_id: Workflow instance ID
            worker_id: Worker ID that currently holds the lock
            channel: Channel name being waited on
            activity_id: Current activity ID to record
            timeout_seconds: Optional timeout in seconds for the message wait

        Returns:
            True if the wait was registered, False if a pending message was
            found inside the registration transaction (workflow left running,
            lock released, so the resumable sweep replays into it).

        Raises:
            RuntimeError: If the worker doesn't hold the lock
            InstanceTerminatedError: If the instance already terminated
            ValueError: If workflow is not subscribed to the channel
        """
        async with self.engine.begin() as conn:
            session = AsyncSession(bind=conn, expire_on_commit=False)

            # Verify subscription exists
            sub_result = await session.execute(
                select(ChannelSubscription).where(
                    and_(
                        ChannelSubscription.instance_id == instance_id,
                        ChannelSubscription.channel == channel,
                    )
                )
            )
            subscription: ChannelSubscription | None = sub_result.scalar_one_or_none()

            if subscription is None:
                raise ValueError(f"Instance {instance_id} is not subscribed to channel {channel}")

            # Re-check pending inside the tx. A publish committed between the
            # caller check and this point must not leave the workflow
            # sleeping on a message nobody will deliver again.
            pending_check = await self._has_pending_channel_message(
                session, instance_id, channel, subscription
            )

            # Fenced suspend + release first: only the lock holder of a live
            # instance may register the wait. With a pending message, only
            # the lock is released (status stays 'running' so the
            # resumable-workflow sweep replays into the message).
            current_time = datetime.now(UTC)
            values: dict[str, Any] = {
                "locked_by": None,
                "locked_at": None,
                "lock_expires_at": None,
                "updated_at": current_time,
            }
            if not pending_check:
                values.update(current_activity_id=activity_id, status="waiting_for_message")
            result = await session.execute(
                update(WorkflowInstance)
                .where(
                    and_(
                        WorkflowInstance.instance_id == instance_id,
                        WorkflowInstance.locked_by == worker_id,
                        WorkflowInstance.status.notin_(_TERMINAL_STATUSES),
                    )
                )
                .values(**values)
            )
            if (result.rowcount or 0) == 0:  # type: ignore[attr-defined]
                from edda.locking import InstanceTerminatedError

                refused = await self._fenced_write_refused(session, instance_id, worker_id)
                if isinstance(refused, InstanceTerminatedError):
                    raise refused
                raise RuntimeError(f"Worker {worker_id} does not hold lock for {instance_id}")
            if pending_check:
                await session.commit()
                return False

            # Only now mark the subscription as waiting
            subscription.activity_id = activity_id
            # Calculate timeout_at if timeout_seconds is provided
            if timeout_seconds is not None:
                subscription.timeout_at = current_time + timedelta(seconds=timeout_seconds)
            else:
                subscription.timeout_at = None

            await session.commit()
            return True

    async def _has_pending_channel_message(
        self,
        session: AsyncSession,
        instance_id: str,
        channel: str,
        subscription: ChannelSubscription,
    ) -> bool:
        """True if a deliverable message exists for this subscription.

        Must be called on the registration transaction session so the check
        and the wait registration are atomic.
        """
        if subscription.mode == "broadcast":
            cursor = subscription.cursor_message_id or 0
            result = await session.execute(
                select(ChannelMessage.id)
                .where(
                    and_(
                        ChannelMessage.channel == channel,
                        ChannelMessage.id > cursor,
                    )
                )
                .limit(1)
            )
            return result.scalar_one_or_none() is not None

        # competing: same visibility rule as get_pending_channel_messages,
        # a fresh claim by a different live instance hides the message.
        stale_before = datetime.now(UTC) - timedelta(seconds=CHANNEL_CLAIM_STALE_SECONDS)
        terminal_owners = select(WorkflowInstance.instance_id).where(
            WorkflowInstance.status.in_(["completed", "failed", "cancelled"])
        )
        hidden_claims = select(ChannelMessageClaim.message_id).where(
            and_(
                ChannelMessageClaim.instance_id != instance_id,
                ChannelMessageClaim.claimed_at >= stale_before,
                ChannelMessageClaim.instance_id.not_in(terminal_owners),
            )
        )
        result = await session.execute(
            select(ChannelMessage.id)
            .where(
                and_(
                    ChannelMessage.channel == channel,
                    ChannelMessage.message_id.not_in(hidden_claims),
                )
            )
            .limit(1)
        )
        return result.scalar_one_or_none() is not None

    async def requeue_channel_waits_with_pending(self) -> int:
        """Requeue 'waiting_for_message' workflows that have pending messages.

        Backstop for the residual missed-wakeup race: under read-committed
        isolation the waiting-subscriber scan of a publisher can still run
        before the wait registration commits. Any instance left waiting while
        a deliverable message exists is flipped back to running so the
        resumable sweep replays it.

        Returns:
            Number of workflows requeued.
        """
        session = self._get_session_for_operation()
        async with self._session_scope(session) as session:
            result = await session.execute(
                select(ChannelSubscription)
                .join(
                    WorkflowInstance,
                    WorkflowInstance.instance_id == ChannelSubscription.instance_id,
                )
                .where(
                    and_(
                        WorkflowInstance.status == "waiting_for_message",
                        WorkflowInstance.locked_by.is_(None),
                        WorkflowInstance.framework == "python",
                        ChannelSubscription.activity_id.isnot(None),
                    )
                )
            )
            waiting_subs = result.scalars().all()

            requeued = 0
            for sub in waiting_subs:
                if await self._has_pending_channel_message(
                    session, sub.instance_id, sub.channel, sub
                ):
                    # Conditional flip (the wait may have been resumed since
                    # the scan), and clear the wait it leaves: a stale
                    # activity_id/timeout_at would later fire a timeout into
                    # a run that has moved on.
                    flipped = await session.execute(
                        update(WorkflowInstance)
                        .where(
                            and_(
                                WorkflowInstance.instance_id == sub.instance_id,
                                WorkflowInstance.status == "waiting_for_message",
                                WorkflowInstance.locked_by.is_(None),
                            )
                        )
                        .values(status="running", updated_at=datetime.now(UTC))
                    )
                    if not flipped.rowcount:  # type: ignore[attr-defined]
                        continue
                    await session.execute(
                        update(ChannelSubscription)
                        .where(ChannelSubscription.instance_id == sub.instance_id)
                        .values(activity_id=None, timeout_at=None)
                    )
                    requeued += 1

            await self._commit_if_not_in_transaction(session)
            return requeued

    async def get_pending_channel_messages(
        self,
        instance_id: str,
        channel: str,
    ) -> list[dict[str, Any]]:
        """
        Get pending messages for a subscriber on a channel.

        For broadcast mode: messages with id > cursor_message_id
        For competing mode: unclaimed messages

        Args:
            instance_id: Workflow instance ID
            channel: Channel name

        Returns:
            List of pending messages
        """
        session = self._get_session_for_operation()
        async with self._session_scope(session) as session:
            # Get subscription info
            sub_result = await session.execute(
                select(ChannelSubscription).where(
                    and_(
                        ChannelSubscription.instance_id == instance_id,
                        ChannelSubscription.channel == channel,
                    )
                )
            )
            subscription = sub_result.scalar_one_or_none()

            if subscription is None:
                return []

            if subscription.mode == "broadcast":
                # Get messages after cursor
                cursor = subscription.cursor_message_id or 0
                msg_result = await session.execute(
                    select(ChannelMessage)
                    .where(
                        and_(
                            ChannelMessage.channel == channel,
                            ChannelMessage.id > cursor,
                        )
                    )
                    .order_by(ChannelMessage.published_at.asc())
                )
            else:  # competing
                # Hidden only when a *different live* instance holds a fresh
                # claim. Own claims stay visible (replay drain), and stale or
                # terminal-owner claims are reclaimable.
                stale_before = datetime.now(UTC) - timedelta(seconds=CHANNEL_CLAIM_STALE_SECONDS)
                terminal_owners = select(WorkflowInstance.instance_id).where(
                    WorkflowInstance.status.in_(["completed", "failed", "cancelled"])
                )
                hidden_claims = select(ChannelMessageClaim.message_id).where(
                    and_(
                        ChannelMessageClaim.instance_id != instance_id,
                        ChannelMessageClaim.claimed_at >= stale_before,
                        ChannelMessageClaim.instance_id.not_in(terminal_owners),
                    )
                )
                msg_result = await session.execute(
                    select(ChannelMessage)
                    .where(
                        and_(
                            ChannelMessage.channel == channel,
                            ChannelMessage.message_id.not_in(hidden_claims),
                        )
                    )
                    .order_by(ChannelMessage.published_at.asc())
                )

            messages = msg_result.scalars().all()
            return [
                {
                    "id": msg.id,
                    "message_id": msg.message_id,
                    "channel": msg.channel,
                    "data": (
                        msg.data_binary
                        if msg.data_type == "binary"
                        else json.loads(msg.data) if msg.data else {}
                    ),
                    "metadata": json.loads(msg.message_metadata) if msg.message_metadata else {},
                    "published_at": msg.published_at.isoformat() if msg.published_at else None,
                }
                for msg in messages
            ]

    async def claim_channel_message(
        self,
        message_id: str,
        instance_id: str,
    ) -> bool:
        """
        Claim a message for competing consumption.

        Succeeds when the message is unclaimed, already claimed by this
        instance (idempotent, refreshes claimed_at), or the existing claim is
        stale (claimed_at older than CHANNEL_CLAIM_STALE_SECONDS) or owned by
        a terminal workflow instance. This prevents an orphaned claim left by
        a crashed consumer from hiding the message forever.

        Args:
            message_id: Message UUID to claim
            instance_id: Workflow instance claiming the message

        Returns:
            True if claim succeeded, False if another live instance holds it
        """
        session = self._get_session_for_operation()
        async with self._session_scope(session) as session:
            # Lock the message row first. A concurrent claimer then waits for
            # this transaction and finds the message gone once it was
            # consumed, instead of waiting on an uncommitted claim and failing
            # its FK check (which aborts an enclosing PostgreSQL transaction;
            # MySQL deadlocks).
            if not await self._lock_message_row(session, message_id):
                await self._commit_if_not_in_transaction(session)
                return False

            now = datetime.now(UTC)
            stale_before = now - timedelta(seconds=CHANNEL_CLAIM_STALE_SECONDS)
            terminal_owners = select(WorkflowInstance.instance_id).where(
                WorkflowInstance.status.in_(["completed", "failed", "cancelled"])
            )

            # Takeover path: same instance (idempotent), stale, or dead owner
            result = await session.execute(
                update(ChannelMessageClaim)
                .where(
                    and_(
                        ChannelMessageClaim.message_id == message_id,
                        or_(
                            ChannelMessageClaim.instance_id == instance_id,
                            ChannelMessageClaim.claimed_at < stale_before,
                            ChannelMessageClaim.instance_id.in_(terminal_owners),
                        ),
                    )
                )
                .values(instance_id=instance_id, claimed_at=now)
            )
            if result.rowcount and result.rowcount > 0:  # type: ignore[attr-defined]
                await self._commit_if_not_in_transaction(session)
                return True

            # Fresh claim; an existing (live) claim keeps the message
            claimed = await self._insert_unless_exists(
                session,
                "INSERT INTO channel_message_claims (message_id, instance_id, claimed_at) "
                "VALUES (:m, :i, :c)",
                "message_id",
                {"m": message_id, "i": instance_id, "c": self._ts_param(now)},
            )
            await self._commit_if_not_in_transaction(session)
            return claimed

    async def _lock_message_row(self, session: AsyncSession, message_id: str) -> bool:
        """Lock a channel message row (PostgreSQL/MySQL: FOR UPDATE; SQLite
        serializes writers). Returns False when the message is gone."""
        stmt = select(ChannelMessage.message_id).where(ChannelMessage.message_id == message_id)
        if self.engine.dialect.name != "sqlite":
            stmt = stmt.with_for_update()
        return (await session.execute(stmt)).first() is not None

    async def delete_channel_message(self, message_id: str) -> None:
        """
        Delete a message from the channel queue.

        Args:
            message_id: Message ID to delete
        """
        session = self._get_session_for_operation()
        async with self._session_scope(session) as session:
            # Delete claim first (foreign key)
            await session.execute(
                delete(ChannelMessageClaim).where(ChannelMessageClaim.message_id == message_id)
            )
            # Delete message
            await session.execute(
                delete(ChannelMessage).where(ChannelMessage.message_id == message_id)
            )
            await self._commit_if_not_in_transaction(session)

    async def update_delivery_cursor(
        self,
        channel: str,
        instance_id: str,
        message_id: int,
    ) -> None:
        """
        Update the delivery cursor for broadcast mode.

        Args:
            channel: Channel name
            instance_id: Subscriber instance ID
            message_id: Last delivered message's internal ID
        """
        session = self._get_session_for_operation()
        async with self._session_scope(session) as session:
            # Update subscription cursor
            await session.execute(
                update(ChannelSubscription)
                .where(
                    and_(
                        ChannelSubscription.instance_id == instance_id,
                        ChannelSubscription.channel == channel,
                    )
                )
                .values(cursor_message_id=message_id)
            )
            await self._commit_if_not_in_transaction(session)

    async def get_channel_subscribers_waiting(
        self,
        channel: str,
    ) -> list[dict[str, Any]]:
        """
        Get channel subscribers that are waiting (activity_id is set).

        Args:
            channel: Channel name

        Returns:
            List of waiting subscribers
        """
        session = self._get_session_for_operation()
        async with self._session_scope(session) as session:
            result = await session.execute(
                select(ChannelSubscription).where(
                    and_(
                        ChannelSubscription.channel == channel,
                        ChannelSubscription.activity_id.isnot(None),
                    )
                )
            )
            subscriptions = result.scalars().all()
            return [
                {
                    "instance_id": sub.instance_id,
                    "channel": sub.channel,
                    "mode": sub.mode,
                    "activity_id": sub.activity_id,
                }
                for sub in subscriptions
            ]

    async def clear_channel_waiting_state(
        self,
        instance_id: str,
        channel: str,
    ) -> None:
        """
        Clear the waiting state for a channel subscription.

        Args:
            instance_id: Workflow instance ID
            channel: Channel name
        """
        session = self._get_session_for_operation()
        async with self._session_scope(session) as session:
            await session.execute(
                update(ChannelSubscription)
                .where(
                    and_(
                        ChannelSubscription.instance_id == instance_id,
                        ChannelSubscription.channel == channel,
                    )
                )
                .values(activity_id=None)
            )
            await self._commit_if_not_in_transaction(session)

    async def deliver_channel_message(
        self,
        instance_id: str,
        channel: str,
        message_id: str,
        data: dict[str, Any] | bytes,
        metadata: dict[str, Any],
        worker_id: str,
    ) -> dict[str, Any] | None:
        """
        Deliver a channel message to a waiting workflow.

        Uses Lock-First pattern for distributed safety.

        Args:
            instance_id: Target workflow instance ID
            channel: Channel name
            message_id: Message ID being delivered
            data: Message payload
            metadata: Message metadata
            worker_id: Worker ID for locking

        Returns:
            Delivery info if successful, None if failed
        """
        try:
            # Try to acquire lock
            if not await self.try_acquire_lock(instance_id, worker_id):
                logger.debug(f"Failed to acquire lock for {instance_id}")
                return None

            try:
                async with self.engine.begin() as conn:
                    session = AsyncSession(bind=conn, expire_on_commit=False)

                    # Get subscription info
                    result = await session.execute(
                        select(ChannelSubscription).where(
                            and_(
                                ChannelSubscription.instance_id == instance_id,
                                ChannelSubscription.channel == channel,
                            )
                        )
                    )
                    subscription = result.scalar_one_or_none()

                    if subscription is None or subscription.activity_id is None:
                        logger.debug(f"No waiting subscription for {instance_id} on {channel}")
                        return None

                    activity_id = subscription.activity_id

                    # Get instance info for return value
                    result = await session.execute(
                        select(WorkflowInstance.workflow_name).where(
                            WorkflowInstance.instance_id == instance_id
                        )
                    )
                    row = result.one_or_none()
                    if row is None:
                        return None
                    workflow_name = row[0]

                    # Competing: lock the message row before claiming it. A
                    # message another subscriber consumed meanwhile is gone,
                    # and nothing is delivered.
                    if subscription.mode != "broadcast" and not await self._lock_message_row(
                        session, message_id
                    ):
                        logger.debug(f"Message {message_id} already consumed; not delivered")
                        return None

                    # Deliver only into a registered wait: flip the instance
                    # out of waiting_for_message before writing anything. A
                    # subscriber that is not waiting (requeued, resumed,
                    # cancelled) keeps the message pending for its next
                    # receive instead of recording it under an id that no
                    # receive reads. The flip also proves this delivery holds
                    # the instance lock, in the transaction that records the
                    # message (claim, history and delete commit together).
                    current_time = datetime.now(UTC)
                    flipped = await session.execute(
                        update(WorkflowInstance)
                        .where(
                            and_(
                                WorkflowInstance.instance_id == instance_id,
                                WorkflowInstance.status == "waiting_for_message",
                                WorkflowInstance.locked_by == worker_id,
                            )
                        )
                        .values(
                            status="running",
                            updated_at=current_time,
                        )
                    )
                    if not flipped.rowcount:  # type: ignore[attr-defined]
                        logger.debug(f"{instance_id} is not waiting on {channel}; not delivered")
                        return None

                    # Prepare message data for history (JSON; bytes base64-encoded).
                    # Use "id" key to match what context.py expects when loading history
                    from edda.channels import _message_history_fields

                    message_result = {
                        "id": message_id,
                        "channel": channel,
                        **_message_history_fields(data),
                        "metadata": metadata,
                        "published_at": current_time.isoformat(),
                    }

                    # Record to history
                    history = WorkflowHistory(
                        instance_id=instance_id,
                        activity_id=activity_id,
                        event_type="ChannelMessageReceived",
                        data_type="json",
                        event_data=json.dumps(message_result),
                        event_data_binary=None,
                    )
                    session.add(history)

                    # Handle mode-specific logic
                    if subscription.mode == "broadcast":
                        # Get message internal id to update cursor
                        result = await session.execute(
                            select(ChannelMessage.id).where(ChannelMessage.message_id == message_id)
                        )
                        msg_row = result.one_or_none()
                        if msg_row:
                            subscription.cursor_message_id = msg_row[0]
                    else:  # competing
                        # Claim and delete the message
                        claim = ChannelMessageClaim(
                            message_id=message_id,
                            instance_id=instance_id,
                        )
                        session.add(claim)

                        # Delete the message (competing mode consumes it)
                        await session.execute(
                            delete(ChannelMessage).where(ChannelMessage.message_id == message_id)
                        )

                    # Clear waiting state
                    subscription.activity_id = None

                    await session.commit()

                # Send NOTIFY for workflow resumable
                await self._send_notify(
                    "workflow_resumable",
                    {"wf_id": instance_id, "wf_name": workflow_name},
                )

                return {
                    "instance_id": instance_id,
                    "workflow_name": workflow_name,
                    "activity_id": activity_id,
                }

            finally:
                # Always release lock
                await self.release_lock(instance_id, worker_id)

        except Exception as e:
            logger.error(f"Error delivering channel message: {e}")
            return None

    async def cleanup_old_channel_messages(self, older_than_days: int = 7) -> int:
        """
        Clean up old messages from channel queues.

        Args:
            older_than_days: Message retention period in days

        Returns:
            Number of messages deleted
        """
        cutoff_time = datetime.now(UTC) - timedelta(days=older_than_days)

        session = self._get_session_for_operation()
        async with self._session_scope(session) as session:
            # First delete claims for old messages
            await session.execute(
                delete(ChannelMessageClaim).where(
                    ChannelMessageClaim.message_id.in_(
                        select(ChannelMessage.message_id).where(
                            self._make_datetime_comparable(ChannelMessage.published_at)
                            < self._get_current_time_expr()
                        )
                    )
                )
            )

            # Delete old messages
            result = await session.execute(
                delete(ChannelMessage).where(ChannelMessage.published_at < cutoff_time)
            )
            await self._commit_if_not_in_transaction(session)

            # MySQL doesn't support DELETE ... RETURNING, so use rowcount.
            # rowcount is reliable across PostgreSQL/MySQL/SQLite for plain DELETE.
            return result.rowcount or 0  # type: ignore[attr-defined]

    def _task_notice(self, session: Any, channel: str, data: dict[str, Any]) -> str:
        """Append an event to {channel}:tasks on the caller's session —
        joins an enclosing transaction when present. Returns message_id;
        the NOTIFY must be sent after commit."""
        msg = ChannelMessage(
            channel=f"{channel}:tasks",
            message_id=str(uuid.uuid4()),
            data_type="json",
            data=json.dumps(data),
        )
        session.add(msg)
        return msg.message_id

    async def _notify_task(self, channel: str, message_id: str) -> None:
        """Post-commit NOTIFY for a task event."""
        await self._send_notify(
            "workflow_channel_message",
            {"ch": f"{channel}:tasks", "msg_id": message_id},
        )

    async def post_task(self, task_id: str, channel: str, data: str) -> None:
        """Insert a new open task on a channel + "task_posted" notice."""
        session = self._get_session_for_operation()
        async with self._session_scope(session) as session:
            await session.execute(
                text(
                    "INSERT INTO actor_tasks (id, channel, data, state) "
                    "VALUES (:i, :c, :d, 'open')"
                ),
                {"i": task_id, "c": channel, "d": data},
            )
            notice_id = self._task_notice(
                session,
                channel,
                {"type": "task_posted", "task_id": task_id, "channel": channel},
            )
            await self._commit_if_not_in_transaction(session)
        await self._notify_task(channel, notice_id)

    async def get_task(self, task_id: str) -> dict[str, Any] | None:
        """Return the task by ID, or None."""
        session = self._get_session_for_operation()
        async with self._session_scope(session) as session:
            row = (
                await session.execute(
                    text(
                        "SELECT id, channel, data, state, claimed_by, result, attempts "
                        "FROM actor_tasks WHERE id = :i"
                    ),
                    {"i": task_id},
                )
            ).first()
            if row is None:
                return None
            return {
                "id": row[0],
                "channel": row[1],
                "data": row[2],
                "state": row[3],
                "claimed_by": row[4],
                "result": row[5],
                "attempts": row[6],
            }

    async def reclaim_stale_tasks(self, max_age_seconds: int) -> int:
        """Return 'claimed' tasks older than max_age_seconds to 'open'.

        Orphan reclaim: a claimant that crashed mid-task leaves the row in
        'claimed' forever; the leader sweep calls this so the task becomes
        claimable again.
        """
        cutoff = datetime.now(UTC) - timedelta(seconds=max_age_seconds)
        session = self._get_session_for_operation()
        async with self._session_scope(session) as session:
            res = await session.execute(
                text(
                    "UPDATE actor_tasks SET state = 'open', claimed_by = NULL, "
                    "claimed_at = NULL, updated_at = :u "
                    "WHERE state = 'claimed' AND claimed_at < :c"
                ),
                {"u": self._ts_param(), "c": self._ts_param(cutoff, tz=True)},
            )
            await self._commit_if_not_in_transaction(session)
            return res.rowcount or 0  # type: ignore[attr-defined]

    async def claim_task(self, channel: str, claimant: str) -> dict[str, Any] | None:
        """Atomically claim the oldest open task on channel for claimant."""
        session = self._get_session_for_operation()
        async with self._session_scope(session) as session:
            for _ in range(3):
                task_id = (
                    await session.execute(
                        text(
                            "SELECT id FROM actor_tasks WHERE channel = :c AND state = 'open' "
                            "ORDER BY created_at, id LIMIT 1"
                        ),
                        {"c": channel},
                    )
                ).scalar_one_or_none()
                if task_id is None:
                    break
                # claimed_at is the TIMESTAMPTZ column on PostgreSQL and
                # updated_at a TIMESTAMP, so they take separate binds.
                result = await session.execute(
                    text(
                        "UPDATE actor_tasks SET state = 'claimed', claimed_by = :w, "
                        "claimed_at = :ca, attempts = attempts + 1, updated_at = :u "
                        "WHERE id = :i AND state = 'open'"
                    ),
                    {
                        "w": claimant,
                        "ca": self._ts_param(tz=True),
                        "u": self._ts_param(),
                        "i": task_id,
                    },
                )
                if (result.rowcount or 0) == 0:  # type: ignore[attr-defined]
                    continue  # another claimant won this task; take the next
                row = (
                    await session.execute(
                        text("SELECT id, channel, data, claimed_by FROM actor_tasks WHERE id = :i"),
                        {"i": task_id},
                    )
                ).first()
                if row is None:
                    continue
                notice_id = self._task_notice(
                    session,
                    channel,
                    {
                        "type": "task_claimed",
                        "task_id": row[0],
                        "claimed_by": claimant,
                    },
                )
                await self._commit_if_not_in_transaction(session)
                await self._notify_task(channel, notice_id)
                return {
                    "id": row[0],
                    "channel": row[1],
                    "data": row[2],
                    "state": "claimed",
                    "claimed_by": row[3],
                }
            await self._commit_if_not_in_transaction(session)
            return None

    async def complete_task(
        self, task_id: str, claimant: str, result: str | None, failed: bool
    ) -> None:
        """Mark a claimed task done/failed; TaskConflictError if not claimant.

        Idempotent: settling a task again into the same state by the same
        claimant succeeds without a second notice. TaskNotFoundError when no
        task has that ID.
        """
        from edda.exceptions import TaskConflictError, TaskNotFoundError

        state = "failed" if failed else "done"
        session = self._get_session_for_operation()
        async with self._session_scope(session) as session:
            res = await session.execute(
                text(
                    "UPDATE actor_tasks SET state = :s, result = :r, updated_at = :u "
                    "WHERE id = :i AND claimed_by = :w AND state = 'claimed'"
                ),
                {
                    "s": state,
                    "r": result,
                    "u": self._ts_param(),
                    "i": task_id,
                    "w": claimant,
                },
            )
            if (res.rowcount or 0) == 0:  # type: ignore[attr-defined]
                current = (
                    await session.execute(
                        text("SELECT state, claimed_by FROM actor_tasks WHERE id = :i"),
                        {"i": task_id},
                    )
                ).first()
                if current is None:
                    raise TaskNotFoundError(f"actor_tasks: no task {task_id}")
                if tuple(current) == (state, claimant):
                    return  # already settled by this claimant
                raise TaskConflictError(f"actor_tasks conflict: {task_id}")
            channel = (
                await session.execute(
                    text("SELECT channel FROM actor_tasks WHERE id = :i"),
                    {"i": task_id},
                )
            ).scalar_one()
            notice_id = self._task_notice(
                session,
                channel,
                {"type": "task_state", "task_id": task_id, "state": state},
            )
            await self._commit_if_not_in_transaction(session)
        await self._notify_task(channel, notice_id)

    async def list_tasks(self, channel: str, state: str | None = None) -> list[dict[str, Any]]:
        """Return tasks on channel filtered by state (None = all)."""
        query = (
            "SELECT id, channel, data, state, claimed_by, result, attempts "
            "FROM actor_tasks WHERE channel = :c"
        )
        params: dict[str, Any] = {"c": channel}
        if state:
            query += " AND state = :s"
            params["s"] = state
        query += " ORDER BY created_at"
        session = self._get_session_for_operation()
        async with self._session_scope(session) as session:
            rows = (await session.execute(text(query), params)).all()
            return [
                {
                    "id": r[0],
                    "channel": r[1],
                    "data": r[2],
                    "state": r[3],
                    "claimed_by": r[4],
                    "result": r[5],
                    "attempts": r[6],
                }
                for r in rows
            ]

    async def channel_messages_since(
        self, channel: str, message_id: str | None
    ) -> list[dict[str, Any]]:
        """Return messages on channel after message_id cursor (None/"" = all).

        A cursor with no message of that id on this channel raises
        CursorNotFoundError instead of returning the whole channel.
        """
        session = self._get_session_for_operation()
        async with self._session_scope(session) as session:
            after_id = 0
            if message_id:
                cursor = (
                    await session.execute(
                        text(
                            "SELECT id FROM channel_messages WHERE message_id = :m AND channel = :c"
                        ),
                        {"m": message_id, "c": channel},
                    )
                ).scalar_one_or_none()
                if cursor is None:
                    from edda.exceptions import CursorNotFoundError

                    raise CursorNotFoundError(
                        f"message {message_id!r} is not on channel {channel!r}"
                    )
                after_id = int(cursor)
            return await self._channel_messages_after(session, channel, after_id)

    async def channel_messages_after(self, channel: str, after_id: int) -> list[dict[str, Any]]:
        """Return messages on channel with a row id above after_id (0 = all), by id."""
        session = self._get_session_for_operation()
        async with self._session_scope(session) as session:
            return await self._channel_messages_after(session, channel, after_id)

    async def _channel_messages_after(
        self, session: AsyncSession, channel: str, after_id: int
    ) -> list[dict[str, Any]]:
        rows = (
            await session.execute(
                text(
                    "SELECT id, message_id, channel, data_type, data, data_binary, "
                    "metadata, published_at FROM channel_messages "
                    "WHERE channel = :c AND id > :a ORDER BY id"
                ),
                {"c": channel, "a": after_id},
            )
        ).all()
        return [
            {
                "id": r[0],
                "message_id": r[1],
                "channel": r[2],
                "data_type": r[3],
                "data": r[4] if r[3] == "json" else r[5],
                "metadata": json.loads(r[6]) if r[6] else {},
                "published_at": r[7],
            }
            for r in rows
        ]
