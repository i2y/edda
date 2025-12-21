"""
SQLAlchemy storage implementation for Edda framework.

This module provides a SQLAlchemy-based implementation of the StorageProtocol,
supporting SQLite, PostgreSQL, and MySQL with database-based exclusive control
and transactional outbox pattern.
"""

import json
import logging
import re
from collections.abc import AsyncIterator, Awaitable, Callable
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
    func,
    inspect,
    or_,
    select,
    text,
    update,
)
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

logger = logging.getLogger(__name__)


# Declarative base for ORM models
class Base(DeclarativeBase):
    pass


# ============================================================================
# SQLAlchemy ORM Models
# ============================================================================


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

    @property
    def _is_postgresql(self) -> bool:
        """Check if the database is PostgreSQL."""
        return self.engine.dialect.name == "postgresql"

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
            nested_tx = await ctx.session.begin_nested()  # type: ignore[union-attr]
            ctx.savepoint_stack.append(nested_tx)
            logger.debug(f"Created nested transaction (savepoint) at depth={ctx.depth}")

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
    ) -> None:
        """Create a new workflow instance."""
        session = self._get_session_for_operation()
        async with self._session_scope(session) as session:
            instance = WorkflowInstance(
                instance_id=instance_id,
                workflow_name=workflow_name,
                source_hash=source_hash,
                owner_service=owner_service,
                framework="python",
                input_data=json.dumps(input_data),
                lock_timeout_seconds=lock_timeout_seconds,
                continued_from=continued_from,
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
            }

    async def update_instance_status(
        self,
        instance_id: str,
        status: str,
        output_data: dict[str, Any] | None = None,
    ) -> None:
        """Update workflow instance status."""
        session = self._get_session_for_operation()
        async with self._session_scope(session) as session:
            stmt = (
                update(WorkflowInstance)
                .where(WorkflowInstance.instance_id == instance_id)
                .values(
                    status=status,
                    updated_at=func.now(),
                )
            )

            if output_data is not None:
                stmt = stmt.values(output_data=json.dumps(output_data))

            await session.execute(stmt)
            await self._commit_if_not_in_transaction(session)

    async def update_instance_activity(self, instance_id: str, activity_id: str) -> None:
        """Update the current activity ID for a workflow instance."""
        session = self._get_session_for_operation()
        async with self._session_scope(session) as session:
            await session.execute(
                update(WorkflowInstance)
                .where(WorkflowInstance.instance_id == instance_id)
                .values(current_activity_id=activity_id, updated_at=func.now())
            )
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
        Try to acquire lock using SELECT FOR UPDATE.

        This implements distributed locking with automatic stale lock detection.
        Returns True if lock was acquired, False if already locked by another worker.
        Can acquire locks that have timed out.

        Note: ALWAYS uses separate session (not external session).
        """
        session = self._get_session_for_operation(is_lock_operation=True)
        async with self._session_scope(session) as session:
            # Calculate timeout threshold and current time
            # Use UTC time consistently (timezone-aware to match DateTime(timezone=True) columns)
            current_time = datetime.now(UTC)

            # SELECT FOR UPDATE SKIP LOCKED to prevent blocking (PostgreSQL/MySQL)
            # SKIP LOCKED: If row is already locked, return None immediately (no blocking)
            result = await session.execute(
                select(WorkflowInstance)
                .where(WorkflowInstance.instance_id == instance_id)
                .with_for_update(skip_locked=True)
            )
            instance = result.scalar_one_or_none()

            if instance is None:
                # Instance doesn't exist
                await session.commit()
                return False

            # Determine actual timeout (priority: instance > parameter > default)
            actual_timeout = int(
                instance.lock_timeout_seconds
                if instance.lock_timeout_seconds is not None
                else timeout_seconds
            )

            # Check if we can acquire the lock
            # Lock is available if: not locked OR lock has expired
            # Note: SQLite stores datetime without timezone, add UTC timezone
            if instance.locked_by is None:
                can_acquire = True
            elif instance.lock_expires_at is not None:
                lock_expires_at_utc = (
                    instance.lock_expires_at.replace(tzinfo=UTC)
                    if instance.lock_expires_at.tzinfo is None
                    else instance.lock_expires_at
                )
                can_acquire = lock_expires_at_utc < current_time
            else:
                can_acquire = False

            # Debug logging
            logger.debug(
                f"Lock acquisition check: instance_id={instance_id}, "
                f"locked_by={instance.locked_by}, lock_expires_at={instance.lock_expires_at}, "
                f"current_time={current_time}, can_acquire={can_acquire}"
            )

            if not can_acquire:
                # Already locked by another worker
                logger.debug(f"Lock acquisition failed: already locked by {instance.locked_by}")
                await session.commit()
                return False

            # Acquire the lock and set expiry time
            lock_expires_at = current_time + timedelta(seconds=actual_timeout)
            await session.execute(
                update(WorkflowInstance)
                .where(WorkflowInstance.instance_id == instance_id)
                .values(
                    locked_by=worker_id,
                    locked_at=current_time,
                    lock_expires_at=lock_expires_at,
                    updated_at=current_time,
                )
            )

            await session.commit()
            return True

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
                if instance.status in ["running", "compensating"]:
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
    ) -> None:
        """Append an event to workflow execution history."""
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

            # 4. Update status to 'waiting_for_timer' and release lock
            await session.execute(
                update(WorkflowInstance)
                .where(
                    and_(
                        WorkflowInstance.instance_id == instance_id,
                        WorkflowInstance.locked_by == worker_id,
                    )
                )
                .values(
                    status="waiting_for_timer",
                    locked_by=None,
                    locked_at=None,
                    updated_at=func.now(),
                )
            )

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
            # 1. SELECT FOR UPDATE to lock rows (both 'pending' and 'failed' for retry)
            result = await session.execute(
                select(OutboxEvent)
                .where(OutboxEvent.status.in_(["pending", "failed"]))
                .order_by(OutboxEvent.created_at.asc())
                .limit(limit)
                .with_for_update(skip_locked=True)
            )
            rows = result.scalars().all()

            # 2. Mark as 'processing' to prevent duplicate fetches
            if rows:
                event_ids = [row.event_id for row in rows]
                await session.execute(
                    update(OutboxEvent)
                    .where(OutboxEvent.event_id.in_(event_ids))
                    .values(status="processing")
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
                .values(status="published", published_at=func.now())
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
            return result.rowcount or 0  # type: ignore[attr-defined]

    # -------------------------------------------------------------------------
    # Workflow Cancellation Methods
    # -------------------------------------------------------------------------

    async def cancel_instance(self, instance_id: str, cancelled_by: str) -> bool:
        """
        Cancel a workflow instance.

        Only running or waiting_for_event workflows can be cancelled.
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

        Returns:
            True if successfully cancelled, False otherwise

        Note: Uses LOCK operation session (separate from external session).
        """
        cancellable_statuses = (
            "running",
            "waiting_for_event",
            "waiting_for_timer",
            "waiting_for_message",
            "compensating",
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

            # Only allow cancellation of running, waiting, or compensating workflows
            # compensating workflows can be marked as cancelled after compensation completes
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

            # Remove timer subscriptions if waiting for timer
            if current_status == "waiting_for_timer":
                await session.execute(
                    delete(WorkflowTimerSubscription).where(
                        WorkflowTimerSubscription.instance_id == instance_id
                    )
                )

            # Clear channel subscriptions if waiting for event/message
            if current_status in ("waiting_for_event", "waiting_for_message"):
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
        import uuid

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

                # Build message data for history
                message_id = str(uuid.uuid4())
                message_data = {
                    "id": message_id,
                    "channel": channel,
                    "data": data if isinstance(data, dict) else None,
                    "metadata": metadata,
                }

                # Handle binary data
                if isinstance(data, bytes):
                    data_type = "binary"
                    event_data_json = None
                    event_data_binary = data
                else:
                    data_type = "json"
                    event_data_json = json.dumps(message_data)
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
                        self._make_datetime_comparable(ChannelSubscription.timeout_at)
                        <= self._get_current_time_expr(),
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
                    WorkflowInstance.status == "running",
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
        import uuid

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
            Subscription info dict with: mode, activity_id, cursor_message_id
            or None if not subscribed
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
        timeout_seconds: int | None = None,
    ) -> None:
        """
        Atomically register that workflow is waiting for channel message and release lock.

        Args:
            instance_id: Workflow instance ID
            worker_id: Worker ID that currently holds the lock
            channel: Channel name being waited on
            activity_id: Current activity ID to record
            timeout_seconds: Optional timeout in seconds for the message wait

        Raises:
            RuntimeError: If the worker doesn't hold the lock
            ValueError: If workflow is not subscribed to the channel
        """
        async with self.engine.begin() as conn:
            session = AsyncSession(bind=conn, expire_on_commit=False)

            # Verify lock ownership
            result = await session.execute(
                select(WorkflowInstance).where(WorkflowInstance.instance_id == instance_id)
            )
            instance = result.scalar_one_or_none()

            if instance is None:
                raise RuntimeError(f"Instance not found: {instance_id}")

            if instance.locked_by != worker_id:
                raise RuntimeError(
                    f"Worker {worker_id} does not hold lock for {instance_id}. "
                    f"Locked by: {instance.locked_by}"
                )

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

            # Update subscription to mark as waiting
            current_time = datetime.now(UTC)
            subscription.activity_id = activity_id
            # Calculate timeout_at if timeout_seconds is provided
            if timeout_seconds is not None:
                subscription.timeout_at = current_time + timedelta(seconds=timeout_seconds)
            else:
                subscription.timeout_at = None

            # Update instance: set activity, status, release lock
            await session.execute(
                update(WorkflowInstance)
                .where(WorkflowInstance.instance_id == instance_id)
                .values(
                    current_activity_id=activity_id,
                    status="waiting_for_message",
                    locked_by=None,
                    locked_at=None,
                    lock_expires_at=None,
                    updated_at=current_time,
                )
            )

            await session.commit()

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
                # Get unclaimed messages (not in channel_message_claims)
                subquery = select(ChannelMessageClaim.message_id)
                msg_result = await session.execute(
                    select(ChannelMessage)
                    .where(
                        and_(
                            ChannelMessage.channel == channel,
                            ChannelMessage.message_id.not_in(subquery),
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

        Uses INSERT with conflict check to ensure only one subscriber claims.

        Args:
            message_id: Message ID to claim
            instance_id: Workflow instance claiming the message

        Returns:
            True if claim succeeded, False if already claimed
        """
        session = self._get_session_for_operation()
        async with self._session_scope(session) as session:
            try:
                # Check if already claimed
                result = await session.execute(
                    select(ChannelMessageClaim).where(ChannelMessageClaim.message_id == message_id)
                )
                if result.scalar_one_or_none() is not None:
                    return False  # Already claimed

                claim = ChannelMessageClaim(
                    message_id=message_id,
                    instance_id=instance_id,
                )
                session.add(claim)
                await self._commit_if_not_in_transaction(session)
                return True
            except Exception:
                return False

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

                    # Prepare message data for history
                    # Use "id" key to match what context.py expects when loading history
                    current_time = datetime.now(UTC)
                    message_result = {
                        "id": message_id,
                        "channel": channel,
                        "data": data if isinstance(data, dict) else None,
                        "metadata": metadata,
                        "published_at": current_time.isoformat(),
                    }

                    # Record to history
                    if isinstance(data, bytes):
                        history = WorkflowHistory(
                            instance_id=instance_id,
                            activity_id=activity_id,
                            event_type="ChannelMessageReceived",
                            data_type="binary",
                            event_data=None,
                            event_data_binary=data,
                        )
                    else:
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

                    # Update instance status to running
                    current_time = datetime.now(UTC)
                    await session.execute(
                        update(WorkflowInstance)
                        .where(WorkflowInstance.instance_id == instance_id)
                        .values(
                            status="running",
                            updated_at=current_time,
                        )
                    )

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
                delete(ChannelMessage)
                .where(ChannelMessage.published_at < cutoff_time)
                .returning(ChannelMessage.id)
            )
            deleted_ids = result.fetchall()
            await self._commit_if_not_in_transaction(session)

            return len(deleted_ids)
