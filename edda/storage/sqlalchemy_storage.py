"""
SQLAlchemy storage implementation for Edda framework.

This module provides a SQLAlchemy-based implementation of the StorageProtocol,
supporting SQLite, PostgreSQL, and MySQL with database-based exclusive control
and transactional outbox pattern.
"""

import json
import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from contextvars import ContextVar
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

from sqlalchemy import (
    CheckConstraint,
    Column,
    DateTime,
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
    or_,
    select,
    text,
    update,
)
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession
from sqlalchemy.orm import declarative_base

logger = logging.getLogger(__name__)

# Declarative base for ORM models
Base = declarative_base()


# ============================================================================
# SQLAlchemy ORM Models
# ============================================================================


class SchemaVersion(Base):  # type: ignore[valid-type, misc]
    """Schema version tracking."""

    __tablename__ = "schema_version"

    version = Column(Integer, primary_key=True)
    applied_at = Column(DateTime(timezone=True), nullable=False, server_default=func.now())
    description = Column(Text, nullable=False)


class WorkflowDefinition(Base):  # type: ignore[valid-type, misc]
    """Workflow definition (source code storage)."""

    __tablename__ = "workflow_definitions"

    workflow_name = Column(String(255), nullable=False, primary_key=True)
    source_hash = Column(String(64), nullable=False, primary_key=True)
    source_code = Column(Text, nullable=False)
    created_at = Column(DateTime(timezone=True), nullable=False, server_default=func.now())

    __table_args__ = (
        Index("idx_definitions_name", "workflow_name"),
        Index("idx_definitions_hash", "source_hash"),
    )


class WorkflowInstance(Base):  # type: ignore[valid-type, misc]
    """Workflow instance with distributed locking support."""

    __tablename__ = "workflow_instances"

    instance_id = Column(String(255), primary_key=True)
    workflow_name = Column(String(255), nullable=False)
    source_hash = Column(String(64), nullable=False)
    owner_service = Column(String(255), nullable=False)
    status = Column(
        String(50),
        nullable=False,
        server_default=text("'running'"),
    )
    current_activity_id = Column(String(255), nullable=True)
    started_at = Column(DateTime(timezone=True), nullable=False, server_default=func.now())
    updated_at = Column(
        DateTime(timezone=True), nullable=False, server_default=func.now(), onupdate=func.now()
    )
    input_data = Column(Text, nullable=False)  # JSON
    output_data = Column(Text, nullable=True)  # JSON
    locked_by = Column(String(255), nullable=True)
    locked_at = Column(DateTime(timezone=True), nullable=True)
    lock_timeout_seconds = Column(Integer, nullable=True)  # None = use global default (300s)
    lock_expires_at = Column(DateTime(timezone=True), nullable=True)  # Absolute expiry time

    __table_args__ = (
        ForeignKeyConstraint(
            ["workflow_name", "source_hash"],
            ["workflow_definitions.workflow_name", "workflow_definitions.source_hash"],
        ),
        CheckConstraint(
            "status IN ('running', 'completed', 'failed', 'waiting_for_event', "
            "'waiting_for_timer', 'compensating', 'cancelled')",
            name="valid_status",
        ),
        Index("idx_instances_status", "status"),
        Index("idx_instances_workflow", "workflow_name"),
        Index("idx_instances_owner", "owner_service"),
        Index("idx_instances_locked", "locked_by", "locked_at"),
        Index("idx_instances_lock_expires", "lock_expires_at"),
        Index("idx_instances_updated", "updated_at"),
        Index("idx_instances_hash", "source_hash"),
    )


class WorkflowHistory(Base):  # type: ignore[valid-type, misc]
    """Workflow execution history (for deterministic replay)."""

    __tablename__ = "workflow_history"

    id = Column(Integer, primary_key=True, autoincrement=True)
    instance_id = Column(
        String(255),
        nullable=False,
    )
    activity_id = Column(String(255), nullable=False)
    event_type = Column(String(100), nullable=False)
    data_type = Column(String(10), nullable=False)  # 'json' or 'binary'
    event_data = Column(Text, nullable=True)  # JSON (when data_type='json')
    event_data_binary = Column(LargeBinary, nullable=True)  # Binary (when data_type='binary')
    created_at = Column(DateTime(timezone=True), nullable=False, server_default=func.now())

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


class WorkflowCompensation(Base):  # type: ignore[valid-type, misc]
    """Compensation transactions (LIFO stack for Saga pattern)."""

    __tablename__ = "workflow_compensations"

    id = Column(Integer, primary_key=True, autoincrement=True)
    instance_id = Column(
        String(255),
        nullable=False,
    )
    activity_id = Column(String(255), nullable=False)
    activity_name = Column(String(255), nullable=False)
    args = Column(Text, nullable=False)  # JSON
    created_at = Column(DateTime(timezone=True), nullable=False, server_default=func.now())

    __table_args__ = (
        ForeignKeyConstraint(
            ["instance_id"],
            ["workflow_instances.instance_id"],
            ondelete="CASCADE",
        ),
        Index("idx_compensations_instance", "instance_id", "created_at"),
    )


class WorkflowEventSubscription(Base):  # type: ignore[valid-type, misc]
    """Event subscriptions (for wait_event)."""

    __tablename__ = "workflow_event_subscriptions"

    id = Column(Integer, primary_key=True, autoincrement=True)
    instance_id = Column(
        String(255),
        nullable=False,
    )
    event_type = Column(String(255), nullable=False)
    activity_id = Column(String(255), nullable=True)
    timeout_at = Column(DateTime(timezone=True), nullable=True)
    created_at = Column(DateTime(timezone=True), nullable=False, server_default=func.now())

    __table_args__ = (
        ForeignKeyConstraint(
            ["instance_id"],
            ["workflow_instances.instance_id"],
            ondelete="CASCADE",
        ),
        UniqueConstraint("instance_id", "event_type", name="unique_instance_event"),
        Index("idx_subscriptions_event", "event_type"),
        Index("idx_subscriptions_timeout", "timeout_at"),
        Index("idx_subscriptions_instance", "instance_id"),
    )


class WorkflowTimerSubscription(Base):  # type: ignore[valid-type, misc]
    """Timer subscriptions (for wait_timer)."""

    __tablename__ = "workflow_timer_subscriptions"

    id = Column(Integer, primary_key=True, autoincrement=True)
    instance_id = Column(
        String(255),
        nullable=False,
    )
    timer_id = Column(String(255), nullable=False)
    expires_at = Column(DateTime(timezone=True), nullable=False)
    activity_id = Column(String(255), nullable=True)
    created_at = Column(DateTime(timezone=True), nullable=False, server_default=func.now())

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


class OutboxEvent(Base):  # type: ignore[valid-type, misc]
    """Transactional outbox pattern events."""

    __tablename__ = "outbox_events"

    event_id = Column(String(255), primary_key=True)
    event_type = Column(String(255), nullable=False)
    event_source = Column(String(255), nullable=False)
    data_type = Column(String(10), nullable=False)  # 'json' or 'binary'
    event_data = Column(Text, nullable=True)  # JSON (when data_type='json')
    event_data_binary = Column(LargeBinary, nullable=True)  # Binary (when data_type='binary')
    content_type = Column(String(100), nullable=False, server_default=text("'application/json'"))
    created_at = Column(DateTime(timezone=True), nullable=False, server_default=func.now())
    published_at = Column(DateTime(timezone=True), nullable=True)
    status = Column(String(50), nullable=False, server_default=text("'pending'"))
    retry_count = Column(Integer, nullable=False, server_default=text("0"))
    last_error = Column(Text, nullable=True)

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

    def __init__(self, engine: AsyncEngine):
        """
        Initialize SQLAlchemy storage.

        Args:
            engine: SQLAlchemy AsyncEngine instance
        """
        self.engine = engine

    async def initialize(self) -> None:
        """Initialize database connection and create tables."""
        # Create all tables and indexes
        async with self.engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)

        # Initialize schema version
        await self._initialize_schema_version()

    async def close(self) -> None:
        """Close database connection."""
        await self.engine.dispose()

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

    def _get_current_time_expr(self):
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

    def _make_datetime_comparable(self, column):
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
            >>> self._make_datetime_comparable(WorkflowEventSubscription.timeout_at)
            >>>     <= self._get_current_time_expr()
        """
        if self.engine.dialect.name == "sqlite":
            # SQLite: wrap in datetime() for proper comparison
            return func.datetime(column)
        else:
            # PostgreSQL/MySQL: column is already timezone-aware
            return column

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

        if ctx.depth == 1:
            # Top-level transaction - commit the session
            logger.debug("Committing top-level transaction")
            await ctx.session.commit()  # type: ignore[union-attr]
            await ctx.session.close()  # type: ignore[union-attr]
        else:
            # Nested transaction - commit the savepoint
            nested_tx = ctx.savepoint_stack.pop()
            await nested_tx.commit()
            logger.debug(f"Committed nested transaction (savepoint) at depth={ctx.depth}")

        ctx.depth -= 1

        if ctx.depth == 0:
            # All transactions completed - clear context
            _transaction_context.set(None)

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
                existing.source_code = source_code  # type: ignore[assignment]
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
    ) -> None:
        """Create a new workflow instance."""
        session = self._get_session_for_operation()
        async with self._session_scope(session) as session:
            instance = WorkflowInstance(
                instance_id=instance_id,
                workflow_name=workflow_name,
                source_hash=source_hash,
                owner_service=owner_service,
                input_data=json.dumps(input_data),
                lock_timeout_seconds=lock_timeout_seconds,
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
        status_filter: str | None = None,
    ) -> list[dict[str, Any]]:
        """List workflow instances with optional filtering."""
        session = self._get_session_for_operation()
        async with self._session_scope(session) as session:
            stmt = (
                select(WorkflowInstance, WorkflowDefinition.source_code)
                .join(
                    WorkflowDefinition,
                    and_(
                        WorkflowInstance.workflow_name == WorkflowDefinition.workflow_name,
                        WorkflowInstance.source_hash == WorkflowDefinition.source_hash,
                    ),
                )
                .order_by(WorkflowInstance.started_at.desc())
                .limit(limit)
            )

            if status_filter:
                stmt = stmt.where(WorkflowInstance.status == status_filter)

            result = await session.execute(stmt)
            rows = result.all()

            return [
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
            from datetime import UTC, datetime, timedelta

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
            actual_timeout = (
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
            from datetime import UTC, datetime

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
            from datetime import UTC, datetime, timedelta

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
            actual_timeout = (
                instance.lock_timeout_seconds
                if instance.lock_timeout_seconds is not None
                else timeout_seconds
            )

            # Calculate new expiry time
            lock_expires_at = current_time + timedelta(seconds=actual_timeout)  # type: ignore[arg-type]

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
            from datetime import UTC, datetime

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
                            "instance_id": instance.instance_id,
                            "workflow_name": instance.workflow_name,
                            "source_hash": instance.source_hash,
                            "status": instance.status,
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
                .order_by(WorkflowCompensation.created_at.desc())
            )
            rows = result.scalars().all()

            return [
                {
                    "id": row.id,
                    "instance_id": row.instance_id,
                    "activity_id": row.activity_id,
                    "activity_name": row.activity_name,
                    "args": json.loads(row.args),  # type: ignore[arg-type]
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
    # Event Subscription Methods (prefer external session for registration)
    # -------------------------------------------------------------------------

    async def add_event_subscription(
        self,
        instance_id: str,
        event_type: str,
        timeout_at: datetime | None = None,
    ) -> None:
        """Register an event wait subscription."""
        session = self._get_session_for_operation()
        async with self._session_scope(session) as session:
            subscription = WorkflowEventSubscription(
                instance_id=instance_id,
                event_type=event_type,
                timeout_at=timeout_at,
            )
            session.add(subscription)
            await self._commit_if_not_in_transaction(session)

    async def find_waiting_instances(self, event_type: str) -> list[dict[str, Any]]:
        """Find workflow instances waiting for a specific event type."""
        session = self._get_session_for_operation()
        async with self._session_scope(session) as session:
            result = await session.execute(
                select(WorkflowEventSubscription).where(
                    and_(
                        WorkflowEventSubscription.event_type == event_type,
                        or_(
                            WorkflowEventSubscription.timeout_at.is_(None),
                            self._make_datetime_comparable(WorkflowEventSubscription.timeout_at)
                            > self._get_current_time_expr(),
                        ),
                    )
                )
            )
            rows = result.scalars().all()

            return [
                {
                    "id": row.id,
                    "instance_id": row.instance_id,
                    "event_type": row.event_type,
                    "activity_id": row.activity_id,
                    "timeout_at": row.timeout_at.isoformat() if row.timeout_at else None,
                    "created_at": row.created_at.isoformat(),
                }
                for row in rows
            ]

    async def remove_event_subscription(
        self,
        instance_id: str,
        event_type: str,
    ) -> None:
        """Remove event subscription after the event is received."""
        session = self._get_session_for_operation()
        async with self._session_scope(session) as session:
            await session.execute(
                delete(WorkflowEventSubscription).where(
                    and_(
                        WorkflowEventSubscription.instance_id == instance_id,
                        WorkflowEventSubscription.event_type == event_type,
                    )
                )
            )
            await self._commit_if_not_in_transaction(session)

    async def cleanup_expired_subscriptions(self) -> int:
        """Clean up event subscriptions that have timed out."""
        session = self._get_session_for_operation()
        async with self._session_scope(session) as session:
            result = await session.execute(
                delete(WorkflowEventSubscription).where(
                    and_(
                        WorkflowEventSubscription.timeout_at.isnot(None),
                        self._make_datetime_comparable(WorkflowEventSubscription.timeout_at)
                        <= self._get_current_time_expr(),
                    )
                )
            )
            await self._commit_if_not_in_transaction(session)
            return result.rowcount or 0  # type: ignore[attr-defined]

    async def find_expired_event_subscriptions(self) -> list[dict[str, Any]]:
        """Find event subscriptions that have timed out."""
        session = self._get_session_for_operation()
        async with self._session_scope(session) as session:
            result = await session.execute(
                select(
                    WorkflowEventSubscription.instance_id,
                    WorkflowEventSubscription.event_type,
                    WorkflowEventSubscription.activity_id,
                    WorkflowEventSubscription.timeout_at,
                    WorkflowEventSubscription.created_at,
                ).where(
                    and_(
                        WorkflowEventSubscription.timeout_at.isnot(None),
                        self._make_datetime_comparable(WorkflowEventSubscription.timeout_at)
                        <= self._get_current_time_expr(),
                    )
                )
            )
            rows = result.all()

            return [
                {
                    "instance_id": row[0],
                    "event_type": row[1],
                    "activity_id": row[2],
                    "timeout_at": row[3].isoformat() if row[3] else None,
                    "created_at": row[4].isoformat() if row[4] else None,
                }
                for row in rows
            ]

    async def register_event_subscription_and_release_lock(
        self,
        instance_id: str,
        worker_id: str,
        event_type: str,
        timeout_at: datetime | None = None,
        activity_id: str | None = None,
    ) -> None:
        """
        Atomically register event subscription and release workflow lock.

        This performs THREE operations in a SINGLE transaction:
        1. Register event subscription
        2. Update current activity
        3. Release lock

        This ensures distributed coroutines work correctly - when a workflow
        calls wait_event(), the subscription is registered and lock is released
        atomically, so ANY worker can resume the workflow when the event arrives.

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

            # 2. Register event subscription (INSERT OR REPLACE equivalent)
            # First delete existing
            await session.execute(
                delete(WorkflowEventSubscription).where(
                    and_(
                        WorkflowEventSubscription.instance_id == instance_id,
                        WorkflowEventSubscription.event_type == event_type,
                    )
                )
            )

            # Then insert new
            subscription = WorkflowEventSubscription(
                instance_id=instance_id,
                event_type=event_type,
                activity_id=activity_id,
                timeout_at=timeout_at,
            )
            session.add(subscription)

            # 3. Update current activity (if provided)
            if activity_id is not None:
                await session.execute(
                    update(WorkflowInstance)
                    .where(WorkflowInstance.instance_id == instance_id)
                    .values(current_activity_id=activity_id, updated_at=func.now())
                )

            # 4. Release lock
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
                    updated_at=func.now(),
                )
            )

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
        """Find timer subscriptions that have expired."""
        session = self._get_session_for_operation()
        async with self._session_scope(session) as session:
            result = await session.execute(
                select(
                    WorkflowTimerSubscription.instance_id,
                    WorkflowTimerSubscription.timer_id,
                    WorkflowTimerSubscription.expires_at,
                    WorkflowTimerSubscription.activity_id,
                    WorkflowInstance.workflow_name,
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
            from datetime import UTC, datetime, timedelta

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
        2. Updates status to 'cancelled' if allowed
        3. Clears locks
        4. Records cancellation metadata
        5. Removes event subscriptions (if waiting for event)
        6. Removes timer subscriptions (if waiting for timer)

        Args:
            instance_id: Workflow instance to cancel
            cancelled_by: Who/what triggered the cancellation

        Returns:
            True if successfully cancelled, False otherwise

        Note: Uses LOCK operation session (separate from external session).
        """
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
            if current_status not in (
                "running",
                "waiting_for_event",
                "waiting_for_timer",
                "compensating",
            ):
                # Already completed, failed, or cancelled
                return False

            # Update status to cancelled and record metadata
            from datetime import UTC, datetime

            cancellation_metadata = {
                "cancelled_by": cancelled_by,
                "cancelled_at": datetime.now(UTC).isoformat(),
                "previous_status": current_status,
            }

            await session.execute(
                update(WorkflowInstance)
                .where(WorkflowInstance.instance_id == instance_id)
                .values(
                    status="cancelled",
                    output_data=json.dumps(cancellation_metadata),
                    locked_by=None,
                    locked_at=None,
                    updated_at=func.now(),
                )
            )

            # Remove event subscriptions if waiting for event
            if current_status == "waiting_for_event":
                await session.execute(
                    delete(WorkflowEventSubscription).where(
                        WorkflowEventSubscription.instance_id == instance_id
                    )
                )

            # Remove timer subscriptions if waiting for timer
            if current_status == "waiting_for_timer":
                await session.execute(
                    delete(WorkflowTimerSubscription).where(
                        WorkflowTimerSubscription.instance_id == instance_id
                    )
                )

            return True
