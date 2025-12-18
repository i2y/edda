"""
Automatic migration support for dbmate-style SQL files.

This module provides functions to automatically apply dbmate migration files
at application startup, eliminating the need to manually run `dbmate up`.

The migration system is compatible with dbmate:
- Uses the same `schema_migrations` table for tracking applied migrations
- Reads the same `-- migrate:up` / `-- migrate:down` SQL format
- Supports SQLite, PostgreSQL, and MySQL
"""

from __future__ import annotations

import logging
import re
from pathlib import Path
from typing import TYPE_CHECKING

from sqlalchemy import text

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncEngine

logger = logging.getLogger(__name__)


def detect_db_type(engine: AsyncEngine) -> str:
    """
    Detect database type from engine URL.

    Args:
        engine: SQLAlchemy async engine

    Returns:
        One of 'sqlite', 'postgresql', 'mysql'

    Raises:
        ValueError: If database type cannot be detected
    """
    url = str(engine.url)

    if "sqlite" in url:
        return "sqlite"
    elif "postgresql" in url or "postgres" in url:
        return "postgresql"
    elif "mysql" in url:
        return "mysql"
    else:
        raise ValueError(f"Cannot detect database type from URL: {url}")


def find_migrations_dir() -> Path | None:
    """
    Auto-detect migrations directory.

    Searches in the following order:
    1. Package-bundled migrations (edda/migrations/)
    2. Development environment (schema/db/migrations/)

    Returns:
        Path to migrations directory or None if not found
    """
    # 1. Package-bundled migrations
    pkg_dir = Path(__file__).parent.parent / "migrations"
    if pkg_dir.exists():
        return pkg_dir

    # 2. Development environment (schema submodule)
    schema_dir = Path.cwd() / "schema" / "db" / "migrations"
    if schema_dir.exists():
        return schema_dir

    return None


async def ensure_schema_migrations_table(engine: AsyncEngine) -> None:
    """
    Create schema_migrations table if it doesn't exist.

    This table is compatible with dbmate's migration tracking.

    Args:
        engine: SQLAlchemy async engine
    """
    db_type = detect_db_type(engine)

    if db_type == "mysql":
        # MySQL uses VARCHAR with explicit length
        create_sql = """
            CREATE TABLE IF NOT EXISTS schema_migrations (
                version VARCHAR(255) PRIMARY KEY
            )
        """
    else:
        # SQLite and PostgreSQL
        create_sql = """
            CREATE TABLE IF NOT EXISTS schema_migrations (
                version VARCHAR(255) PRIMARY KEY
            )
        """

    async with engine.begin() as conn:
        await conn.execute(text(create_sql))


async def get_applied_migrations(engine: AsyncEngine) -> set[str]:
    """
    Get set of already applied migration versions.

    Args:
        engine: SQLAlchemy async engine

    Returns:
        Set of applied migration version strings (e.g., "20251217000000")
    """
    try:
        async with engine.connect() as conn:
            result = await conn.execute(text("SELECT version FROM schema_migrations"))
            return {row[0] for row in result.fetchall()}
    except Exception:
        # Table might not exist yet
        return set()


async def record_migration(engine: AsyncEngine, version: str) -> bool:
    """
    Record a migration as applied.

    Args:
        engine: SQLAlchemy async engine
        version: Migration version string (e.g., "20251217000000")

    Returns:
        True if recorded successfully, False if already recorded (race condition)
    """
    try:
        async with engine.begin() as conn:
            await conn.execute(
                text("INSERT INTO schema_migrations (version) VALUES (:version)"),
                {"version": version},
            )
        return True
    except Exception as e:
        # Handle race condition: another worker already recorded this migration
        err_msg = str(e).lower()
        if "unique constraint" in err_msg or "duplicate" in err_msg:
            return False
        raise


def extract_version_from_filename(filename: str) -> str:
    """
    Extract version from migration filename.

    Args:
        filename: Migration filename (e.g., "20251217000000_initial_schema.sql")

    Returns:
        Version string (e.g., "20251217000000")
    """
    # dbmate uses format: YYYYMMDDHHMMSS_description.sql
    match = re.match(r"^(\d+)_", filename)
    if match:
        return match.group(1)
    return filename.replace(".sql", "")


def parse_migration_file(content: str) -> tuple[str, str]:
    """
    Parse dbmate migration file content.

    Args:
        content: Full content of migration SQL file

    Returns:
        Tuple of (up_sql, down_sql)
    """
    # Extract "-- migrate:up" section
    up_match = re.search(
        r"-- migrate:up\s*(.*?)(?:-- migrate:down|$)",
        content,
        re.DOTALL,
    )
    up_sql = up_match.group(1).strip() if up_match else ""

    # Extract "-- migrate:down" section
    down_match = re.search(
        r"-- migrate:down\s*(.*?)$",
        content,
        re.DOTALL,
    )
    down_sql = down_match.group(1).strip() if down_match else ""

    return up_sql, down_sql


async def execute_sql_statements(
    engine: AsyncEngine,
    sql: str,
    db_type: str,
) -> None:
    """
    Execute SQL statements from migration file.

    Handles:
    - Multiple statements separated by semicolons
    - Comment lines
    - MySQL DELIMITER blocks (simplified handling)

    Args:
        engine: SQLAlchemy async engine
        sql: SQL string containing one or more statements
        db_type: Database type ('sqlite', 'postgresql', 'mysql')
    """
    if not sql:
        return

    # For MySQL, handle DELIMITER and stored procedures specially
    if db_type == "mysql" and "DELIMITER" in sql:
        # Skip procedure-based migrations for now, execute simpler parts
        sql = re.sub(
            r"DROP PROCEDURE.*?;|DELIMITER.*?DELIMITER ;",
            "",
            sql,
            flags=re.DOTALL,
        )

    # Split by semicolons and execute each statement
    statements = [s.strip() for s in sql.split(";") if s.strip()]

    async with engine.begin() as conn:
        for stmt in statements:
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

            try:
                await conn.execute(text(actual_sql))
            except Exception as e:
                # Log but continue - some statements might fail if objects exist
                err_msg = str(e).lower()
                if "already exists" not in err_msg and "duplicate" not in err_msg:
                    raise


async def apply_dbmate_migrations(
    engine: AsyncEngine,
    migrations_dir: Path | None = None,
) -> list[str]:
    """
    Apply dbmate migration files automatically.

    This function:
    1. Detects database type from engine URL
    2. Finds migration files for that database type
    3. Checks which migrations have already been applied
    4. Applies pending migrations in order
    5. Records applied migrations in schema_migrations table

    Args:
        engine: SQLAlchemy async engine
        migrations_dir: Base migrations directory (contains sqlite/, postgresql/, mysql/ subdirs).
                       If None, auto-detects from package or schema/ submodule.

    Returns:
        List of applied migration versions

    Raises:
        FileNotFoundError: If migrations directory not found
        ValueError: If database type cannot be detected
    """
    # Find migrations directory
    if migrations_dir is None:
        migrations_dir = find_migrations_dir()

    if migrations_dir is None:
        logger.warning(
            "No migrations directory found. "
            "Skipping automatic migration. "
            "Set migrations_dir parameter or use auto_migrate=False."
        )
        return []

    # Detect database type
    db_type = detect_db_type(engine)
    db_migrations_dir = migrations_dir / db_type

    if not db_migrations_dir.exists():
        logger.warning(
            f"Migrations directory for {db_type} not found: {db_migrations_dir}. "
            "Skipping automatic migration."
        )
        return []

    # Ensure schema_migrations table exists
    await ensure_schema_migrations_table(engine)

    # Get already applied migrations
    applied = await get_applied_migrations(engine)

    # Get all migration files sorted by name (timestamp order)
    migration_files = sorted(db_migrations_dir.glob("*.sql"))

    applied_versions: list[str] = []

    for migration_file in migration_files:
        version = extract_version_from_filename(migration_file.name)

        # Skip if already applied
        if version in applied:
            logger.debug(f"Migration {version} already applied, skipping")
            continue

        logger.info(f"Applying migration: {migration_file.name}")

        # Read and parse migration file
        content = migration_file.read_text()
        up_sql, _ = parse_migration_file(content)

        if not up_sql:
            logger.warning(f"No '-- migrate:up' section found in {migration_file.name}")
            continue

        # Execute migration
        try:
            await execute_sql_statements(engine, up_sql, db_type)

            # Record as applied (handles race condition with other workers)
            recorded = await record_migration(engine, version)
            if recorded:
                applied_versions.append(version)
                logger.info(f"Successfully applied migration: {version}")
            else:
                # Another worker already applied this migration
                logger.debug(f"Migration {version} was applied by another worker")
        except Exception as e:
            logger.error(f"Failed to apply migration {version}: {e}")
            raise

    if applied_versions:
        logger.info(f"Applied {len(applied_versions)} migration(s)")
    else:
        logger.debug("No pending migrations to apply")

    return applied_versions
