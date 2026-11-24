"""Cross-repo schema conformance: every checkout's sqlite migrations must
produce the same shared tables/columns.

Catches hand-copied migration drift — e.g. a dialect-specific column type
copied into the wrong schema file, or a migration added in one repo but
not propagated to the others.

Sources compared:
  - romancy internal/migrations (embedded Go source)
  - romancy schema/ submodule
  - edda    schema/ submodule
  - shikibu schema/ submodule
  - kairo   schema/ submodule (this repo)
"""

from __future__ import annotations

import re
import sqlite3
from pathlib import Path

import pytest

BASE = Path(__file__).parent.parent.parent

SOURCES = {
    "romancy-internal": BASE / "romancy/internal/migrations/sql/sqlite",
    "romancy-schema": BASE / "romancy/schema/db/migrations/sqlite",
    "edda": BASE / "edda/schema/db/migrations/sqlite",
    "shikibu": BASE / "shikibu/schema/db/migrations/sqlite",
    "kairo": BASE / "kairo/schema/db/migrations/sqlite",
}

SHARED_TABLES = [
    "actor_names",
    "actor_monitors",
    "actor_capabilities",
    "payload_blobs",
    "actor_fs",
]


def extract_up(sql: str) -> str:
    """Return the `-- migrate:up` section of a dbmate-style migration."""
    m = re.search(r"-- migrate:up(.*?)-- migrate:down", sql, re.DOTALL)
    return m.group(1) if m else sql


def build_schema(migration_dir: Path) -> dict[str, dict[str, str]]:
    """Apply all sqlite migrations to an in-memory DB; return
    {table: {column: normalized_type}}."""
    if not migration_dir.is_dir():
        return {}
    db = sqlite3.connect(":memory:")
    try:
        for f in sorted(migration_dir.glob("*.sql")):
            up = extract_up(f.read_text())
            if up.strip():
                db.executescript(up)
        schema: dict[str, dict[str, str]] = {}
        rows = db.execute(
            "SELECT name FROM sqlite_master WHERE type='table' "
            "AND name NOT LIKE 'sqlite_%' AND name != 'schema_migrations'"
        ).fetchall()
        for (table,) in rows:
            cols = db.execute(f"PRAGMA table_info({table})").fetchall()
            schema[table] = {c[1]: c[2].upper() for c in cols}
        return schema
    finally:
        db.close()


@pytest.fixture(scope="module")
def schemas() -> dict[str, dict[str, dict[str, str]]]:
    return {name: build_schema(path) for name, path in SOURCES.items()}


def test_all_sources_buildable(schemas):
    missing = [n for n, s in schemas.items() if not s]
    assert not missing, f"migration dirs missing/empty: {missing}"


@pytest.mark.parametrize("table", SHARED_TABLES)
def test_shared_table_columns_match(table, schemas):
    """Every source must define the shared table with identical columns."""
    ref_name, ref = None, None
    for name, schema in sorted(schemas.items()):
        cols = schema.get(table)
        assert cols is not None, f"{name}: table {table} missing"
        if ref is None:
            ref_name, ref = name, cols
        else:
            assert cols == ref, (
                f"{name} vs {ref_name}: {table} columns differ\n"
                f"  {ref_name}: {sorted(ref.items())}\n"
                f"  {name}: {sorted(cols.items())}"
            )


def test_shared_tables_present(schemas):
    """Each source defines every shared table (regression guard for
    forgetting to sync a migration into one checkout)."""
    for name, schema in schemas.items():
        missing = [t for t in SHARED_TABLES if t not in schema]
        assert not missing, f"{name} missing tables: {missing}"
