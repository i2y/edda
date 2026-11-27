"""find_migrations_dir looks next to the package, never in the current directory."""

from pathlib import Path

import edda
from edda.storage.migrations import find_migrations_dir


def test_lookup_ignores_a_schema_in_the_current_directory(tmp_path, monkeypatch):
    foreign = tmp_path / "schema" / "db" / "migrations" / "sqlite"
    foreign.mkdir(parents=True)
    (foreign / "20990101000000_foreign.sql").write_text("-- migrate:up\nSELECT 1;\n")
    monkeypatch.chdir(tmp_path)

    found = find_migrations_dir()

    package_dir = Path(edda.__file__).resolve().parent
    assert found in (
        package_dir / "migrations",
        package_dir.parent / "schema" / "db" / "migrations",
    )
