"""Minimal versioned SQL migration runner (forward-only, checksummed, one transaction each)."""

from __future__ import annotations

import hashlib
import re
import sqlite3
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import UTC, datetime
from importlib import resources
from importlib.resources.abc import Traversable

from fx_sync.errors import MigrationError

_NAME = re.compile(r"^(\d{4})_([a-z0-9_]+)\.sql$")

_BOOTSTRAP = """
CREATE TABLE IF NOT EXISTS schema_migrations (
    version    INTEGER PRIMARY KEY,
    name       TEXT NOT NULL,
    checksum   TEXT NOT NULL,
    applied_at TEXT NOT NULL
)
"""


@dataclass(frozen=True)
class Migration:
    version: int
    name: str
    sql: str

    @property
    def checksum(self) -> str:
        return hashlib.sha256(self.sql.encode()).hexdigest()


def load_migrations(directory: Traversable | None = None) -> list[Migration]:
    directory = directory or resources.files("fx_sync") / "sql"
    found: list[Migration] = []
    for entry in directory.iterdir():
        match = _NAME.match(entry.name)
        if match:
            found.append(Migration(int(match[1]), match[2], entry.read_text(encoding="utf-8")))
    found.sort(key=lambda m: m.version)
    versions = [m.version for m in found]
    if len(set(versions)) != len(versions):
        raise MigrationError("duplicate migration versions")
    return found


def split_statements(sql: str) -> Iterator[str]:
    """Split a script into statements using SQLite's own completeness check."""
    buffer = ""
    for line in sql.splitlines(keepends=True):
        buffer += line
        if sqlite3.complete_statement(buffer):
            statement = buffer.strip()
            buffer = ""
            if statement:
                yield statement
    if buffer.strip() and not all(
        ln.strip().startswith("--") or not ln.strip() for ln in buffer.splitlines()
    ):
        raise MigrationError("migration ends with an incomplete statement")


def migrate(conn: sqlite3.Connection, migrations: list[Migration] | None = None) -> list[int]:
    """Apply pending migrations; returns the versions applied. Expects autocommit mode."""
    migrations = load_migrations() if migrations is None else migrations
    conn.execute(_BOOTSTRAP)
    applied = {
        int(row[0]): str(row[1])
        for row in conn.execute("SELECT version, checksum FROM schema_migrations")
    }
    newly_applied: list[int] = []
    for migration in migrations:
        if migration.version in applied:
            if applied[migration.version] != migration.checksum:
                raise MigrationError(
                    f"migration {migration.version:04d}_{migration.name} "
                    "changed after being applied"
                )
            continue
        conn.execute("BEGIN IMMEDIATE")
        try:
            for statement in split_statements(migration.sql):
                conn.execute(statement)
            conn.execute(
                "INSERT INTO schema_migrations (version, name, checksum, applied_at)"
                " VALUES (?, ?, ?, ?)",
                (
                    migration.version,
                    migration.name,
                    migration.checksum,
                    datetime.now(UTC).isoformat(timespec="seconds"),
                ),
            )
        except sqlite3.Error as exc:
            conn.execute("ROLLBACK")
            raise MigrationError(
                f"migration {migration.version:04d}_{migration.name} failed: {exc}"
            ) from exc
        conn.execute("COMMIT")
        newly_applied.append(migration.version)
    return newly_applied
