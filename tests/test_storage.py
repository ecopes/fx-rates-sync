from __future__ import annotations

import sqlite3
from datetime import date, timedelta
from decimal import Decimal
from pathlib import Path

import pytest

from fx_sync.errors import MigrationError, StorageError
from fx_sync.migrations import Migration, load_migrations, migrate, split_statements
from fx_sync.models import DateRange, RateRow, Rejection, RunCounters, RunStatus
from fx_sync.sqlite_repo import SqliteRepository
from tests.conftest import NOW

D1, D2, D3 = date(2025, 3, 10), date(2025, 3, 11), date(2025, 3, 12)


def row(day: date, rate: str, quote: str = "EUR") -> RateRow:
    return RateRow(base="USD", quote=quote, rate_date=day, rate=Decimal(rate))


def test_commit_chunk_insert_update_unchanged(repo: SqliteRepository) -> None:
    first = repo.commit_chunk(1, [row(D1, "0.9"), row(D2, "0.91")], [], ("USD:EUR", D2), NOW)
    assert (first.inserted, first.updated, first.unchanged) == (2, 0, 0)
    again = repo.commit_chunk(1, [row(D1, "0.90"), row(D2, "0.95"), row(D3, "0.96")], [], None, NOW)
    assert (again.inserted, again.updated, again.unchanged) == (1, 1, 1)
    assert again.changed == {("EUR", D2), ("EUR", D3)}
    assert repo.get_watermark("USD:EUR") == D2
    assert repo.get_watermark("other") is None
    assert repo.get_rate("USD", "EUR", D2, exact=True) == row(D2, "0.95")


def test_exact_decimal_roundtrip(repo: SqliteRepository) -> None:
    repo.commit_chunk(1, [row(D1, "0.123456789012")], [], None, NOW)
    got = repo.get_rate("USD", "EUR", D1)
    assert got is not None and got.rate == Decimal("0.123456789012")


def test_commit_is_atomic(repo: SqliteRepository) -> None:
    bad = Rejection(DateRange(D1, D2), "x", "{}")
    # run_id 999 violates the FK of rejected_payloads -> the whole chunk must roll back
    with pytest.raises(StorageError):
        repo.commit_chunk(999, [row(D1, "0.9")], [bad], ("s", D2), NOW)
    assert repo.get_rate("USD", "EUR", D1) is None
    assert repo.get_watermark("s") is None
    assert repo.snapshot().rate_count == 0


def test_queries(repo: SqliteRepository) -> None:
    rows = [row(D1, "0.9"), row(D2, "0.91"), row(D3, "0.92"), row(D3, "0.8", "GBP")]
    repo.commit_chunk(1, rows, [], None, NOW)
    assert repo.get_rate("USD", "EUR", date(2025, 3, 15)) == row(D3, "0.92")  # fallback
    assert repo.get_rate("USD", "EUR", date(2025, 3, 15), exact=True) is None
    assert repo.get_rate("USD", "EUR", date(2025, 3, 1)) is None
    series = repo.get_series("USD", "EUR", DateRange(D1, D3))
    assert [r.rate_date for r in series] == [D1, D2, D3]
    limited = repo.get_series("USD", "EUR", DateRange(D1, D3), limit=2)
    assert [r.rate_date for r in limited] == [D2, D3]
    assert {r.quote for r in repo.get_latest("USD")} == {"EUR", "GBP"}
    assert [r.quote for r in repo.get_latest("USD", ("gbp",))] == ["GBP"]
    assert repo.get_latest("CHF") == []
    assert repo.previous_rates("USD", D3) == {"EUR": Decimal("0.91")}


def test_lock_lifecycle(repo: SqliteRepository) -> None:
    assert repo.acquire_lock("l", "a", 60, NOW)
    assert not repo.acquire_lock("l", "b", 60, NOW + timedelta(seconds=30))
    assert repo.renew_lock("l", "a", 60, NOW + timedelta(seconds=30))
    assert not repo.renew_lock("l", "b", 60, NOW)
    assert not repo.acquire_lock("l", "b", 60, NOW + timedelta(seconds=80))
    assert repo.acquire_lock("l", "b", 60, NOW + timedelta(seconds=100))  # expired -> stolen
    repo.release_lock("l", "a")  # not the owner: no effect
    assert not repo.acquire_lock("l", "c", 60, NOW + timedelta(seconds=101))
    repo.release_lock("l", "b")
    assert repo.acquire_lock("l", "c", 60, NOW + timedelta(seconds=101))


def test_runs_and_snapshot(repo: SqliteRepository) -> None:
    empty = repo.snapshot()
    assert empty.rate_count == 0 and empty.last_run is None and empty.first_date is None
    run = repo.start_run("sync", "USD:EUR", NOW)
    assert repo.snapshot().last_run is not None
    repo.finish_run(run, RunStatus.SUCCESS, RunCounters(1, 0, 2, 0, 1, 0), NOW, None)
    rejection = Rejection(DateRange(D1, D2), "why", "{}", D1)
    repo.commit_chunk(run, [row(D1, "0.9")], [rejection], ("USD:EUR", D1), NOW)
    snap = repo.snapshot()
    assert snap.run_count == 1 and snap.rejected_count == 1 and snap.rate_count == 1
    assert snap.last_success is not None and snap.last_success.counters.inserted == 2
    assert snap.first_date == snap.last_date == D1
    assert [w.series for w in snap.watermarks] == ["USD:EUR"]


def test_file_database_persists_and_uses_wal(db_path: Path) -> None:
    repo = SqliteRepository(db_path)
    repo.commit_chunk(1, [row(D1, "0.9")], [], None, NOW)
    repo.close()
    reopened = SqliteRepository(db_path)
    assert reopened.get_rate("USD", "EUR", D1, exact=True) is not None
    mode = reopened._conn.execute("PRAGMA journal_mode").fetchone()[0]
    reopened.close()
    assert mode == "wal"


def test_unopenable_database_is_a_storage_error(tmp_path: Path) -> None:
    with pytest.raises(StorageError, match="cannot open"):
        SqliteRepository(tmp_path / "missing-dir" / "x.db")


def test_closed_connection_surfaces_storage_error() -> None:
    repo = SqliteRepository(":memory:")
    repo.close()
    with pytest.raises(StorageError):
        repo.get_watermark("x")


def test_migrations_are_idempotent_and_checksummed() -> None:
    conn = sqlite3.connect(":memory:", isolation_level=None)
    migrations = load_migrations()
    assert migrate(conn, migrations) == [1]
    assert migrate(conn, migrations) == []
    tampered = [Migration(1, migrations[0].name, migrations[0].sql + "\n-- edited")]
    with pytest.raises(MigrationError, match="changed after being applied"):
        migrate(conn, tampered)


def test_failed_migration_rolls_back() -> None:
    conn = sqlite3.connect(":memory:", isolation_level=None)
    bad = [Migration(1, "bad", "CREATE TABLE a (x); CREATE TABLE a (y);")]
    with pytest.raises(MigrationError, match="failed"):
        migrate(conn, bad)
    assert conn.execute("SELECT COUNT(*) FROM schema_migrations").fetchone()[0] == 0
    assert conn.execute("SELECT name FROM sqlite_master WHERE name = 'a'").fetchone() is None


def test_split_statements() -> None:
    sql = "-- c\nCREATE TABLE t (a TEXT DEFAULT ';');\n\nINSERT INTO t VALUES ('x');\n-- end\n"
    assert len(list(split_statements(sql))) == 2
    with pytest.raises(MigrationError, match="incomplete"):
        list(split_statements("CREATE TABLE t (a"))


def test_duplicate_migration_versions_rejected(tmp_path: Path) -> None:
    (tmp_path / "0001_a.sql").write_text("SELECT 1;")
    (tmp_path / "0001_b.sql").write_text("SELECT 2;")
    (tmp_path / "ignore.txt").write_text("x")
    with pytest.raises(MigrationError, match="duplicate"):
        load_migrations(tmp_path)
