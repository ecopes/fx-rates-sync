"""SQLite implementation of :class:`~fx_sync.repository.Repository` using the stdlib driver."""

from __future__ import annotations

import sqlite3
from collections.abc import Iterator, Sequence
from contextlib import contextmanager
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any

from fx_sync.errors import StorageError
from fx_sync.migrations import migrate
from fx_sync.models import (
    DateRange,
    RateRow,
    Rejection,
    RunCounters,
    RunRecord,
    RunStatus,
    SnapshotWatermark,
    StatusSnapshot,
    UpsertResult,
)

_UPSERT = """
INSERT INTO fx_rates (base, quote, rate_date, rate, source_hash, created_at, updated_at)
VALUES (?, ?, ?, ?, ?, ?, ?)
ON CONFLICT (base, quote, rate_date) DO UPDATE SET
    rate = excluded.rate,
    source_hash = excluded.source_hash,
    updated_at = excluded.updated_at
WHERE fx_rates.source_hash <> excluded.source_hash
"""

_RUN_COLUMNS = (
    "id, mode, series_key, started_at, finished_at, status, chunks_total, chunks_failed,"
    " rows_inserted, rows_updated, rows_unchanged, rows_rejected, error"
)


def _ts(value: datetime) -> str:
    return value.astimezone(UTC).isoformat(timespec="seconds")


def _row_to_rate(row: sqlite3.Row | tuple[Any, ...]) -> RateRow:
    return RateRow(
        base=row[0], quote=row[1], rate_date=date.fromisoformat(row[2]), rate=Decimal(row[3])
    )


def _row_to_run(row: tuple[Any, ...]) -> RunRecord:
    return RunRecord(
        id=row[0],
        mode=row[1],
        series=row[2],
        started_at=datetime.fromisoformat(row[3]),
        finished_at=datetime.fromisoformat(row[4]) if row[4] else None,
        status=row[5],
        counters=RunCounters(*row[6:12]),
        error=row[12],
    )


class SqliteRepository:
    def __init__(self, path: str | Path, *, busy_timeout_ms: int = 5000) -> None:
        self._path = str(path)
        try:
            # Autocommit mode: transactions are explicit (BEGIN IMMEDIATE) and visible in the code.
            self._conn = sqlite3.connect(self._path, isolation_level=None, check_same_thread=False)
            self._conn.execute(f"PRAGMA busy_timeout = {int(busy_timeout_ms)}")
            self._conn.execute("PRAGMA foreign_keys = ON")
            if self._path != ":memory:":
                self._conn.execute("PRAGMA journal_mode = WAL")
            migrate(self._conn)
        except sqlite3.Error as exc:
            raise StorageError(f"cannot open database {self._path}: {exc}") from exc

    def close(self) -> None:
        self._conn.close()

    @contextmanager
    def _tx(self) -> Iterator[sqlite3.Connection]:
        try:
            self._conn.execute("BEGIN IMMEDIATE")
        except sqlite3.Error as exc:
            raise StorageError(f"cannot start transaction: {exc}") from exc
        try:
            yield self._conn
            self._conn.execute("COMMIT")
        except sqlite3.Error as exc:
            self._rollback()
            raise StorageError(f"database error: {exc}") from exc
        except BaseException:
            self._rollback()
            raise

    def _rollback(self) -> None:
        if self._conn.in_transaction:
            self._conn.execute("ROLLBACK")

    def _query(self, sql: str, params: Sequence[Any] = ()) -> list[tuple[Any, ...]]:
        try:
            return self._conn.execute(sql, params).fetchall()
        except sqlite3.Error as exc:
            raise StorageError(f"database error: {exc}") from exc

    def _write(self, sql: str, params: Sequence[Any] = ()) -> int:
        try:
            return self._conn.execute(sql, params).rowcount
        except sqlite3.Error as exc:
            raise StorageError(f"database error: {exc}") from exc

    # -- locking ---------------------------------------------------------------------------

    def acquire_lock(self, name: str, owner: str, ttl_seconds: int, now: datetime) -> bool:
        changed = self._write(
            "INSERT INTO locks (name, owner, acquired_at, expires_at) VALUES (?, ?, ?, ?)"
            " ON CONFLICT (name) DO UPDATE SET owner = excluded.owner,"
            " acquired_at = excluded.acquired_at, expires_at = excluded.expires_at"
            " WHERE locks.expires_at <= ?",
            (name, owner, _ts(now), _ts(now + timedelta(seconds=ttl_seconds)), _ts(now)),
        )
        return changed == 1

    def renew_lock(self, name: str, owner: str, ttl_seconds: int, now: datetime) -> bool:
        changed = self._write(
            "UPDATE locks SET expires_at = ? WHERE name = ? AND owner = ?",
            (_ts(now + timedelta(seconds=ttl_seconds)), name, owner),
        )
        return changed == 1

    def release_lock(self, name: str, owner: str) -> None:
        self._write("DELETE FROM locks WHERE name = ? AND owner = ?", (name, owner))

    # -- run log ---------------------------------------------------------------------------

    def start_run(self, mode: str, series: str, started_at: datetime) -> int:
        try:
            cursor = self._conn.execute(
                "INSERT INTO sync_runs (mode, series_key, started_at, status) VALUES (?, ?, ?, ?)",
                (mode, series, _ts(started_at), RunStatus.RUNNING.value),
            )
        except sqlite3.Error as exc:
            raise StorageError(f"database error: {exc}") from exc
        if cursor.lastrowid is None:  # pragma: no cover - INSERT always sets it
            raise StorageError("run id not generated")
        return cursor.lastrowid

    def finish_run(
        self,
        run_id: int,
        status: RunStatus,
        counters: RunCounters,
        finished_at: datetime,
        error: str | None,
    ) -> None:
        self._write(
            "UPDATE sync_runs SET finished_at = ?, status = ?, chunks_total = ?, chunks_failed = ?,"
            " rows_inserted = ?, rows_updated = ?, rows_unchanged = ?, rows_rejected = ?,"
            " error = ? WHERE id = ?",
            (
                _ts(finished_at),
                status.value,
                counters.chunks_total,
                counters.chunks_failed,
                counters.inserted,
                counters.updated,
                counters.unchanged,
                counters.rejected,
                error,
                run_id,
            ),
        )

    # -- sync writes -----------------------------------------------------------------------

    def get_watermark(self, series: str) -> date | None:
        rows = self._query("SELECT high_water_date FROM sync_state WHERE series_key = ?", (series,))
        return date.fromisoformat(rows[0][0]) if rows else None

    def previous_rates(self, base: str, before: date) -> dict[str, Decimal]:
        rows = self._query(
            "SELECT r.quote, r.rate FROM fx_rates r WHERE r.base = ? AND r.rate_date = ("
            " SELECT MAX(p.rate_date) FROM fx_rates p"
            " WHERE p.base = r.base AND p.quote = r.quote AND p.rate_date < ?)",
            (base, before.isoformat()),
        )
        return {quote: Decimal(rate) for quote, rate in rows}

    def commit_chunk(
        self,
        run_id: int,
        rows: Sequence[RateRow],
        rejections: Sequence[Rejection],
        watermark: tuple[str, date] | None,
        now: datetime,
    ) -> UpsertResult:
        stamp = _ts(now)
        with self._tx() as conn:
            existing: dict[tuple[str, str, str], str] = {}
            if rows:
                low = min(r.rate_date for r in rows).isoformat()
                high = max(r.rate_date for r in rows).isoformat()
                for base in {r.base for r in rows}:
                    for quote, day, digest in conn.execute(
                        "SELECT quote, rate_date, source_hash FROM fx_rates"
                        " WHERE base = ? AND rate_date BETWEEN ? AND ?",
                        (base, low, high),
                    ):
                        existing[(base, quote, day)] = digest

            inserted = updated = unchanged = 0
            changed: set[tuple[str, date]] = set()
            for row in rows:
                digest = row.source_hash
                previous = existing.get((row.base, row.quote, row.rate_date.isoformat()))
                if previous == digest:
                    unchanged += 1
                    continue
                if previous is None:
                    inserted += 1
                else:
                    updated += 1
                changed.add((row.quote, row.rate_date))
                conn.execute(
                    _UPSERT,
                    (
                        row.base,
                        row.quote,
                        row.rate_date.isoformat(),
                        row.rate_text,
                        digest,
                        stamp,
                        stamp,
                    ),
                )

            for rejection in rejections:
                conn.execute(
                    "INSERT INTO rejected_payloads (run_id, window_start, window_end, rate_date,"
                    " reason, payload, created_at) VALUES (?, ?, ?, ?, ?, ?, ?)",
                    (
                        run_id,
                        rejection.window.start.isoformat(),
                        rejection.window.end.isoformat(),
                        rejection.rate_date.isoformat() if rejection.rate_date else None,
                        rejection.reason,
                        rejection.payload,
                        stamp,
                    ),
                )

            if watermark is not None:
                conn.execute(
                    "INSERT INTO sync_state (series_key, high_water_date, updated_at)"
                    " VALUES (?, ?, ?) ON CONFLICT (series_key) DO UPDATE SET"
                    " high_water_date = excluded.high_water_date, updated_at = excluded.updated_at",
                    (watermark[0], watermark[1].isoformat(), stamp),
                )
        return UpsertResult(inserted, updated, unchanged, frozenset(changed))

    # -- reads -----------------------------------------------------------------------------

    def get_rate(self, base: str, quote: str, on: date, *, exact: bool = False) -> RateRow | None:
        operator = "=" if exact else "<="  # fixed choice of two literals, never user input
        rows = self._query(
            "SELECT base, quote, rate_date, rate FROM fx_rates"  # noqa: S608
            " WHERE base = ? AND quote = ? AND rate_date " + operator + " ?"
            " ORDER BY rate_date DESC LIMIT 1",
            (base, quote, on.isoformat()),
        )
        return _row_to_rate(rows[0]) if rows else None

    def get_series(
        self, base: str, quote: str, window: DateRange, limit: int | None = None
    ) -> list[RateRow]:
        rows = self._query(
            "SELECT base, quote, rate_date, rate FROM fx_rates"
            " WHERE base = ? AND quote = ? AND rate_date BETWEEN ? AND ?"
            " ORDER BY rate_date DESC LIMIT ?",
            (base, quote, window.start.isoformat(), window.end.isoformat(), limit or -1),
        )
        return [_row_to_rate(r) for r in reversed(rows)]

    def get_latest(self, base: str, symbols: Sequence[str] = ()) -> list[RateRow]:
        rows = self._query("SELECT MAX(rate_date) FROM fx_rates WHERE base = ?", (base,))
        if not rows or rows[0][0] is None:
            return []
        wanted = {s.upper() for s in symbols}
        found = self._query(
            "SELECT base, quote, rate_date, rate FROM fx_rates"
            " WHERE base = ? AND rate_date = ? ORDER BY quote",
            (base, rows[0][0]),
        )
        return [_row_to_rate(r) for r in found if not wanted or r[1] in wanted]

    def snapshot(self) -> StatusSnapshot:
        ((rate_count, first, last),) = self._query(
            "SELECT COUNT(*), MIN(rate_date), MAX(rate_date) FROM fx_rates"
        )
        ((rejected,),) = self._query("SELECT COUNT(*) FROM rejected_payloads")
        ((runs,),) = self._query("SELECT COUNT(*) FROM sync_runs")
        last_run = self._query(
            f"SELECT {_RUN_COLUMNS} FROM sync_runs ORDER BY id DESC LIMIT 1"  # noqa: S608
        )
        last_ok = self._query(
            f"SELECT {_RUN_COLUMNS} FROM sync_runs WHERE status = ? ORDER BY id DESC LIMIT 1",  # noqa: S608
            (RunStatus.SUCCESS.value,),
        )
        marks = self._query(
            "SELECT series_key, high_water_date, updated_at FROM sync_state ORDER BY series_key"
        )
        return StatusSnapshot(
            rate_count=rate_count,
            rejected_count=rejected,
            run_count=runs,
            first_date=date.fromisoformat(first) if first else None,
            last_date=date.fromisoformat(last) if last else None,
            watermarks=[
                SnapshotWatermark(s, date.fromisoformat(d), datetime.fromisoformat(u))
                for s, d, u in marks
            ],
            last_run=_row_to_run(last_run[0]) if last_run else None,
            last_success=_row_to_run(last_ok[0]) if last_ok else None,
        )
