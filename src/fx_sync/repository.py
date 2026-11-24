"""Storage contract. The service and CLI depend on this Protocol, never on SQLite directly."""

from __future__ import annotations

from collections.abc import Sequence
from datetime import date, datetime
from decimal import Decimal
from typing import Protocol

from fx_sync.models import (
    DateRange,
    RateRow,
    Rejection,
    RunCounters,
    RunStatus,
    StatusSnapshot,
    UpsertResult,
)


class Repository(Protocol):
    def close(self) -> None: ...

    def acquire_lock(self, name: str, owner: str, ttl_seconds: int, now: datetime) -> bool:
        """Take the named lock if free or expired. Must be atomic across processes."""
        ...

    def renew_lock(self, name: str, owner: str, ttl_seconds: int, now: datetime) -> bool: ...

    def release_lock(self, name: str, owner: str) -> None: ...

    def start_run(self, mode: str, series: str, started_at: datetime) -> int: ...

    def finish_run(
        self,
        run_id: int,
        status: RunStatus,
        counters: RunCounters,
        finished_at: datetime,
        error: str | None,
    ) -> None: ...

    def get_watermark(self, series: str) -> date | None: ...

    def previous_rates(self, base: str, before: date) -> dict[str, Decimal]:
        """Most recent stored rate per quote strictly before ``before``."""
        ...

    def commit_chunk(
        self,
        run_id: int,
        rows: Sequence[RateRow],
        rejections: Sequence[Rejection],
        watermark: tuple[str, date] | None,
        now: datetime,
    ) -> UpsertResult:
        """Atomically upsert rows, quarantine rejections and advance the watermark.

        Either everything in the call is durable or nothing is.
        """
        ...

    def get_rate(self, base: str, quote: str, on: date, *, exact: bool = False) -> RateRow | None:
        """Rate on ``on``; unless ``exact``, falls back to the closest earlier business day."""
        ...

    def get_series(
        self, base: str, quote: str, window: DateRange, limit: int | None = None
    ) -> list[RateRow]: ...

    def get_latest(self, base: str, symbols: Sequence[str] = ()) -> list[RateRow]: ...

    def snapshot(self) -> StatusSnapshot: ...
