"""Domain models: no I/O, no framework coupling beyond Pydantic validation."""

from __future__ import annotations

import hashlib
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from decimal import Decimal
from enum import StrEnum
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, field_validator

from fx_sync.errors import InvalidRangeError

ONE_DAY = timedelta(days=1)


@dataclass(frozen=True, order=True)
class DateRange:
    """Inclusive calendar-day range."""

    start: date
    end: date

    def __post_init__(self) -> None:
        if self.start > self.end:
            raise InvalidRangeError(f"start {self.start} is after end {self.end}")

    @property
    def days(self) -> int:
        return (self.end - self.start).days + 1

    def __contains__(self, item: object) -> bool:
        return isinstance(item, date) and self.start <= item <= self.end

    def __str__(self) -> str:
        return f"{self.start}..{self.end}"


def plan_chunks(window: DateRange, chunk_days: int) -> list[DateRange]:
    """Split ``window`` into consecutive, non-overlapping chunks of at most ``chunk_days`` days."""
    if chunk_days < 1:
        raise InvalidRangeError("chunk_days must be >= 1")
    chunks: list[DateRange] = []
    cursor = window.start
    while cursor <= window.end:
        end = min(cursor + timedelta(days=chunk_days - 1), window.end)
        chunks.append(DateRange(cursor, end))
        cursor = end + ONE_DAY
    return chunks


def series_key(base: str, symbols: tuple[str, ...]) -> str:
    """Identity of a synced series; each distinct (base, symbols) has its own watermark."""
    return f"{base}:{','.join(sorted(symbols)) if symbols else '*'}"


class RateRow(BaseModel):
    """One validated daily rate: 1 ``base`` = ``rate`` ``quote``."""

    model_config = ConfigDict(frozen=True)

    base: str = Field(pattern=r"^[A-Z]{3}$")
    quote: str = Field(pattern=r"^[A-Z]{3}$")
    rate_date: date
    rate: Decimal = Field(gt=0, allow_inf_nan=False, max_digits=24)

    @field_validator("rate", mode="before")
    @classmethod
    def _numeric_only(cls, value: Any) -> Any:
        if isinstance(value, bool) or not isinstance(value, int | float | Decimal):
            raise ValueError("rate must be a JSON number")
        return value

    @property
    def rate_text(self) -> str:
        """Canonical plain-notation text, so 0.9 and 0.90 hash identically."""
        return format(self.rate.normalize(), "f")

    @property
    def source_hash(self) -> str:
        raw = f"{self.base}|{self.quote}|{self.rate_date.isoformat()}|{self.rate_text}"
        return hashlib.sha256(raw.encode()).hexdigest()


@dataclass(frozen=True)
class Rejection:
    """A payload fragment that failed validation, kept for inspection."""

    window: DateRange
    reason: str
    payload: str
    rate_date: date | None = None


@dataclass(frozen=True)
class UpsertResult:
    inserted: int = 0
    updated: int = 0
    unchanged: int = 0
    changed: frozenset[tuple[str, date]] = frozenset()


class RunStatus(StrEnum):
    RUNNING = "running"
    SUCCESS = "success"
    PARTIAL = "partial"
    FAILED = "failed"


class RunMode(StrEnum):
    SYNC = "sync"
    BACKFILL = "backfill"


@dataclass(frozen=True)
class RunCounters:
    chunks_total: int = 0
    chunks_failed: int = 0
    inserted: int = 0
    updated: int = 0
    unchanged: int = 0
    rejected: int = 0


@dataclass(frozen=True)
class RunRecord:
    id: int
    mode: str
    series: str
    started_at: datetime
    finished_at: datetime | None
    status: str
    counters: RunCounters
    error: str | None


@dataclass(frozen=True)
class Anomaly:
    base: str
    quote: str
    rate_date: date
    previous: Decimal
    current: Decimal

    @property
    def change_pct(self) -> Decimal:
        return (self.current - self.previous) / self.previous * 100


@dataclass(frozen=True)
class SnapshotWatermark:
    series: str
    high_water_date: date
    updated_at: datetime


@dataclass(frozen=True)
class StatusSnapshot:
    rate_count: int
    rejected_count: int
    run_count: int
    first_date: date | None
    last_date: date | None
    watermarks: list[SnapshotWatermark] = field(default_factory=list)
    last_run: RunRecord | None = None
    last_success: RunRecord | None = None
