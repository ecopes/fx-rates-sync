from __future__ import annotations

import json
from collections.abc import Callable, Iterator, Sequence
from datetime import UTC, date, datetime
from decimal import Decimal
from pathlib import Path

import httpx
import pytest

from fx_sync.models import DateRange
from fx_sync.notifier import Alert
from fx_sync.sqlite_repo import SqliteRepository

NOW = datetime(2025, 3, 12, 9, 0, tzinfo=UTC)  # a Wednesday


def make_payload(
    base: str, window: DateRange, quotes: dict[str, str], skip_weekends: bool = True
) -> dict[str, object]:
    """Frankfurter-shaped range payload; rates drift deterministically by calendar date."""
    rates: dict[str, dict[str, object]] = {}
    day = window.start
    while day <= window.end:
        if not skip_weekends or day.weekday() < 5:
            offset = day.toordinal() - date(2025, 1, 1).toordinal()  # same date -> same rate
            rates[day.isoformat()] = {
                q: float(Decimal(v) + Decimal("0.0001") * offset) for q, v in quotes.items()
            }
        day = date.fromordinal(day.toordinal() + 1)
    return {"amount": 1.0, "base": base, "start_date": window.start.isoformat(),
            "end_date": window.end.isoformat(), "rates": rates}  # fmt: skip


class FakeSource:
    """In-memory RateSource: records calls, can fail or return arbitrary payloads per window."""

    def __init__(self, quotes: dict[str, str] | None = None) -> None:
        self.quotes = quotes or {"EUR": "0.9", "GBP": "0.8"}
        self.calls: list[DateRange] = []
        self.overrides: dict[DateRange, object | Exception] = {}
        self.fail_all: Exception | None = None

    async def fetch_range(self, base: str, symbols: Sequence[str], window: DateRange) -> object:
        self.calls.append(window)
        if self.fail_all is not None:
            raise self.fail_all
        found = self.overrides.get(window)
        if isinstance(found, Exception):
            raise found
        if found is not None:
            return found
        return make_payload(base, window, self.quotes)


class RecordingNotifier:
    def __init__(self) -> None:
        self.alerts: list[Alert] = []

    async def notify(self, alert: Alert) -> None:
        self.alerts.append(alert)


@pytest.fixture
def repo() -> Iterator[SqliteRepository]:
    repository = SqliteRepository(":memory:")
    yield repository
    repository.close()


@pytest.fixture
def db_path(tmp_path: Path) -> Path:
    return tmp_path / "fx.db"


@pytest.fixture
def clock() -> Callable[[], datetime]:
    return lambda: NOW


def json_response(
    body: object, status: int = 200, headers: dict[str, str] | None = None
) -> httpx.Response:
    return httpx.Response(status, content=json.dumps(body), headers=headers)
