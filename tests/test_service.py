from __future__ import annotations

from datetime import date, datetime, timedelta
from decimal import Decimal

import pytest

from fx_sync.errors import (
    InvalidRangeError,
    LockHeldError,
    MalformedPayloadError,
    StorageError,
    TransientApiError,
)
from fx_sync.models import DateRange, RateRow, RunMode, RunStatus
from fx_sync.notifier import Alert, Severity
from fx_sync.service import SyncConfig, SyncRequest, SyncService, detect_anomalies
from fx_sync.sqlite_repo import SqliteRepository
from tests.conftest import NOW, FakeSource, RecordingNotifier

TODAY = NOW.date()  # 2025-03-12
CONFIG = SyncConfig(chunk_days=10, concurrency=2, lookback_days=3, initial_lookback_days=30)
SYNC = SyncRequest(RunMode.SYNC, "USD", ("EUR", "GBP"))


def service(source: FakeSource, repo: SqliteRepository, notifier: RecordingNotifier,
            config: SyncConfig = CONFIG, now: datetime = NOW) -> SyncService:  # fmt: skip
    return SyncService(source, repo, notifier, config, clock=lambda: now)


async def test_first_run_backfills_initial_lookback_in_chunks(repo: SqliteRepository) -> None:
    source, notifier = FakeSource(), RecordingNotifier()
    report = await service(source, repo, notifier).run(SYNC)
    assert report.status is RunStatus.SUCCESS
    assert report.window == DateRange(TODAY - timedelta(days=29), TODAY)
    assert report.counters.chunks_total == 3 and report.counters.chunks_failed == 0
    assert report.counters.inserted == 2 * 22  # 22 weekdays in the window x 2 quotes
    assert report.watermark == TODAY
    assert repo.get_watermark("USD:EUR,GBP") == TODAY
    assert notifier.alerts == []
    assert len(source.calls) == 3


async def test_second_run_is_idempotent_and_uses_lookback(repo: SqliteRepository) -> None:
    source, notifier = FakeSource(), RecordingNotifier()
    await service(source, repo, notifier).run(SYNC)
    source.calls.clear()
    report = await service(source, repo, notifier).run(SYNC)
    # watermark Mar 12 + 1 day - 3 lookback days = Mar 10
    assert report.window == DateRange(date(2025, 3, 10), TODAY)
    assert source.calls == [report.window]
    assert (report.counters.inserted, report.counters.updated) == (0, 0)
    assert report.counters.unchanged == 6
    assert repo.snapshot().rate_count == 44


async def test_next_day_picks_up_only_new_data_plus_lookback(repo: SqliteRepository) -> None:
    source, notifier = FakeSource(), RecordingNotifier()
    await service(source, repo, notifier).run(SYNC)
    tomorrow = NOW + timedelta(days=1)
    report = await service(source, repo, notifier, now=tomorrow).run(SYNC)
    assert report.window is not None and report.window.start == date(2025, 3, 10)
    assert (report.counters.inserted, report.counters.updated) == (2, 0)  # only Mar 13 is new
    assert report.watermark == date(2025, 3, 13)


async def test_revised_rates_are_updated_not_duplicated(repo: SqliteRepository) -> None:
    source, notifier = FakeSource({"EUR": "0.9"}), RecordingNotifier()
    await service(source, repo, notifier).run(SYNC)
    source.quotes = {"EUR": "0.91"}  # tiny revision, below the anomaly threshold
    report = await service(source, repo, notifier).run(SYNC)
    assert report.counters.updated == 3 and report.counters.inserted == 0
    assert repo.snapshot().rate_count == 22
    assert report.anomalies == []


async def test_large_move_raises_anomaly_alert(repo: SqliteRepository) -> None:
    source, notifier = FakeSource({"EUR": "0.9"}), RecordingNotifier()
    await service(source, repo, notifier).run(SYNC)
    source.quotes = {"EUR": "1.5"}
    report = await service(source, repo, notifier).run(SYNC)
    assert len(report.anomalies) == 1
    assert report.anomalies[0].quote == "EUR" and report.anomalies[0].change_pct > 50
    assert [a.severity for a in notifier.alerts] == [Severity.WARNING]
    assert "anomal" in notifier.alerts[0].title and "USD/EUR" in notifier.alerts[0].message


async def test_one_failed_chunk_is_partial_and_holds_back_the_watermark(
    repo: SqliteRepository,
) -> None:
    source, notifier = FakeSource(), RecordingNotifier()
    middle = DateRange(TODAY - timedelta(days=19), TODAY - timedelta(days=10))
    source.overrides[middle] = TransientApiError("boom")
    report = await service(source, repo, notifier).run(SYNC)
    assert report.status is RunStatus.PARTIAL
    assert report.counters.chunks_failed == 1 and report.errors
    # chunk 1 (before the failure) advanced the mark; later chunks must not move it further
    assert report.watermark == TODAY - timedelta(days=20)
    assert notifier.alerts[0].severity is Severity.WARNING
    # a later healthy run repairs the gap from the watermark
    source.overrides.clear()
    repaired = await service(source, repo, notifier).run(SYNC)
    assert repaired.status is RunStatus.SUCCESS
    assert repo.snapshot().rate_count == 44


async def test_all_chunks_failing_is_failed_with_error_alert(repo: SqliteRepository) -> None:
    source, notifier = FakeSource(), RecordingNotifier()
    source.fail_all = TransientApiError("api down")
    report = await service(source, repo, notifier).run(SYNC)
    assert report.status is RunStatus.FAILED and report.counters.chunks_failed == 3
    assert repo.get_watermark("USD:EUR,GBP") is None
    assert notifier.alerts[0].severity is Severity.ERROR


async def test_malformed_payload_is_quarantined_not_fatal(repo: SqliteRepository) -> None:
    source, notifier = FakeSource(), RecordingNotifier()
    first = DateRange(TODAY - timedelta(days=29), TODAY - timedelta(days=20))
    source.overrides[first] = MalformedPayloadError("not json", raw="<html>")
    report = await service(source, repo, notifier).run(SYNC)
    assert report.status is RunStatus.PARTIAL and report.counters.rejected == 1
    assert repo.snapshot().rejected_count == 1


async def test_invalid_rows_inside_a_payload_are_quarantined(repo: SqliteRepository) -> None:
    source, notifier = FakeSource(), RecordingNotifier()
    window = DateRange(TODAY - timedelta(days=29), TODAY)
    config = SyncConfig(chunk_days=90)
    source.overrides[window] = {"base": "USD", "rates": {"2025-03-10": {"EUR": 0.9, "GBP": "bad"}}}
    report = await service(source, repo, notifier, config).run(SYNC)
    assert report.status is RunStatus.PARTIAL
    assert (report.counters.inserted, report.counters.rejected) == (1, 1)


async def test_unexpected_exception_ends_failed_and_releases_lock(repo: SqliteRepository) -> None:
    source, notifier = FakeSource(), RecordingNotifier()
    source.fail_all = RuntimeError("bug")
    report = await service(source, repo, notifier).run(SYNC)
    assert report.status is RunStatus.FAILED
    assert any("unexpected RuntimeError" in e for e in report.errors)
    assert repo.acquire_lock("fx-sync", "someone-else", 60, NOW)  # lock was released


async def test_lock_held_by_another_process(repo: SqliteRepository) -> None:
    repo.acquire_lock("fx-sync", "other-process", 900, NOW)
    with pytest.raises(LockHeldError):
        await service(FakeSource(), repo, RecordingNotifier()).run(SYNC)


async def test_since_in_the_future_means_up_to_date(repo: SqliteRepository) -> None:
    source = FakeSource()
    request = SyncRequest(RunMode.SYNC, "USD", ("EUR",), since=TODAY + timedelta(days=5))
    report = await service(source, repo, RecordingNotifier()).run(request)
    assert report.window is None and report.status is RunStatus.SUCCESS and source.calls == []


async def test_backfill_clamps_to_today_and_does_not_move_mark_backwards(
    repo: SqliteRepository,
) -> None:
    source, notifier = FakeSource(), RecordingNotifier()
    await service(source, repo, notifier).run(SYNC)
    old = DateRange(date(2024, 1, 1), date(2024, 1, 31))
    report = await service(source, repo, notifier).run(
        SyncRequest(RunMode.BACKFILL, "USD", ("EUR", "GBP"), window=old)
    )
    assert report.status is RunStatus.SUCCESS and report.watermark == TODAY
    assert repo.get_watermark("USD:EUR,GBP") == TODAY
    clamped = await service(source, repo, notifier).run(
        SyncRequest(
            RunMode.BACKFILL,
            "USD",
            ("EUR", "GBP"),
            window=DateRange(TODAY, TODAY + timedelta(days=9)),
        )
    )
    assert clamped.window == DateRange(TODAY, TODAY)


async def test_backfill_validation(repo: SqliteRepository) -> None:
    svc = service(FakeSource(), repo, RecordingNotifier())
    with pytest.raises(InvalidRangeError, match="requires a window"):
        await svc.run(SyncRequest(RunMode.BACKFILL, "USD", ()))
    future = DateRange(TODAY + timedelta(days=1), TODAY + timedelta(days=2))
    with pytest.raises(InvalidRangeError, match="future"):
        await svc.run(SyncRequest(RunMode.BACKFILL, "USD", (), window=future))


async def test_notifier_failure_never_breaks_the_run(repo: SqliteRepository) -> None:
    class Exploding:
        async def notify(self, alert: Alert) -> None:
            raise RuntimeError("slack down")

    source = FakeSource()
    source.fail_all = TransientApiError("down")
    svc = SyncService(source, repo, Exploding(), CONFIG, clock=lambda: NOW)
    report = await svc.run(SYNC)
    assert report.status is RunStatus.FAILED


async def test_storage_failure_is_fatal_and_recorded(
    repo: SqliteRepository, monkeypatch: pytest.MonkeyPatch
) -> None:
    def broken(*_: object, **__: object) -> None:
        raise StorageError("disk full")

    monkeypatch.setattr(repo, "commit_chunk", broken)
    report = await service(FakeSource(), repo, RecordingNotifier()).run(SYNC)
    assert report.status is RunStatus.FAILED and "disk full" in report.errors[0]
    last = repo.snapshot().last_run
    assert last is not None and last.status == "failed"


async def test_many_anomalies_are_summarised_in_one_alert(repo: SqliteRepository) -> None:
    quotes = {f"C{chr(65 + i)}{chr(65 + j)}": "1" for i in range(2) for j in range(7)}
    source, notifier = FakeSource(quotes), RecordingNotifier()
    await service(source, repo, notifier).run(SyncRequest(RunMode.SYNC, "USD", ()))
    source.quotes = dict.fromkeys(quotes, "3")
    report = await service(source, repo, notifier).run(SyncRequest(RunMode.SYNC, "USD", ()))
    assert len(report.anomalies) == 14
    assert "and 4 more" in notifier.alerts[-1].message


def test_detect_anomalies_unit() -> None:
    def r(day: int, rate: str) -> RateRow:
        return RateRow(base="USD", quote="EUR", rate_date=date(2025, 3, day), rate=Decimal(rate))

    rows = [r(11, "1.0"), r(12, "1.2"), r(13, "1.21")]
    changed = {("EUR", date(2025, 3, 12)), ("EUR", date(2025, 3, 13))}
    found = detect_anomalies(rows, changed, {"EUR": Decimal("1.0")}, Decimal(10))
    assert [a.rate_date.day for a in found] == [12]
    assert found[0].change_pct == Decimal(20)
    assert (
        detect_anomalies(rows, set(), {"EUR": Decimal("1.0")}, Decimal(10)) == []
    )  # unchanged rows
    assert (
        detect_anomalies(rows[:1], {("EUR", date(2025, 3, 11))}, {}, Decimal(10)) == []
    )  # no history
