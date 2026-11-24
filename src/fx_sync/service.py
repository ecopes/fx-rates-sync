"""Sync orchestration: window resolution, concurrent fetching, ordered atomic commits, alerting."""

from __future__ import annotations

import asyncio
import logging
import uuid
from collections.abc import Callable, Collection, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal

from fx_sync.client import RateSource
from fx_sync.errors import (
    ApiError,
    InvalidRangeError,
    LockHeldError,
    MalformedPayloadError,
    StorageError,
)
from fx_sync.models import (
    ONE_DAY,
    Anomaly,
    DateRange,
    RateRow,
    Rejection,
    RunCounters,
    RunMode,
    RunStatus,
    plan_chunks,
    series_key,
)
from fx_sync.notifier import Alert, Notifier, Severity
from fx_sync.parsing import ParsedChunk, parse_range_payload
from fx_sync.repository import Repository

logger = logging.getLogger("fx_sync.service")

LOCK_NAME = "fx-sync"
MAX_ANOMALIES_IN_ALERT = 10


def utc_now() -> datetime:
    return datetime.now(UTC)


@dataclass(frozen=True)
class SyncConfig:
    chunk_days: int = 90
    concurrency: int = 3
    lookback_days: int = 3
    initial_lookback_days: int = 30
    anomaly_pct: Decimal = Decimal(10)
    lock_ttl_seconds: int = 900


@dataclass(frozen=True)
class SyncRequest:
    mode: RunMode
    base: str
    symbols: tuple[str, ...]
    window: DateRange | None = None
    since: date | None = None


@dataclass
class RunReport:
    run_id: int | None
    status: RunStatus
    window: DateRange | None
    counters: RunCounters
    watermark: date | None
    anomalies: list[Anomaly] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)


def detect_anomalies(
    rows: Sequence[RateRow],
    changed: Collection[tuple[str, date]],
    previous: Mapping[str, Decimal],
    threshold_pct: Decimal,
) -> list[Anomaly]:
    """Flag changed rows that moved more than ``threshold_pct`` versus the prior business day.

    Unchanged rows are skipped so the lookback re-fetch does not re-alert on old moves.
    """
    last = dict(previous)
    found: list[Anomaly] = []
    for row in sorted(rows, key=lambda r: (r.rate_date, r.quote)):
        before = last.get(row.quote)
        last[row.quote] = row.rate
        if before is None or (row.quote, row.rate_date) not in changed:
            continue
        move = abs((row.rate - before) / before * 100)
        if move > threshold_pct:
            found.append(Anomaly(row.base, row.quote, row.rate_date, before, row.rate))
    return found


class SyncService:
    def __init__(
        self,
        source: RateSource,
        repo: Repository,
        notifier: Notifier,
        config: SyncConfig | None = None,
        clock: Callable[[], datetime] = utc_now,
    ) -> None:
        self._source = source
        self._repo = repo
        self._notifier = notifier
        self._config = config or SyncConfig()
        self._clock = clock

    async def run(self, request: SyncRequest) -> RunReport:
        owner = uuid.uuid4().hex
        ttl = self._config.lock_ttl_seconds
        if not self._repo.acquire_lock(LOCK_NAME, owner, ttl, self._clock()):
            raise LockHeldError("another sync is already running")
        try:
            return await self._run_locked(request, owner)
        finally:
            self._repo.release_lock(LOCK_NAME, owner)

    def _resolve_window(
        self, request: SyncRequest, watermark: date | None, today: date
    ) -> DateRange | None:
        if request.mode is RunMode.BACKFILL:
            if request.window is None:
                raise InvalidRangeError("backfill requires a window")
            if request.window.start > today:
                raise InvalidRangeError(f"backfill start {request.window.start} is in the future")
            return DateRange(request.window.start, min(request.window.end, today))
        if request.since is not None:
            start = request.since
        elif watermark is None:
            start = today - timedelta(days=self._config.initial_lookback_days - 1)
        else:
            start = watermark + ONE_DAY - timedelta(days=self._config.lookback_days)
        return DateRange(start, today) if start <= today else None

    async def _run_locked(self, request: SyncRequest, owner: str) -> RunReport:
        repo, config = self._repo, self._config
        series = series_key(request.base, request.symbols)
        watermark = repo.get_watermark(series)
        window = self._resolve_window(request, watermark, self._clock().date())
        run_id = repo.start_run(request.mode.value, series, self._clock())
        report = RunReport(run_id, RunStatus.RUNNING, window, RunCounters(), watermark)
        if window is None:
            return self._finish(report, RunCounters(), None)

        chunks = plan_chunks(window, config.chunk_days)
        semaphore = asyncio.Semaphore(config.concurrency)

        async def fetch(chunk: DateRange) -> object:
            async with semaphore:
                return await self._source.fetch_range(request.base, request.symbols, chunk)

        tasks = [asyncio.create_task(fetch(chunk)) for chunk in chunks]
        counters = RunCounters(chunks_total=len(chunks))
        fatal: str | None = None
        advance = (
            request.mode is RunMode.SYNC or watermark is None or window.start <= watermark + ONE_DAY
        )
        try:
            for chunk, task in zip(chunks, tasks, strict=True):
                try:
                    parsed = parse_range_payload(await task, base=request.base, window=chunk)
                except MalformedPayloadError as exc:
                    parsed = ParsedChunk(rejections=[Rejection(chunk, str(exc), exc.raw)])
                except ApiError as exc:
                    logger.error("chunk %s failed: %s", chunk, exc)
                    report.errors.append(f"{chunk}: {exc}")
                    counters = _add(counters, chunks_failed=1)
                    advance = False
                    continue

                previous = repo.previous_rates(request.base, chunk.start)
                new_mark = max(chunk.end, watermark or chunk.end) if advance else None
                result = repo.commit_chunk(
                    run_id,
                    parsed.rows,
                    parsed.rejections,
                    (series, new_mark) if new_mark else None,
                    self._clock(),
                )
                watermark = new_mark or watermark
                counters = _add(
                    counters,
                    inserted=result.inserted,
                    updated=result.updated,
                    unchanged=result.unchanged,
                    rejected=len(parsed.rejections),
                )
                report.anomalies += detect_anomalies(
                    parsed.rows, result.changed, previous, config.anomaly_pct
                )
                logger.info(
                    "chunk %s committed: +%d ~%d =%d rejected=%d",
                    chunk,
                    result.inserted,
                    result.updated,
                    result.unchanged,
                    len(parsed.rejections),
                )
                if not repo.renew_lock(LOCK_NAME, owner, config.lock_ttl_seconds, self._clock()):
                    raise LockHeldError("lock lost during run")
        except (StorageError, LockHeldError) as exc:
            fatal = f"{type(exc).__name__}: {exc}"
        except Exception as exc:
            logger.exception("unexpected error during sync")
            fatal = f"unexpected {type(exc).__name__}: {exc}"
        finally:
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)

        report.watermark = watermark
        self._finish(report, counters, fatal)
        await self._alert(report)
        return report

    def _finish(self, report: RunReport, counters: RunCounters, fatal: str | None) -> RunReport:
        if fatal is not None or (
            counters.chunks_total > 0 and counters.chunks_failed == counters.chunks_total
        ):
            status = RunStatus.FAILED
        elif counters.chunks_failed or counters.rejected:
            status = RunStatus.PARTIAL
        else:
            status = RunStatus.SUCCESS
        if fatal:
            report.errors.append(fatal)
        report.status, report.counters = status, counters
        try:
            if report.run_id is not None:
                self._repo.finish_run(
                    report.run_id,
                    status,
                    counters,
                    self._clock(),
                    "; ".join(report.errors)[:2000] or None,
                )
        except StorageError:
            logger.exception("could not record run outcome")
        return report

    async def _alert(self, report: RunReport) -> None:
        alerts: list[Alert] = []
        c = report.counters
        if report.status is RunStatus.FAILED:
            alerts.append(
                Alert(Severity.ERROR, "FX sync failed", "; ".join(report.errors) or "unknown error")
            )
        elif report.status is RunStatus.PARTIAL:
            detail = f"{c.chunks_failed} chunk(s) failed, {c.rejected} payload(s) quarantined"
            alerts.append(
                Alert(Severity.WARNING, "FX sync partial", "; ".join([detail, *report.errors]))
            )
        if report.anomalies:
            shown = report.anomalies[:MAX_ANOMALIES_IN_ALERT]
            lines = [
                f"{a.base}/{a.quote} {a.rate_date}: {a.previous:.6f} -> {a.current:.6f}"
                f" ({a.change_pct:+.2f}%)"
                for a in shown
            ]
            more = len(report.anomalies) - len(shown)
            if more > 0:
                lines.append(f"... and {more} more")
            alerts.append(
                Alert(
                    Severity.WARNING,
                    f"FX anomalies: {len(report.anomalies)} large day-over-day move(s)",
                    "\n".join(lines),
                )
            )
        for alert in alerts:
            try:
                await self._notifier.notify(alert)
            except Exception:
                logger.exception("alert delivery failed")


def _add(counters: RunCounters, **delta: int) -> RunCounters:
    values = {name: getattr(counters, name) + delta.get(name, 0) for name in counters.__dict__}
    return RunCounters(**values)
