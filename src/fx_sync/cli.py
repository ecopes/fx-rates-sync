"""Command-line interface (``fx-sync``)."""

from __future__ import annotations

import argparse
import asyncio
import json
import sys
from collections.abc import Callable, Sequence
from datetime import date, datetime, timedelta
from decimal import Decimal

import httpx
from pydantic import ValidationError

from fx_sync import __version__
from fx_sync.client import FrankfurterClient
from fx_sync.config import Settings
from fx_sync.errors import FxSyncError, LockHeldError
from fx_sync.logging_setup import configure_logging
from fx_sync.models import DateRange, RateRow, RunMode, RunStatus
from fx_sync.notifier import CompositeNotifier, LogNotifier, WebhookNotifier
from fx_sync.resilience import CircuitBreaker, RetryPolicy
from fx_sync.server import make_server
from fx_sync.service import RunReport, SyncConfig, SyncRequest, SyncService, utc_now
from fx_sync.sqlite_repo import SqliteRepository
from fx_sync.status import evaluate_health, status_payload

EXIT_OK, EXIT_FAILED, EXIT_USAGE, EXIT_LOCKED = 0, 1, 2, 3


def _out(text: str = "") -> None:
    sys.stdout.write(text + "\n")


def _csv(text: str) -> tuple[str, ...]:
    return tuple(part for part in text.replace(" ", "").upper().split(",") if part)


def _parse_date(text: str, today: date) -> date:
    return today if text.lower() == "today" else date.fromisoformat(text)


def build_parser() -> argparse.ArgumentParser:
    common = argparse.ArgumentParser(add_help=False)
    common.add_argument("--db", dest="db_path", help="SQLite file (env FX_DB_PATH)")
    common.add_argument("--log-level", dest="log_level", help="DEBUG|INFO|WARNING|ERROR")
    common.add_argument("--log-format", dest="log_format", choices=["text", "json"])

    series = argparse.ArgumentParser(add_help=False)
    series.add_argument("--base", help="base currency, e.g. USD (env FX_BASE)")
    series.add_argument(
        "--symbols", type=_csv, help="comma-separated quote currencies (env FX_SYMBOLS)"
    )
    tuning = argparse.ArgumentParser(add_help=False)
    tuning.add_argument(
        "--concurrency", type=int, help="parallel chunk fetches (env FX_CONCURRENCY)"
    )
    tuning.add_argument("--chunk-days", dest="chunk_days", type=int)
    tuning.add_argument("--anomaly-pct", dest="anomaly_pct", type=float)
    tuning.add_argument("--webhook-url", dest="webhook_url")

    parser = argparse.ArgumentParser(
        prog="fx-sync", description="Incremental, idempotent FX rate sync (Frankfurter -> SQLite)."
    )
    parser.add_argument("--version", action="version", version=f"fx-sync {__version__}")
    sub = parser.add_subparsers(dest="command", required=True)

    p_sync = sub.add_parser("sync", parents=[common, series, tuning], help="incremental sync")
    p_sync.add_argument("--since", help="override the start date (YYYY-MM-DD)")
    p_sync.add_argument("--lookback-days", dest="lookback_days", type=int)

    p_back = sub.add_parser("backfill", parents=[common, series, tuning], help="fetch a date range")
    p_back.add_argument("--from", dest="from_date", required=True, help="YYYY-MM-DD")
    p_back.add_argument("--to", dest="to_date", default="today", help="YYYY-MM-DD or 'today'")

    p_query = sub.add_parser("query", help="read-only queries")
    qsub = p_query.add_subparsers(dest="query_command", required=True)
    q_rate = qsub.add_parser("rate", parents=[common], help="rate for a pair on a date")
    q_rate.add_argument("pair_base", metavar="base")
    q_rate.add_argument("quote")
    q_rate.add_argument("--date", dest="on", default="today")
    q_rate.add_argument(
        "--exact", action="store_true", help="no fallback to the prior business day"
    )
    q_series = qsub.add_parser("series", parents=[common], help="rates over a date range")
    q_series.add_argument("pair_base", metavar="base")
    q_series.add_argument("quote")
    q_series.add_argument("--from", dest="from_date", required=True)
    q_series.add_argument("--to", dest="to_date", default="today")
    q_series.add_argument("--limit", type=int, help="keep only the most recent N rows")
    q_latest = qsub.add_parser("latest", parents=[common], help="newest stored rates")
    q_latest.add_argument("--base", default=None)
    q_latest.add_argument("--symbols", type=_csv)
    for q in (q_rate, q_series, q_latest):
        q.add_argument("--json", action="store_true", help="machine-readable output")

    p_status = sub.add_parser("status", parents=[common], help="last run, watermark, counts")
    p_status.add_argument("--json", action="store_true")
    p_status.add_argument("--check", action="store_true", help="exit 1 when unhealthy")

    p_serve = sub.add_parser("serve", parents=[common], help="read-only HTTP status endpoint")
    p_serve.add_argument("--host", default="127.0.0.1")
    p_serve.add_argument("--port", type=int, default=8080)
    return parser


def _rate_json(row: RateRow) -> dict[str, str]:
    return {
        "base": row.base,
        "quote": row.quote,
        "date": row.rate_date.isoformat(),
        "rate": row.rate_text,
    }


def _print_rows(rows: Sequence[RateRow], as_json: bool) -> None:
    if as_json:
        _out(json.dumps([_rate_json(r) for r in rows], indent=2))
        return
    for row in rows:
        _out(f"{row.rate_date}  {row.base}/{row.quote}  {row.rate_text}")


def _settings(args: argparse.Namespace) -> Settings:
    names = Settings.model_fields.keys()
    overrides = {k: v for k, v in vars(args).items() if k in names and v is not None}
    return Settings(**overrides)


def _report_line(report: RunReport) -> str:
    c = report.counters
    return (
        f"{report.status.value}: run #{report.run_id} window={report.window or 'up-to-date'} "
        f"chunks={c.chunks_total} failed={c.chunks_failed} inserted={c.inserted} "
        f"updated={c.updated} unchanged={c.unchanged} rejected={c.rejected} "
        f"anomalies={len(report.anomalies)} watermark={report.watermark}"
    )


async def _execute_sync(
    settings: Settings,
    request: SyncRequest,
    transport: httpx.AsyncBaseTransport | None,
    clock: Callable[[], datetime],
) -> RunReport:
    async with httpx.AsyncClient(
        timeout=httpx.Timeout(settings.http_timeout),
        transport=transport,
        follow_redirects=True,
        headers={"User-Agent": f"fx-rates-sync/{__version__}"},
    ) as http:
        client = FrankfurterClient(
            http,
            base_url=settings.api_url,
            retry=RetryPolicy(
                max_retries=settings.max_retries,
                backoff_base=settings.backoff_base,
                backoff_max=settings.backoff_max,
            ),
            breaker=CircuitBreaker(settings.breaker_threshold),
            now=clock,
        )
        notifier = (
            CompositeNotifier(LogNotifier(), WebhookNotifier(settings.webhook_url, http))
            if settings.webhook_url
            else LogNotifier()
        )
        config = SyncConfig(
            chunk_days=settings.chunk_days,
            concurrency=settings.concurrency,
            lookback_days=settings.lookback_days,
            initial_lookback_days=settings.initial_lookback_days,
            anomaly_pct=Decimal(str(settings.anomaly_pct)),
            lock_ttl_seconds=settings.lock_ttl_seconds,
        )
        repo = SqliteRepository(settings.db_path)
        try:
            return await SyncService(client, repo, notifier, config, clock).run(request)
        finally:
            repo.close()


def _cmd_sync(
    args: argparse.Namespace,
    settings: Settings,
    transport: httpx.AsyncBaseTransport | None,
    clock: Callable[[], datetime],
) -> int:
    today = clock().date()
    if args.command == "backfill":
        window = DateRange(_parse_date(args.from_date, today), _parse_date(args.to_date, today))
        request = SyncRequest(RunMode.BACKFILL, settings.base, settings.symbols, window=window)
    else:
        since = _parse_date(args.since, today) if args.since else None
        request = SyncRequest(RunMode.SYNC, settings.base, settings.symbols, since=since)
    try:
        report = asyncio.run(_execute_sync(settings, request, transport, clock))
    except LockHeldError as exc:
        sys.stderr.write(f"error: {exc}\n")
        return EXIT_LOCKED
    _out(_report_line(report))
    for error in report.errors:
        sys.stderr.write(f"  error: {error}\n")
    for anomaly in report.anomalies[:5]:
        _out(
            f"  anomaly: {anomaly.base}/{anomaly.quote} {anomaly.rate_date} "
            f"{anomaly.change_pct:+.2f}%"
        )
    return EXIT_OK if report.status is RunStatus.SUCCESS else EXIT_FAILED


def _cmd_query(args: argparse.Namespace, settings: Settings, clock: Callable[[], datetime]) -> int:
    today = clock().date()
    repo = SqliteRepository(settings.db_path)
    try:
        if args.query_command == "rate":
            base, quote = args.pair_base.upper(), args.quote.upper()
            row = repo.get_rate(base, quote, _parse_date(args.on, today), exact=args.exact)
            if row is None:
                sys.stderr.write(f"no rate found for {base}/{quote} on {args.on}\n")
                return EXIT_FAILED
            _print_rows([row], args.json)
        elif args.query_command == "series":
            window = DateRange(_parse_date(args.from_date, today), _parse_date(args.to_date, today))
            rows = repo.get_series(args.pair_base.upper(), args.quote.upper(), window, args.limit)
            _print_rows(rows, args.json)
        else:
            _print_rows(
                repo.get_latest(settings.base, settings.symbols if args.symbols else ()), args.json
            )
    finally:
        repo.close()
    return EXIT_OK


def _cmd_status(args: argparse.Namespace, settings: Settings, clock: Callable[[], datetime]) -> int:
    repo = SqliteRepository(settings.db_path)
    try:
        snapshot = repo.snapshot()
    finally:
        repo.close()
    health = evaluate_health(snapshot, clock(), timedelta(hours=settings.stale_after_hours))
    payload = status_payload(snapshot, health)
    if args.json:
        _out(json.dumps(payload, indent=2))
    else:
        run = payload["last_run"]
        _out(f"health:      {'OK' if health.healthy else 'UNHEALTHY'}")
        for reason in health.reasons:
            _out(f"  - {reason}")
        _out(f"rates:       {payload['rates']} ({payload['first_date']} .. {payload['last_date']})")
        _out(f"quarantined: {payload['rejected_payloads']}   runs: {payload['runs']}")
        for series_name, mark in payload["watermarks"].items():
            _out(f"watermark:   {series_name} -> {mark}")
        if run:
            _out(
                f"last run:    #{run['id']} {run['mode']} {run['status']} "
                f"finished={run['finished_at']} +{run['inserted']} ~{run['updated']} "
                f"={run['unchanged']} rejected={run['rejected']}"
            )
    return EXIT_FAILED if args.check and not health.healthy else EXIT_OK


def _cmd_serve(args: argparse.Namespace, settings: Settings) -> int:
    server = make_server(
        args.host,
        args.port,
        lambda: SqliteRepository(settings.db_path),
        timedelta(hours=settings.stale_after_hours),
    )
    _out(f"serving /health and /status on http://{args.host}:{server.server_address[1]}")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()
    return EXIT_OK


def main(
    argv: Sequence[str] | None = None,
    *,
    transport: httpx.AsyncBaseTransport | None = None,
    clock: Callable[[], datetime] = utc_now,
) -> int:
    args = build_parser().parse_args(argv)
    try:
        settings = _settings(args)
        configure_logging(settings.log_level, settings.log_format)
        if args.command in ("sync", "backfill"):
            return _cmd_sync(args, settings, transport, clock)
        if args.command == "query":
            return _cmd_query(args, settings, clock)
        if args.command == "status":
            return _cmd_status(args, settings, clock)
        return _cmd_serve(args, settings)
    except ValidationError as exc:
        sys.stderr.write(
            f"configuration error: {exc.errors()[0]['loc']}: {exc.errors()[0]['msg']}\n"
        )
        return EXIT_USAGE
    except (ValueError, FxSyncError) as exc:
        sys.stderr.write(f"error: {exc}\n")
        return EXIT_USAGE if isinstance(exc, ValueError) else EXIT_FAILED
