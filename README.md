# fx-rates-sync

Incremental, idempotent sync of daily exchange rates from a public REST API
([Frankfurter](https://frankfurter.dev)) into a local SQLite database, with retries, a circuit
breaker, quarantine of bad payloads, run history, health checks and optional webhook alerts.

It is a compact reference for the kind of work "connect system A to system B reliably":
pull from a third-party API, validate and transform the data, store it exactly once, survive
failures, and tell a human when something needs attention.

## Why it is safe to run on a schedule

| Problem | How it is handled |
|---|---|
| Run twice / overlapping runs | Re-fetching is idempotent (rows are keyed and hashed, unchanged rows are not rewritten). A DB lock with expiry stops two syncs at once (exit code 3). |
| API hiccups (timeouts, 429, 5xx) | Exponential backoff with full jitter, honours `Retry-After`, then a circuit breaker stops hammering a dead API. |
| API revises past values | A configurable lookback window is re-fetched each run; changed rows are updated and counted. |
| Garbage in the response | Each bad value is **quarantined** with its reason instead of failing the run or being dropped silently. |
| Crash halfway | Every chunk commits atomically together with the watermark; the next run resumes from the last good point. |
| Float rounding of money | Rates travel as exact `Decimal` and are stored as canonical text. |
| Something looks wrong | Day-over-day jumps above a threshold, failed or partial runs raise an alert (log and/or webhook). |
| "Is it working?" | `fx-sync status --check` (exit 1 when unhealthy) and a read-only `/health` + `/status` HTTP endpoint. |

## Quick start

```bash
python3 -m venv .venv && .venv/bin/python -m pip install -e .
.venv/bin/fx-sync sync --symbols EUR,GBP,JPY --since 2025-10-01
.venv/bin/fx-sync sync --symbols EUR,GBP,JPY        # second run: nothing inserted
.venv/bin/fx-sync query latest --symbols EUR,GBP,JPY
.venv/bin/fx-sync status
```

Real output from a run against the live API (2026-11-24):

```text
success: run #1 window=2025-10-01..2026-11-24 chunks=5 failed=0 inserted=882 updated=0 unchanged=0 rejected=0 anomalies=0 watermark=2026-11-24
success: run #2 window=2026-11-22..2026-11-24 chunks=1 failed=0 inserted=0 updated=0 unchanged=6 rejected=0 anomalies=0 watermark=2026-11-24

$ fx-sync query latest --symbols EUR,GBP,JPY
2026-11-24  USD/EUR  0.91216
2026-11-24  USD/GBP  0.76348
2026-11-24  USD/JPY  159.07

$ fx-sync query rate USD EUR --date 2026-11-21          # a Saturday: falls back to Friday
2026-11-20  USD/EUR  0.90992
$ fx-sync query rate USD EUR --date 2026-11-21 --exact
no rate found for USD/EUR on 2026-11-21
```

Commands: `sync`, `backfill --from --to`, `query rate|series|latest [--json]`, `status [--json] [--check]`,
`serve` (HTTP). Exit codes: `0` ok, `1` failed/partial/unhealthy, `2` bad usage or config, `3` locked.

## Configuration

Environment variables (prefix `FX_`), a `.env` file, or CLI flags (flags win). Main ones:
`FX_DB_PATH`, `FX_BASE`, `FX_SYMBOLS`, `FX_CHUNK_DAYS`, `FX_CONCURRENCY`, `FX_LOOKBACK_DAYS`,
`FX_MAX_RETRIES`, `FX_ANOMALY_PCT`, `FX_WEBHOOK_URL`, `FX_STALE_AFTER_HOURS`, `FX_LOG_FORMAT=json`.
Full list in `src/fx_sync/config.py`. Secrets (such as a webhook URL) come from the environment
and are never written to the database or logs.

## Webhook alerts

With `--webhook-url` (or `FX_WEBHOOK_URL`) failures, partial runs and anomalies are posted as
`{"text": ":rotating_light: *FX sync failed*\n..."}`, the body Slack-compatible incoming webhooks
accept. A broken webhook never breaks the sync (covered by tests).

## Architecture

See [`docs/architecture.md`](docs/architecture.md). In short: `client` (HTTP + retries) ->
`parsing` (validate, quarantine) -> `service` (chunking, concurrency, ordered atomic commits,
anomaly detection) -> `repository` (a Protocol; `sqlite_repo` is one implementation, the schema
notes how it maps to PostgreSQL).

## Quality

```bash
python -m pip install -e '.[dev]'
ruff check . && ruff format --check . && mypy && pytest --cov
```

95 tests, 97% line+branch coverage, `mypy --strict`. The HTTP layer is tested with
`httpx.MockTransport` (no network), time and randomness are injected, and the CLI tests run
the real commands end to end. A `Dockerfile` is included (the CI builds it).

## Limits

* SQLite, single machine. The repository interface is the seam for PostgreSQL/MySQL.
* One upstream API (Frankfurter, which publishes ECB reference rates on business days). Rates are
  reference rates, not tradable quotes.
* The lock is cooperative via the database file; it does not coordinate across different hosts.

MIT licensed.
