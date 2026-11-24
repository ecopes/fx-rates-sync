# Architecture

```text
 CLI / scheduler
       │
       ▼
 SyncService ──► resolves the window (watermark + lookback), splits it into chunks
       │             fetches chunks concurrently (semaphore), commits them IN ORDER
       │
       ├── RateSource (FrankfurterClient)   httpx, retry+jitter, Retry-After, circuit breaker
       ├── parse_range_payload              validate -> RateRow | Rejection (never raises on bad data)
       ├── Repository (SqliteRepository)    commit_chunk = upsert rows + quarantine + watermark, one transaction
       └── Notifier (Log / Webhook / Composite)   failures isolated, never break the run
```

## Key decisions

* **Watermark with lookback.** The next window starts at `watermark + 1 - lookback_days`, so late
  revisions are picked up; rows are compared by a SHA-256 of `(base, quote, date, rate)` and only
  changed rows are written. Re-running is therefore harmless.
* **Ordered commits, concurrent fetches.** Chunks download in parallel but commit sequentially so
  the watermark can only move forward over a contiguous good prefix. After a failed chunk the
  watermark stops advancing; the next run repairs the gap.
* **Bad data is data.** Rejections are stored (`rejected_payloads`) with the offending payload
  (truncated), the window and the reason. `status` reports the count.
* **Exact numbers.** `json.loads(..., parse_float=Decimal)`, canonical text storage.
* **Ports and adapters.** `RateSource`, `Repository` and `Notifier` are Protocols; tests use
  fakes, production uses httpx/SQLite/webhook.
* **Migrations.** Forward-only, checksummed SQL files in `src/fx_sync/sql/`; editing an applied
  migration is refused.

## Porting to another API or database

Implement `RateSource.fetch_range` (return the raw payload) and adapt `parsing.py` for the new
shape; implement `Repository` for another database. `service.py` does not change.
