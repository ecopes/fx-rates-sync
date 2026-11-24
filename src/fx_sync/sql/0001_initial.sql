-- Initial schema. Dates are ISO-8601 text, rates are canonical decimal text (exact, no floats).
-- On PostgreSQL: rate -> NUMERIC(24, 12), timestamps -> TIMESTAMPTZ, id -> BIGINT GENERATED.

CREATE TABLE fx_rates (
    base        TEXT NOT NULL,
    quote       TEXT NOT NULL,
    rate_date   TEXT NOT NULL,
    rate        TEXT NOT NULL,
    source_hash TEXT NOT NULL,
    created_at  TEXT NOT NULL,
    updated_at  TEXT NOT NULL,
    PRIMARY KEY (base, quote, rate_date)
) WITHOUT ROWID;

CREATE INDEX idx_fx_rates_date ON fx_rates (rate_date);

CREATE TABLE sync_state (
    series_key      TEXT PRIMARY KEY,
    high_water_date TEXT NOT NULL,
    updated_at      TEXT NOT NULL
);

CREATE TABLE sync_runs (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    mode            TEXT NOT NULL,
    series_key      TEXT NOT NULL,
    started_at      TEXT NOT NULL,
    finished_at     TEXT,
    status          TEXT NOT NULL,
    chunks_total    INTEGER NOT NULL DEFAULT 0,
    chunks_failed   INTEGER NOT NULL DEFAULT 0,
    rows_inserted   INTEGER NOT NULL DEFAULT 0,
    rows_updated    INTEGER NOT NULL DEFAULT 0,
    rows_unchanged  INTEGER NOT NULL DEFAULT 0,
    rows_rejected   INTEGER NOT NULL DEFAULT 0,
    error           TEXT
);

CREATE TABLE rejected_payloads (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id       INTEGER REFERENCES sync_runs (id),
    window_start TEXT NOT NULL,
    window_end   TEXT NOT NULL,
    rate_date    TEXT,
    reason       TEXT NOT NULL,
    payload      TEXT NOT NULL,
    created_at   TEXT NOT NULL
);

CREATE TABLE locks (
    name        TEXT PRIMARY KEY,
    owner       TEXT NOT NULL,
    acquired_at TEXT NOT NULL,
    expires_at  TEXT NOT NULL
);
