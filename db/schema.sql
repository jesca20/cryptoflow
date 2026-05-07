CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS mart;

CREATE TABLE IF NOT EXISTS raw.coins (
    id               TEXT,
    symbol           TEXT,
    name             TEXT,
    current_price    NUMERIC,
    market_cap       BIGINT,
    total_volume     BIGINT,
    price_change_24h NUMERIC,
    ingested_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS raw.ingestion_log (
    id            SERIAL PRIMARY KEY,
    run_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    rows_fetched  INTEGER,
    status        TEXT NOT NULL,
    error_message TEXT
);
