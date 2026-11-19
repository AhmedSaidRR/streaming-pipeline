CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS marts;

-- Written by Spark Structured Streaming (append, at-least-once).
-- Duplicates after a restart are expected and removed downstream in dbt (stg_price_windows).
CREATE TABLE IF NOT EXISTS raw.price_windows (
    symbol        VARCHAR(30)      NOT NULL,
    window_start  TIMESTAMP        NOT NULL,
    window_end    TIMESTAMP        NOT NULL,
    avg_price     DOUBLE PRECISION NOT NULL,
    min_price     DOUBLE PRECISION NOT NULL,
    max_price     DOUBLE PRECISION NOT NULL,
    tick_count    BIGINT           NOT NULL,
    ingested_at   TIMESTAMP        NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_price_windows_symbol_start
    ON raw.price_windows (symbol, window_start);
