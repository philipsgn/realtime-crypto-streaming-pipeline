-- Auto-run by TimescaleDB Docker on first start
CREATE EXTENSION IF NOT EXISTS timescaledb;

CREATE TABLE IF NOT EXISTS trade_metrics_1min (
    window_start     TIMESTAMPTZ      NOT NULL,
    window_end       TIMESTAMPTZ      NOT NULL,
    symbol           TEXT             NOT NULL,
    vwap             DOUBLE PRECISION,
    total_volume     DOUBLE PRECISION,
    trade_count      BIGINT,
    price_open       DOUBLE PRECISION,
    price_close      DOUBLE PRECISION,
    buy_volume       DOUBLE PRECISION,
    price_change_pct DOUBLE PRECISION,
    window_minutes   TEXT
);

CREATE TABLE IF NOT EXISTS trade_metrics_5min (LIKE trade_metrics_1min INCLUDING ALL);

SELECT create_hypertable('trade_metrics_1min', 'window_start', if_not_exists => TRUE);
SELECT create_hypertable('trade_metrics_5min', 'window_start', if_not_exists => TRUE);

CREATE INDEX IF NOT EXISTS idx_1min_symbol ON trade_metrics_1min (symbol, window_start DESC);
CREATE INDEX IF NOT EXISTS idx_5min_symbol ON trade_metrics_5min (symbol, window_start DESC);

-- Day 4: transparent AI and degraded-mode market summaries.
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'summary_source') THEN
        CREATE TYPE summary_source AS ENUM ('gemini', 'fallback_template');
    END IF;
END
$$;

CREATE TABLE IF NOT EXISTS market_summaries (
    id           BIGSERIAL      PRIMARY KEY,
    symbol       TEXT           NOT NULL,
    summary_text TEXT           NOT NULL,
    source       summary_source NOT NULL,
    created_at   TIMESTAMPTZ    NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_market_summaries_symbol_created
    ON market_summaries (symbol, created_at DESC);
