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
