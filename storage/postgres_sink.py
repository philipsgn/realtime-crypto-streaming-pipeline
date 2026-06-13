"""
Stage 3 — Storage
TimescaleDB schema init + helper for manual writes.
Main writes happen via Spark JDBC in spark_streaming.py
"""

import os
import psycopg2

POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")
POSTGRES_PORT = int(os.getenv("POSTGRES_PORT", 5432))
POSTGRES_DB   = os.getenv("POSTGRES_DB", "crypto_pipeline")
POSTGRES_USER = os.getenv("POSTGRES_USER", "pipeline")
POSTGRES_PASS = os.getenv("POSTGRES_PASSWORD", "changeme")


def get_conn():
    return psycopg2.connect(
        host=POSTGRES_HOST, port=POSTGRES_PORT,
        dbname=POSTGRES_DB, user=POSTGRES_USER, password=POSTGRES_PASS
    )


def init_schema() -> None:
    """Create hypertables for time-series metrics."""
    ddl = """
    CREATE EXTENSION IF NOT EXISTS timescaledb;

    CREATE TABLE IF NOT EXISTS trade_metrics_1min (
        window_start     TIMESTAMPTZ NOT NULL,
        window_end       TIMESTAMPTZ NOT NULL,
        symbol           TEXT        NOT NULL,
        vwap             DOUBLE PRECISION,
        total_volume     DOUBLE PRECISION,
        trade_count      BIGINT,
        price_open       DOUBLE PRECISION,
        price_close      DOUBLE PRECISION,
        buy_volume       DOUBLE PRECISION,
        price_change_pct DOUBLE PRECISION,
        window_minutes   TEXT
    );

    CREATE TABLE IF NOT EXISTS trade_metrics_5min (
        LIKE trade_metrics_1min INCLUDING ALL
    );

    -- Convert to TimescaleDB hypertables (time-series optimized)
    SELECT create_hypertable('trade_metrics_1min', 'window_start', if_not_exists => TRUE);
    SELECT create_hypertable('trade_metrics_5min', 'window_start', if_not_exists => TRUE);

    -- Index for Grafana queries
    CREATE INDEX IF NOT EXISTS idx_1min_symbol ON trade_metrics_1min (symbol, window_start DESC);
    CREATE INDEX IF NOT EXISTS idx_5min_symbol ON trade_metrics_5min (symbol, window_start DESC);
    """
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(ddl)
        conn.commit()
    print("Schema initialized.")


if __name__ == "__main__":
    init_schema()
