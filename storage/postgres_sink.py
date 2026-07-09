"""Initialize and migrate the Stage 3 TimescaleDB schema."""

import logging
import os

import psycopg2
from psycopg2.extensions import connection

log = logging.getLogger(__name__)


def get_conn() -> connection:
    """Create a PostgreSQL connection from environment configuration."""
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=int(os.getenv("POSTGRES_PORT", "5432")),
        dbname=os.getenv("POSTGRES_DB", "crypto_pipeline"),
        user=os.getenv("POSTGRES_USER", "pipeline"),
        password=os.getenv("POSTGRES_PASSWORD"),
    )


def init_schema() -> None:
    """Create TimescaleDB hypertables and query indexes."""
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

    SELECT create_hypertable('trade_metrics_1min', 'window_start', if_not_exists => TRUE);
    SELECT create_hypertable('trade_metrics_5min', 'window_start', if_not_exists => TRUE);

    CREATE INDEX IF NOT EXISTS idx_1min_symbol
        ON trade_metrics_1min (symbol, window_start DESC);
    CREATE INDEX IF NOT EXISTS idx_5min_symbol
        ON trade_metrics_5min (symbol, window_start DESC);
    """
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(ddl)
    log.info("Schema initialized.")


def deduplicate_existing() -> None:
    """Remove duplicate metric windows while retaining one complete row per grain."""
    sql = """
    DELETE FROM trade_metrics_1min AS older
    USING trade_metrics_1min AS newer
    WHERE older.tableoid = newer.tableoid
      AND older.ctid < newer.ctid
      AND older.window_start = newer.window_start
      AND older.symbol = newer.symbol;

    DELETE FROM trade_metrics_5min AS older
    USING trade_metrics_5min AS newer
    WHERE older.tableoid = newer.tableoid
      AND older.ctid < newer.ctid
      AND older.window_start = newer.window_start
      AND older.symbol = newer.symbol;
    """
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql)
    log.info("Deduplication complete.")


def add_unique_constraints() -> None:
    """Add idempotent uniqueness constraints for streaming window grains."""
    ddl = """
    DO $$
    BEGIN
        IF NOT EXISTS (
            SELECT 1
            FROM pg_constraint
            WHERE conname = 'uq_1min_window'
              AND conrelid = 'trade_metrics_1min'::regclass
        ) THEN
            ALTER TABLE trade_metrics_1min
                ADD CONSTRAINT uq_1min_window UNIQUE (window_start, symbol);
        END IF;
    END
    $$;

    DO $$
    BEGIN
        IF NOT EXISTS (
            SELECT 1
            FROM pg_constraint
            WHERE conname = 'uq_5min_window'
              AND conrelid = 'trade_metrics_5min'::regclass
        ) THEN
            ALTER TABLE trade_metrics_5min
                ADD CONSTRAINT uq_5min_window UNIQUE (window_start, symbol);
        END IF;
    END
    $$;
    """
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(ddl)
    log.info("Unique streaming window constraints are present.")


# Verify zero duplicates:
# SELECT window_start, symbol, COUNT(*) FROM trade_metrics_1min
#   GROUP BY 1,2 HAVING COUNT(*) > 1;  -> must return 0 rows
# Verify constraint exists:
# SELECT conname FROM pg_constraint WHERE conname LIKE 'uq_%';


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    init_schema()
    deduplicate_existing()
    add_unique_constraints()
