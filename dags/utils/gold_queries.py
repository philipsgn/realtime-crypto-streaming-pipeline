import logging
import os
from typing import Any

import psycopg2
from psycopg2.extensions import connection
from psycopg2.extras import RealDictCursor

log = logging.getLogger(__name__)

def get_db_connection() -> connection:
    """Create and return a TimescaleDB (PostgreSQL) connection."""
    host = os.getenv("POSTGRES_HOST", "localhost")
    port = os.getenv("POSTGRES_PORT", "5433")
    db = os.getenv("POSTGRES_DB", "crypto_pipeline")
    user = os.getenv("POSTGRES_USER", "pipeline")
    password = os.getenv("POSTGRES_PASSWORD", "changeme")
    
    log.info(f"Connecting to PostgreSQL at {host}:{port}/{db}")
    
    return psycopg2.connect(
        host=host,
        port=port,
        dbname=db,
        user=user,
        password=password,
        cursor_factory=RealDictCursor,
    )


def fetch_daily_summary() -> list[dict[str, Any]]:
    """Fetch the latest daily summary for all symbols from the Gold table."""
    query = """
        SELECT DISTINCT ON (symbol)
            symbol, day_start, daily_vwap, daily_volume, daily_high, daily_low,
            daily_price_change_pct
        FROM gold_daily_summary
        ORDER BY symbol, day_start DESC
    """
    
    summaries: list[dict[str, Any]] = []
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query)
                summaries = [dict(row) for row in cur.fetchall()]
    except Exception:
        log.exception("Database query failed")
        raise

    return summaries


def insert_market_summaries(rows: list[tuple[str, str, str]]) -> None:
    """Insert generated or fallback market summaries into PostgreSQL."""
    query = """
        INSERT INTO market_summaries (symbol, summary_text, source)
        VALUES (%s, %s, %s::summary_source)
    """

    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.executemany(query, rows)
    except Exception:
        log.exception("Failed to store market summaries")
        raise
