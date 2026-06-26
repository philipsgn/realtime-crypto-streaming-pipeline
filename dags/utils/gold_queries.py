import os
import logging
import psycopg2
from psycopg2.extras import RealDictCursor

log = logging.getLogger(__name__)

def get_db_connection():
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
        cursor_factory=RealDictCursor
    )

def fetch_daily_summary() -> list:
    """Fetch the latest daily summary for all symbols from the Gold table."""
    query = """
        SELECT symbol, day_start, daily_vwap, daily_volume, daily_high, daily_low, daily_price_change_pct
        FROM gold_daily_summary
        WHERE day_start = (SELECT MAX(day_start) FROM gold_daily_summary)
    """
    
    summaries = []
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query)
                summaries = cur.fetchall()
    except Exception:
        log.exception("Database query failed")
        raise
        
    return summaries
