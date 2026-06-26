"""
Log a formatted daily summary of the crypto market.
Schedule: @daily
"""
import logging
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from utils.gold_queries import fetch_daily_summary

log = logging.getLogger(__name__)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def log_daily_summary():
    """Fetch daily summary from TimescaleDB and log it."""
    summaries = fetch_daily_summary()
    
    if not summaries:
        log.warning("no data for yesterday")
        return
        
    log.info("=== DAILY CRYPTO MARKET SUMMARY ===")
    for s in summaries:
        log.info(
            f"Symbol: {s['symbol']} | "
            f"VWAP: {s['daily_vwap']:.2f} | "
            f"Vol: {s['daily_volume']:.2f} | "
            f"High/Low: {s['daily_high']:.2f}/{s['daily_low']:.2f} | "
            f"Change: {s['daily_price_change_pct']:.2f}%"
        )
    log.info("===================================")

with DAG(
    'daily_summary_dag',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
    tags=['reporting'],
) as dag:

    summary_task = PythonOperator(
        task_id='log_daily_summary',
        python_callable=log_daily_summary,
    )
