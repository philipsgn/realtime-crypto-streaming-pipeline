"""
Monitor pipeline health via Gold table data freshness and symbol coverage.

Replaces the unreliable Kafka consumer group lag check.
Schedule: every 5 minutes.

Manual acceptance test:
1. Run ``docker compose -f infrastructure/docker-compose.yml stop spark``.
2. Wait at least 10 minutes.
3. Trigger this DAG manually; the task must fail with ``AirflowException``.
4. Run ``docker compose -f infrastructure/docker-compose.yml start spark``.
5. Wait 5 minutes for Spark to write new records.
6. Trigger this DAG again; the task must succeed.
"""

import logging
import os
from datetime import datetime, timedelta

import psycopg2
from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.operators.python import PythonOperator
from psycopg2.extensions import connection

log = logging.getLogger(__name__)

FRESHNESS_WARN_SECONDS = 300
FRESHNESS_ERROR_SECONDS = 600
SYMBOL_COVERAGE_MIN = 8


def get_postgres_conn() -> connection:
    """Create a PostgreSQL connection from environment configuration."""
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "postgres"),
        port=int(os.getenv("POSTGRES_PORT", "5432")),
        dbname=os.getenv("POSTGRES_DB", "crypto_pipeline"),
        user=os.getenv("POSTGRES_USER", "pipeline"),
        password=os.getenv("POSTGRES_PASSWORD"),
    )


def check_pipeline_health() -> None:
    """Fail the task when Gold data is stale or recent symbol coverage drops."""
    conn = get_postgres_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT EXTRACT(EPOCH FROM (NOW() - MAX(window_start)))
                FROM gold_minute_volume
                """
            )
            freshness_row = cur.fetchone()
            seconds_stale = (
                float(freshness_row[0])
                if freshness_row and freshness_row[0] is not None
                else 9999.0
            )

            cur.execute(
                """
                SELECT COUNT(DISTINCT symbol)
                FROM gold_minute_volume
                WHERE window_start >= NOW() - INTERVAL '10 minutes'
                """
            )
            coverage_row = cur.fetchone()
            active_symbols = int(coverage_row[0]) if coverage_row else 0

        if seconds_stale >= FRESHNESS_ERROR_SECONDS:
            raise AirflowException(
                f"PIPELINE DOWN: no Gold data for {seconds_stale:.0f}s "
                f"(threshold={FRESHNESS_ERROR_SECONDS}s)"
            )
        if active_symbols < SYMBOL_COVERAGE_MIN:
            raise AirflowException(
                f"SYMBOL DROPOUT: only {active_symbols}/{SYMBOL_COVERAGE_MIN} "
                "symbols active in the last 10 minutes"
            )
        if seconds_stale >= FRESHNESS_WARN_SECONDS:
            log.warning("Pipeline slow: %.0fs since last Gold record", seconds_stale)
        else:
            log.info(
                "Pipeline healthy: %.0fs stale, %d/%d symbols active",
                seconds_stale,
                active_symbols,
                SYMBOL_COVERAGE_MIN,
            )
    finally:
        conn.close()


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}


with DAG(
    "kafka_lag_monitor_dag",
    default_args=default_args,
    schedule=timedelta(minutes=5),
    catchup=False,
    tags=["monitoring"],
) as dag:
    check_pipeline_health_task = PythonOperator(
        task_id="check_pipeline_health",
        python_callable=check_pipeline_health,
    )
