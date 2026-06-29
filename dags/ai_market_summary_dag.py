"""Generate resilient Gemini market summaries every 30 minutes."""

from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

from ai.gemini_summary import generate_market_summaries

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "retries": 0,
}

with DAG(
    "ai_market_summary_dag",
    default_args=default_args,
    schedule_interval="*/30 * * * *",
    catchup=False,
    tags=["ai", "gemini", "reporting"],
) as dag:
    generate_summaries = PythonOperator(
        task_id="generate_market_summaries",
        python_callable=generate_market_summaries,
    )
