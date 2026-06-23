"""
Run dbt models (Bronze, Silver, Gold) hourly.
Schedule: @hourly
"""
import os
from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'dbt_hourly_dag',
    default_args=default_args,
    schedule_interval='@hourly',
    catchup=False,
    tags=['dbt'],
) as dag:

    # Run dbt deps, run, and test sequentially.
    # Airflow container mounts dbt_project at /opt/airflow/dbt_project
    run_dbt = BashOperator(
        task_id='run_dbt_models',
        bash_command='cd /opt/airflow/dbt_project && dbt deps && dbt run && dbt test',
    )
