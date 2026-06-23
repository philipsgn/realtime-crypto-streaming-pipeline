"""
Run dbt models (Bronze, Silver, Gold) hourly.
Schedule: @hourly
"""
import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

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
        bash_command=(
            'cd /opt/airflow/dbt_project && '
            'dbt deps --profiles-dir . && '
            'dbt run --profiles-dir . && '
            'dbt test --profiles-dir .'
        ),
        env={
            'POSTGRES_HOST': os.getenv('POSTGRES_HOST', 'postgres'),
            'POSTGRES_PORT': os.getenv('POSTGRES_PORT', '5432'),
            'POSTGRES_DB': os.getenv('POSTGRES_DB', 'crypto_pipeline'),
            'POSTGRES_USER': os.getenv('POSTGRES_USER', 'pipeline'),
            'POSTGRES_PASSWORD': os.getenv('POSTGRES_PASSWORD', 'changeme'),
        },
        append_env=True,
    )
