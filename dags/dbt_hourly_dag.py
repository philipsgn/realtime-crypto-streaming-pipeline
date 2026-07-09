"""
Run dbt models (Bronze, Silver, Gold) hourly.
Schedule: @hourly
"""
import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator


def required_env(name: str) -> str:
    """Return a required environment variable or fail DAG parsing clearly."""
    value = os.getenv(name)
    if value is None:
        raise ValueError(f"{name} env var is required, not set")
    return value


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
    # Airflow container mounts dbt_project at DBT_PROJECT_DIR.
    run_dbt = BashOperator(
        task_id='run_dbt_models',
        bash_command=(
            'cd "$DBT_PROJECT_DIR" && '
            'dbt deps --profiles-dir "$DBT_PROFILES_DIR" --target "$DBT_TARGET" && '
            'dbt run --profiles-dir "$DBT_PROFILES_DIR" --target "$DBT_TARGET" && '
            'dbt test --profiles-dir "$DBT_PROFILES_DIR" --target "$DBT_TARGET"'
        ),
        env={
            'DBT_PROJECT_DIR': os.getenv('DBT_PROJECT_DIR', '/opt/airflow/dbt_project'),
            'DBT_PROFILES_DIR': os.getenv('DBT_PROFILES_DIR', '.'),
            'DBT_TARGET': os.getenv('DBT_TARGET', 'dev'),
            'POSTGRES_HOST': os.getenv('POSTGRES_HOST', 'postgres'),
            'POSTGRES_PORT': os.getenv('POSTGRES_PORT', '5432'),
            'POSTGRES_DB': os.getenv('POSTGRES_DB', 'crypto_pipeline'),
            'POSTGRES_USER': os.getenv('POSTGRES_USER', 'pipeline'),
            'POSTGRES_PASSWORD': required_env('POSTGRES_PASSWORD'),
        },
        append_env=True,
    )
