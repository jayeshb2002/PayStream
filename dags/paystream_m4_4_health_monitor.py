
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from src.orchestration.pipeline_health import (
    check_kafka,
    check_paystream_postgres,
    check_airflow_metadata_database,
)


default_args = {
    "owner": "paystream",
    "retries": 2,
    "retry_delay": timedelta(seconds=30),
}


with DAG(
    dag_id="paystream_m4_4_health_monitor",
    description="Monitor PayStream infrastructure health",
    start_date=datetime(2026, 1, 1),
    schedule="*/5 * * * *",
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    dagrun_timeout=timedelta(minutes=5),
    tags=["paystream", "monitoring", "health"],
) as dag:

    kafka_health = PythonOperator(
        task_id="check_kafka_health",
        python_callable=check_kafka,
        execution_timeout=timedelta(seconds=15),
    )

    postgres_health = PythonOperator(
        task_id="check_paystream_postgres_health",
        python_callable=check_paystream_postgres,
        execution_timeout=timedelta(seconds=15),
    )

    airflow_health = PythonOperator(
        task_id="check_airflow_health",
        python_callable=check_airflow_metadata_database,
        execution_timeout=timedelta(seconds=15),
    )

    # Independent health checks.
    # One failure does not prevent the other checks from running.
    kafka_health
    postgres_health
    airflow_health
