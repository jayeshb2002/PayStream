
import logging

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from src.orchestration.failure_monitor import scan_airflow_task_failures


logger = logging.getLogger(__name__)


def monitor_airflow_failures():
    result = scan_airflow_task_failures(hours=24)

    logger.info("PAYSTREAM FAILURE MONITORING")
    logger.info("============================")
    logger.info(
        "Discovered failures: %s",
        result["discovered_failures"]
    )
    logger.info(
        "Recorded failures: %s",
        result["recorded_failures"]
    )


with DAG(
    dag_id="paystream_m4_4_failure_monitor",
    description="Discover and record PayStream Airflow task failures",
    start_date=datetime(2026, 1, 1),
    schedule="*/5 * * * *",
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "paystream",
        "retries": 1,
        "retry_delay": timedelta(seconds=30)
    },
    tags=[
        "paystream",
        "monitoring",
        "failure-tracking",
        "observability"
    ]
) as dag:

    scan_failures = PythonOperator(
        task_id="scan_airflow_task_failures",
        python_callable=monitor_airflow_failures,
        execution_timeout=timedelta(minutes=3)
    )
