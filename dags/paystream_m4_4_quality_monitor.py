
import logging

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from src.orchestration.quality_monitor import run_quality_monitor


logger = logging.getLogger(__name__)


def monitor_data_quality():
    result = run_quality_monitor()

    logger.info("PAYSTREAM DATA QUALITY MONITORING")
    logger.info("=================================")
    logger.info("Run ID: %s", result["run_id"])
    logger.info("Total records: %s", result["total_records"])
    logger.info("Total checks: %s", result["total_checks"])
    logger.info("Passed checks: %s", result["passed_checks"])
    logger.info("Failed checks: %s", result["failed_checks_count"])
    logger.info("Quality score: %s%%", result["quality_score"])
    logger.info("Required threshold: %s%%", result["threshold_score"])
    logger.info(
        "Execution duration: %s ms",
        result["execution_duration_ms"]
    )
    logger.info("Status: %s", result["status"])

    if result["failed_checks"]:
        logger.warning("Failed data quality validations:")

        for name, count in result["failed_checks"].items():
            logger.warning("%s: %s invalid records", name, count)

    if result["status"] == "WARNING":
        raise ValueError(
            f"Data quality score {result['quality_score']}% "
            f"is below the required threshold "
            f"of {result['threshold_score']}%"
        )

    logger.info("Data quality monitoring completed successfully.")


with DAG(
    dag_id="paystream_m4_4_quality_monitor",
    description="Monitor PayStream data quality and track historical results",
    start_date=datetime(2026, 1, 1),
    schedule="*/15 * * * *",
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "paystream",
        "retries": 0
    },
    tags=[
        "paystream",
        "monitoring",
        "data-quality",
        "observability"
    ]
) as dag:

    quality_monitor = PythonOperator(
        task_id="monitor_data_quality",
        python_callable=monitor_data_quality,
        execution_timeout=timedelta(minutes=5)
    )
