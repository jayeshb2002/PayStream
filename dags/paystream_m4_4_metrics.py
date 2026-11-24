
import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from src.orchestration.metrics_collector import get_recent_metrics

logger = logging.getLogger(__name__)


def report_pipeline_metrics():

    rows = get_recent_metrics(hours=24)

    if not rows:
        logger.warning(
            "No pipeline metrics found in the last 24 hours"
        )
        return

    logger.info(
        "PayStream processing metrics for the last 24 hours"
    )

    for row in rows:

        (
            component,
            metric_name,
            total_value,
            batch_count,
            last_recorded_at,
        ) = row

        logger.info(
            "component=%s metric=%s total=%s batches=%s last_recorded=%s",
            component,
            metric_name,
            total_value,
            batch_count,
            last_recorded_at,
        )


with DAG(
    dag_id="paystream_m4_4_metrics",
    description="Hourly PayStream processing metrics report",
    start_date=datetime(2026, 1, 1),
    schedule="0 * * * *",
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "paystream",
        "retries": 2,
        "retry_delay": timedelta(seconds=30),
    },
    tags=["paystream", "monitoring", "metrics"],
) as dag:

    report_metrics = PythonOperator(
        task_id="report_pipeline_metrics",
        python_callable=report_pipeline_metrics,
        execution_timeout=timedelta(minutes=2),
    )
