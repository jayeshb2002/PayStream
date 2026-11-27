
"""PayStream Milestone 5.5: Automated Transaction Risk Analysis."""

import pendulum

from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import timedelta


default_args = {
    "owner": "paystream",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}


with DAG(
    dag_id="paystream_m5_5_automated_risk_analysis",
    description=(
        "Automated transaction feature engineering, "
        "anomaly detection, risk scoring and risk storage"
    ),
    default_args=default_args,
    start_date=pendulum.datetime(2026, 11, 27, tz="UTC"),
    schedule="*/30 * * * *",
    catchup=False,
    max_active_runs=1,
    max_active_tasks=1,
    tags=["paystream", "risk-analysis", "milestone-5"],
) as dag:

    feature_engineering = BashOperator(
        task_id="transaction_feature_engineering",
        bash_command=(
            "python -m src.orchestration.transaction_features"
        ),
        cwd="/opt/airflow",
        append_env=True,
        env={
            "PYTHONPATH": "/opt/airflow",
        },
        execution_timeout=timedelta(minutes=10),
    )

    anomaly_detection = BashOperator(
        task_id="statistical_anomaly_detection",
        bash_command=(
            "python -m src.orchestration.anomaly_detection"
        ),
        cwd="/opt/airflow",
        append_env=True,
        env={
            "PYTHONPATH": "/opt/airflow",
        },
        execution_timeout=timedelta(minutes=10),
    )

    risk_scoring = BashOperator(
        task_id="transaction_risk_scoring",
        bash_command=(
            "python -m src.orchestration.risk_scoring"
        ),
        cwd="/opt/airflow",
        append_env=True,
        env={
            "PYTHONPATH": "/opt/airflow",
        },
        execution_timeout=timedelta(minutes=10),
    )

    # Execute stages in strict order.
    (
        feature_engineering
        >> anomaly_detection
        >> risk_scoring
    )
