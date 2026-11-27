
"""Daily PayStream ML fraud detection pipeline."""

from datetime import timedelta

import pendulum

from airflow import DAG
from airflow.operators.bash import BashOperator


DEFAULT_ARGS = {
    "owner": "paystream",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


with DAG(
    dag_id="paystream_m6_7_ml_pipeline",
    description=(
        "Prepare ML dataset, train and evaluate Isolation Forest, "
        "persist predictions and integrate transaction risk."
    ),
    default_args=DEFAULT_ARGS,
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule="0 2 * * *",
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(hours=3),
    tags=["paystream", "machine-learning", "fraud-detection"],
) as dag:

    prepare_ml_dataset = BashOperator(
        task_id="prepare_ml_dataset",
        bash_command="python -m src.orchestration.ml_dataset",
        cwd="/opt/airflow",
        execution_timeout=timedelta(minutes=30),
    )

    train_isolation_forest = BashOperator(
        task_id="train_isolation_forest",
        bash_command="python -m src.orchestration.ml_model",
        cwd="/opt/airflow",
        execution_timeout=timedelta(minutes=45),
    )

    evaluate_ml_model = BashOperator(
        task_id="evaluate_ml_model",
        bash_command=(
            "python -m src.orchestration.ml_evaluation "
            "--target-percentile 95"
        ),
        cwd="/opt/airflow",
        execution_timeout=timedelta(minutes=30),
    )

    generate_and_persist_predictions = BashOperator(
        task_id="generate_and_persist_predictions",
        bash_command=(
            "python -m src.orchestration.ml_prediction_storage"
        ),
        cwd="/opt/airflow",
        execution_timeout=timedelta(minutes=45),
    )

    integrate_ml_risk = BashOperator(
        task_id="integrate_ml_risk",
        bash_command=(
            "python -m src.orchestration.ml_risk_integration "
            "--ml-adjustment-points 20"
        ),
        cwd="/opt/airflow",
        execution_timeout=timedelta(minutes=30),
    )

    (
        prepare_ml_dataset
        >> train_isolation_forest
        >> evaluate_ml_model
        >> generate_and_persist_predictions
        >> integrate_ml_risk
    )
