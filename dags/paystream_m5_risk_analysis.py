
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from src.orchestration.feature_engineering import (
    rebuild_transaction_features,
)

from src.orchestration.risk_pipeline import (
    get_database_connection,
)

from src.orchestration.risk_engine import (
    run_risk_analysis,
)


def execute_feature_engineering():
    connection = get_database_connection()

    try:
        count = rebuild_transaction_features(connection)

        print(
            f"Feature engineering completed: {count} transactions"
        )

    finally:
        connection.close()


def execute_risk_analysis():
    connection = get_database_connection()

    try:
        report = run_risk_analysis(connection)

        print(f"Risk analysis report: {report}")

    finally:
        connection.close()


default_args = {
    "owner": "paystream",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}


with DAG(
    dag_id="paystream_m5_risk_analysis",
    description="Transaction feature engineering and risk analysis",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule="0 * * * *",
    catchup=False,
    max_active_runs=1,
    tags=["paystream", "analytics", "risk"],
) as dag:

    feature_engineering = PythonOperator(
        task_id="build_transaction_features",
        python_callable=execute_feature_engineering,
    )

    risk_analysis = PythonOperator(
        task_id="execute_transaction_risk_analysis",
        python_callable=execute_risk_analysis,
    )

    feature_engineering >> risk_analysis
