from datetime import datetime, timezone

from airflow import DAG
from airflow.operators.python import PythonOperator


def check_runtime():
    print("PayStream Airflow is running.")
    print("The scheduler successfully started this DAG.")


def orchestration_ready():
    print("Milestone 4.1 smoke test completed.")
    print("Next: orchestrate PayStream validation and analytics workflows.")


with DAG(
    dag_id="paystream_m4_smoke_test",
    description="Checks that the PayStream Airflow installation can execute tasks.",
    start_date=datetime(2025, 1, 1, tzinfo=timezone.utc),
    schedule=None,
    catchup=False,
    tags=["paystream", "milestone-4", "setup"],
) as dag:
    runtime_check = PythonOperator(
        task_id="check_airflow_runtime",
        python_callable=check_runtime,
    )

    ready_check = PythonOperator(
        task_id="orchestration_ready",
        python_callable=orchestration_ready,
    )

    runtime_check >> ready_check
