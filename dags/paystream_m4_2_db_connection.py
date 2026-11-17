from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from paystream_db_connection import check_paystream_database


def run_database_check():

    result = check_paystream_database()

    print("PostgreSQL connection successful!")

    print(f"Database: {result['database']}")
    print(f"User: {result['user']}")
    print(f"Server Time: {result['server_time']}")


with DAG(
    dag_id="paystream_m4_2_db_connection",

    description="Check PayStream PostgreSQL connectivity",

    start_date=datetime(2026, 1, 1),

    schedule=None,

    catchup=False,

    default_args={
        "owner": "paystream",
        "retries": 2,
        "retry_delay": timedelta(seconds=30)
    },

    tags=["paystream", "database"]

) as dag:

    check_postgres = PythonOperator(
        task_id="check_paystream_postgres_connection",
        python_callable=run_database_check
    )