
from datetime import datetime, timedelta
import os

import psycopg2

from airflow import DAG
from airflow.operators.python import PythonOperator

from src.orchestration.data_quality import run_data_quality_checks


def get_database_connection():
    return psycopg2.connect(
        host=os.environ["PAYSTREAM_DB_HOST"],
        port=int(os.getenv("PAYSTREAM_DB_PORT", "5432")),
        dbname=os.environ["PAYSTREAM_DB_NAME"],
        user=os.environ["PAYSTREAM_DB_USER"],
        password=os.environ["PAYSTREAM_DB_PASSWORD"],
        connect_timeout=5
    )


def check_database_connection():
    connection = get_database_connection()

    try:
        with connection.cursor() as cursor:
            cursor.execute("SELECT current_database()")
            database = cursor.fetchone()[0]

        print(f"Connected to database: {database}")

    finally:
        connection.close()


def execute_data_quality_checks():
    connection = get_database_connection()

    try:
        report = run_data_quality_checks(connection)

        print("PAYSTREAM DATA QUALITY REPORT")
        print("=============================")
        print(f"Total records: {report['total_records']}")
        print(f"Total checks: {report['total_checks']}")
        print(f"Passed checks: {report['passed_checks']}")
        print(f"Failed checks: {report['failed_checks_count']}")

        if report["failed_checks"]:
            print("Failed validations:")

            for name, count in report["failed_checks"].items():
                print(f"{name}: {count} records")

            raise ValueError("PayStream data quality validation failed")

        print("All data quality checks passed!")

    finally:
        connection.close()


with DAG(
    dag_id="paystream_data_quality",
    description="Validate PayStream payment data in PostgreSQL",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    default_args={
        "owner": "paystream",
        "retries": 2,
        "retry_delay": timedelta(seconds=30)
    },
    tags=["paystream", "data-quality", "postgres"]
) as dag:

    database_check = PythonOperator(
        task_id="check_database_connection",
        python_callable=check_database_connection
    )

    quality_check = PythonOperator(
        task_id="execute_data_quality_checks",
        python_callable=execute_data_quality_checks
    )

    database_check >> quality_check
