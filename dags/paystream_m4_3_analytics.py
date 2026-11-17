
import logging
import os

from datetime import datetime, timezone, timedelta

import psycopg2

from airflow import DAG
from airflow.operators.python import PythonOperator

from src.orchestration.analytics import (
    generate_daily_analytics,
    generate_bank_analytics
)


logger = logging.getLogger(__name__)


def get_database_connection():
    return psycopg2.connect(
        host=os.environ["PAYSTREAM_DB_HOST"],
        port=int(os.getenv("PAYSTREAM_DB_PORT", "5432")),
        dbname=os.environ["PAYSTREAM_DB_NAME"],
        user=os.environ["PAYSTREAM_DB_USER"],
        password=os.environ["PAYSTREAM_DB_PASSWORD"],
        connect_timeout=5
    )


def run_daily_analytics(target_date):
    connection = get_database_connection()

    try:
        result = generate_daily_analytics(connection, target_date)
        logger.info("Daily analytics result: %s", result)
        return result

    finally:
        connection.close()


def run_bank_analytics(target_date):
    connection = get_database_connection()

    try:
        result = generate_bank_analytics(connection, target_date)
        logger.info("Bank analytics result: %s", result)
        return result

    finally:
        connection.close()


with DAG(
    dag_id="paystream_m4_3_analytics",
    description="Generate daily and bank-wise PayStream payment analytics",
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    schedule="15 0 * * *",
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "paystream",
        "retries": 2,
        "retry_delay": timedelta(minutes=2)
    },
    tags=["paystream", "analytics", "postgres"]
) as dag:

    daily_analytics = PythonOperator(
        task_id="generate_daily_analytics",
        python_callable=run_daily_analytics,
        op_kwargs={
            "target_date": "{{ data_interval_start | ds }}"
        }
    )

    bank_analytics = PythonOperator(
        task_id="generate_bank_analytics",
        python_callable=run_bank_analytics,
        op_kwargs={
            "target_date": "{{ data_interval_start | ds }}"
        }
    )

    daily_analytics
    bank_analytics
