
from __future__ import annotations

import os

import pandas as pd
import psycopg2

from dotenv import load_dotenv


def get_risk_connection():
    load_dotenv()

    return psycopg2.connect(
        host=os.getenv("PAYSTREAM_DB_HOST", "localhost"),
        port=int(os.getenv("PAYSTREAM_DB_PORT", "5433")),
        dbname=os.getenv("PAYSTREAM_DB_NAME", "paystream"),
        user=os.getenv("PAYSTREAM_DB_USER", "paystream"),
        password=os.getenv("PAYSTREAM_DB_PASSWORD"),
        connect_timeout=10,
    )


def get_latest_risk_run():
    query = """
        SELECT *
        FROM risk_analysis_runs
        ORDER BY started_at DESC
        LIMIT 1
    """

    with get_risk_connection() as connection:
        return pd.read_sql_query(query, connection)


def get_latest_risk_scores():
    query = """
        SELECT DISTINCT ON (s.transaction_id)
            s.transaction_id,
            s.payment_rail,
            s.amount_minor,
            s.latest_status,
            s.risk_score,
            s.risk_category,
            s.is_anomaly,
            s.risk_flags,
            s.explanation,
            s.analysed_at

        FROM transaction_risk_scores s

        ORDER BY
            s.transaction_id,
            s.analysed_at DESC,
            s.run_id DESC
    """

    with get_risk_connection() as connection:
        return pd.read_sql_query(query, connection)


def get_risk_trends():
    query = """
        SELECT
            DATE_TRUNC('day', started_at) AS analysis_date,

            AVG(
                high_risk_count + critical_risk_count
            ) AS high_risk_transactions,

            AVG(anomaly_count) AS anomalies,

            AVG(analysed_transactions) AS analysed_transactions

        FROM risk_analysis_runs

        WHERE status = 'COMPLETED'

        GROUP BY 1

        ORDER BY 1
    """

    with get_risk_connection() as connection:
        return pd.read_sql_query(query, connection)
