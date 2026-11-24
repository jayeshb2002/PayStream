
import logging
import os
import socket
from contextlib import closing

import pandas as pd
import psycopg2
from dotenv import load_dotenv


load_dotenv()

logger = logging.getLogger(__name__)


def get_database_connection():
    """Connect to the PayStream PostgreSQL database."""

    return psycopg2.connect(
        host=os.getenv("PAYSTREAM_DB_HOST", "localhost"),
        port=int(os.getenv("PAYSTREAM_DB_PORT", "5433")),
        dbname=os.getenv("PAYSTREAM_DB_NAME", "paystream"),
        user=os.getenv("PAYSTREAM_DB_USER", "paystream"),
        password=os.getenv("PAYSTREAM_DB_PASSWORD", "password"),
        connect_timeout=5
    )


def read_query(query, params=None):
    """Execute a read-only dashboard query and return a DataFrame."""

    with closing(get_database_connection()) as connection:
        return pd.read_sql_query(
            query,
            connection,
            params=params
        )


def get_dashboard_summary():
    """Retrieve the main dashboard KPI values."""

    query = """
        SELECT
            COALESCE(SUM(
                CASE
                    WHEN component = 'spark_processor'
                     AND metric_name = 'records_processed'
                    THEN metric_value
                    ELSE 0
                END
            ), 0) AS records_processed,

            COALESCE(SUM(
                CASE
                    WHEN component = 'spark_processor'
                     AND metric_name = 'records_rejected'
                    THEN metric_value
                    ELSE 0
                END
            ), 0) AS records_rejected,

            COALESCE(SUM(
                CASE
                    WHEN component = 'spark_processor'
                     AND metric_name = 'records_late'
                    THEN metric_value
                    ELSE 0
                END
            ), 0) AS records_late,

            COALESCE(SUM(
                CASE
                    WHEN component = 'postgres_sink'
                     AND metric_name IN (
                         'records_submitted',
                         'records_stored'
                     )
                    THEN metric_value
                    ELSE 0
                END
            ), 0) AS records_submitted

        FROM pipeline_metrics
        WHERE recorded_at >= CURRENT_TIMESTAMP - INTERVAL '24 hours'
    """

    metrics = read_query(query)

    quality = read_query(
        """
        SELECT quality_score, status, created_at
        FROM data_quality_runs
        ORDER BY created_at DESC
        LIMIT 1
        """
    )

    failures = read_query(
        """
        SELECT
            COUNT(*) FILTER (WHERE status = 'OPEN') AS open_failures,
            COUNT(*) FILTER (
                WHERE status = 'OPEN'
                  AND severity = 'CRITICAL'
            ) AS critical_failures
        FROM pipeline_failures
        """
    )

    metric_row = metrics.iloc[0].to_dict()
    quality_row = quality.iloc[0].to_dict() if not quality.empty else {}
    failure_row = failures.iloc[0].to_dict()

    return {
        **metric_row,
        "quality_score": quality_row.get("quality_score"),
        "quality_status": quality_row.get("status", "NO DATA"),
        "quality_updated_at": quality_row.get("created_at"),
        **failure_row
    }


def get_processing_trends(hours=24):
    """Get processing metrics grouped by time."""

    query = """
        SELECT
            DATE_TRUNC('hour', recorded_at) AS metric_hour,

            SUM(metric_value) FILTER (
                WHERE component = 'spark_processor'
                  AND metric_name = 'records_processed'
            ) AS records_processed,

            SUM(metric_value) FILTER (
                WHERE component = 'spark_processor'
                  AND metric_name = 'records_rejected'
            ) AS records_rejected,

            SUM(metric_value) FILTER (
                WHERE component = 'spark_processor'
                  AND metric_name = 'records_late'
            ) AS records_late,

            SUM(metric_value) FILTER (
                WHERE component = 'postgres_sink'
                  AND metric_name IN (
                      'records_submitted',
                      'records_stored'
                  )
            ) AS records_submitted

        FROM pipeline_metrics
        WHERE recorded_at >= CURRENT_TIMESTAMP - (%s * INTERVAL '1 hour')
        GROUP BY 1
        ORDER BY 1
    """

    return read_query(query, (hours,))


def get_quality_trends(days=7):
    """Retrieve historical data quality scores."""

    query = """
        SELECT
            created_at,
            quality_score,
            threshold_score,
            status,
            total_records,
            passed_checks,
            failed_checks
        FROM data_quality_runs
        WHERE created_at >= CURRENT_TIMESTAMP - (%s * INTERVAL '1 day')
        ORDER BY created_at
    """

    return read_query(query, (days,))


def get_latest_quality_checks():
    """Retrieve individual results from the most recent quality run."""

    query = """
        SELECT
            r.run_id,
            r.created_at,
            c.check_name,
            c.failed_records,
            c.status
        FROM data_quality_check_results c
        JOIN data_quality_runs r
          ON r.run_id = c.run_id
        WHERE r.created_at = (
            SELECT MAX(created_at)
            FROM data_quality_runs
        )
        ORDER BY c.check_name
    """

    return read_query(query)


def get_failure_summary():
    """Retrieve open failures grouped by component and severity."""

    query = """
        SELECT
            component,
            severity,
            COUNT(*) AS failure_count
        FROM pipeline_failures
        WHERE status = 'OPEN'
        GROUP BY component, severity
        ORDER BY
            CASE severity
                WHEN 'CRITICAL' THEN 1
                WHEN 'HIGH' THEN 2
                WHEN 'MEDIUM' THEN 3
                WHEN 'LOW' THEN 4
            END,
            component
    """

    return read_query(query)


def get_recent_failures(limit=20):
    """Retrieve the latest failure incidents."""

    query = """
        SELECT
            component,
            failure_type,
            severity,
            error_message,
            status,
            occurrence_count,
            first_seen,
            last_seen,
            resolved_at
        FROM pipeline_failures
        ORDER BY last_seen DESC
        LIMIT %s
    """

    return read_query(query, (limit,))


def check_tcp_service(host, port, timeout=3):
    """Check whether a TCP service accepts connections."""

    if not host:
        return {
            "status": "NOT CONFIGURED",
            "details": "Host is not configured"
        }

    try:
        with socket.create_connection(
            (host, int(port)),
            timeout=timeout
        ):
            return {
                "status": "HEALTHY",
                "details": f"Connection successful: {host}:{port}"
            }

    except Exception as exc:
        return {
            "status": "UNHEALTHY",
            "details": str(exc)
        }


def get_live_component_health():
    """Check connectivity to live infrastructure services."""

    kafka = check_tcp_service(
        os.getenv("KAFKA_BOOTSTRAP_SERVERS_HOST", "kafka"),
        os.getenv("KAFKA_BOOTSTRAP_SERVERS_PORT", "9092")
    )

    postgres = check_tcp_service(
        os.getenv("PAYSTREAM_DB_HOST", "localhost"),
        os.getenv("PAYSTREAM_DB_PORT", "5433")
    )

    airflow_db = check_tcp_service(
        os.getenv("AIRFLOW_DB_HOST", "airflow-db"),
        os.getenv("AIRFLOW_DB_PORT", "5432")
    )

    return {
        "Kafka": kafka,
        "PayStream PostgreSQL": postgres,
        "Airflow Metadata DB": airflow_db
    }
