
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


def get_risk_dashboard_summary():
    """Return high-level KPIs from the latest risk assessment."""

    query = """
        SELECT
            COUNT(*) AS total_transactions,

            COUNT(*) FILTER (
                WHERE is_flagged = TRUE
            ) AS flagged_transactions,

            COUNT(*) FILTER (
                WHERE risk_category = 'HIGH'
            ) AS high_risk_transactions,

            COUNT(*) FILTER (
                WHERE risk_category = 'CRITICAL'
            ) AS critical_risk_transactions,

            ROUND(AVG(risk_score)::NUMERIC, 2)
                AS average_risk_score

        FROM latest_transaction_risk;
    """

    return read_query(query)


def get_risk_category_distribution():
    """Return the latest transaction risk category distribution."""

    query = """
        SELECT
            risk_category,
            COUNT(*) AS transaction_count

        FROM latest_transaction_risk

        GROUP BY risk_category

        ORDER BY
            CASE risk_category
                WHEN 'LOW' THEN 1
                WHEN 'MEDIUM' THEN 2
                WHEN 'HIGH' THEN 3
                WHEN 'CRITICAL' THEN 4
            END;
    """

    return read_query(query)


def get_highest_risk_transactions(limit=20):
    """Return the highest-risk transactions."""

    limit = max(1, min(int(limit), 100))

    query = f"""
        SELECT
            r.transaction_id,
            r.risk_score,
            r.risk_category,
            r.risk_flag_count,
            r.is_flagged,
            r.risk_flags,
            r.analyzed_at,

            f.payment_rail,
            f.currency,
            f.amount_minor,
            f.latest_status,
            f.failed_event_count

        FROM latest_transaction_risk r

        INNER JOIN transaction_features f
            ON r.transaction_id = f.transaction_id

        ORDER BY
            r.risk_score DESC,
            r.analyzed_at DESC

        LIMIT {limit};
    """

    return read_query(query)


def get_transaction_risk_details(transaction_id):
    """Return risk assessment history for a transaction."""

    safe_transaction_id = str(transaction_id).replace("'", "''")

    query = f"""
        SELECT
            r.run_id,
            r.transaction_id,
            r.risk_score,
            r.risk_category,
            r.risk_flags,
            r.risk_flag_count,
            r.is_flagged,
            r.analyzed_at

        FROM transaction_risk_results r

        WHERE r.transaction_id = '{safe_transaction_id}'

        ORDER BY r.analyzed_at DESC;
    """

    return read_query(query)


def get_risk_analysis_trends(limit=30):
    """Return risk category counts across recent analysis runs."""

    limit = max(1, min(int(limit), 100))

    query = f"""
        SELECT
            run_id,
            completed_at,
            total_transactions,
            flagged_transactions,
            low_risk_count,
            medium_risk_count,
            high_risk_count,
            critical_risk_count

        FROM risk_analysis_runs

        ORDER BY completed_at DESC

        LIMIT {limit};
    """

    return read_query(query)
