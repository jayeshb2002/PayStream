
"""Reusable PostgreSQL pipeline metrics collector."""

import logging
import os
from typing import Optional

import psycopg2
from psycopg2.extras import execute_values

logger = logging.getLogger(__name__)

INSERT_METRICS_SQL = """
    INSERT INTO pipeline_metrics (
        component,
        batch_id,
        metric_name,
        metric_value,
        processing_duration_ms
    )
    VALUES %s
    ON CONFLICT (component, batch_id, metric_name)
    DO UPDATE SET
        metric_value = EXCLUDED.metric_value,
        processing_duration_ms = EXCLUDED.processing_duration_ms,
        recorded_at = CURRENT_TIMESTAMP
"""


def get_metrics_connection():
    """Connect to the PayStream PostgreSQL database."""

    return psycopg2.connect(
        host=os.getenv("PAYSTREAM_DB_HOST", "localhost"),
        port=int(os.getenv("PAYSTREAM_DB_PORT", "5433")),
        dbname=os.environ["PAYSTREAM_DB_NAME"],
        user=os.environ["PAYSTREAM_DB_USER"],
        password=os.environ["PAYSTREAM_DB_PASSWORD"],
        connect_timeout=5,
    )


def record_batch_metrics(
    component: str,
    batch_id: int,
    metrics: dict,
    processing_duration_ms: Optional[int] = None,
):
    """Persist metrics for one component and Spark batch."""

    if not component:
        raise ValueError("component cannot be empty")

    if batch_id < 0:
        raise ValueError("batch_id cannot be negative")

    if processing_duration_ms is not None and processing_duration_ms < 0:
        raise ValueError("processing duration cannot be negative")

    rows = []

    for metric_name, metric_value in metrics.items():

        if not metric_name:
            raise ValueError("metric_name cannot be empty")

        if not isinstance(metric_value, int) or isinstance(metric_value, bool):
            raise ValueError(f"{metric_name} must be an integer")

        if metric_value < 0:
            raise ValueError(f"{metric_name} cannot be negative")

        rows.append((
            component,
            batch_id,
            metric_name,
            metric_value,
            processing_duration_ms,
        ))

    if not rows:
        return

    connection = get_metrics_connection()

    try:
        with connection:
            with connection.cursor() as cursor:
                execute_values(
                    cursor,
                    INSERT_METRICS_SQL,
                    rows,
                )

        logger.info(
            "Recorded %s metrics for component=%s batch_id=%s",
            len(rows),
            component,
            batch_id,
        )

    finally:
        connection.close()


def record_batch_metrics_safely(
    component: str,
    batch_id: int,
    metrics: dict,
    processing_duration_ms: Optional[int] = None,
):
    """
    Best-effort metrics recording.

    Metrics failures must not interrupt payment processing.
    """

    try:
        record_batch_metrics(
            component=component,
            batch_id=batch_id,
            metrics=metrics,
            processing_duration_ms=processing_duration_ms,
        )

    except Exception:
        logger.exception(
            "Failed to record metrics for component=%s batch_id=%s",
            component,
            batch_id,
        )


def get_recent_metrics(hours: int = 24):
    """Get aggregated pipeline metrics for the last N hours."""

    if hours < 1:
        raise ValueError("hours must be at least 1")

    connection = get_metrics_connection()

    try:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT
                    component,
                    metric_name,
                    SUM(metric_value) AS total_value,
                    COUNT(DISTINCT batch_id) AS batch_count,
                    MAX(recorded_at) AS last_recorded_at
                FROM pipeline_metrics
                WHERE recorded_at >=
                    CURRENT_TIMESTAMP - (%s * INTERVAL '1 hour')
                GROUP BY component, metric_name
                ORDER BY component, metric_name
                """,
                (hours,),
            )

            return cursor.fetchall()

    finally:
        connection.close()
