
import hashlib
import json
import logging
import traceback
import uuid

from psycopg2.extras import Json

from src.orchestration.metrics_collector import get_metrics_connection


logger = logging.getLogger(__name__)


VALID_SEVERITIES = {
    "LOW",
    "MEDIUM",
    "HIGH",
    "CRITICAL"
}


def generate_failure_key(component, failure_type, context):
    """
    Generate a stable key for the same incident.

    Context should contain stable identifiers such as DAG ID,
    task ID, query name or pipeline stage.
    """

    identity = {
        "component": component,
        "failure_type": failure_type,
        "context": context
    }

    serialized = json.dumps(
        identity,
        sort_keys=True,
        default=str
    )

    return hashlib.sha256(
        serialized.encode("utf-8")
    ).hexdigest()


def record_pipeline_failure(
    component,
    failure_type,
    exception,
    severity="HIGH",
    context=None,
    increment_occurrence=True
):
    """
    Store a pipeline failure in PostgreSQL.

    Repeated incidents with the same component, failure type
    and context update the existing incident.
    """

    if not component or not component.strip():
        raise ValueError("component cannot be empty")

    if not failure_type or not failure_type.strip():
        raise ValueError("failure_type cannot be empty")

    if severity not in VALID_SEVERITIES:
        raise ValueError("Invalid failure severity")

    context = context or {}

    if not isinstance(context, dict):
        raise ValueError("context must be a dictionary")

    error_message = str(exception) or type(exception).__name__

    stack_trace = "".join(
        traceback.format_exception(
            type(exception),
            exception,
            exception.__traceback__
        )
    )

    failure_key = generate_failure_key(
        component,
        failure_type,
        context
    )

    failure_id = str(uuid.uuid4())

    connection = get_metrics_connection()

    try:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO pipeline_failures (
                    failure_id,
                    failure_key,
                    component,
                    failure_type,
                    severity,
                    error_message,
                    stack_trace,
                    context
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (failure_key)
                DO UPDATE SET
                    severity = EXCLUDED.severity,
                    error_message = EXCLUDED.error_message,
                    stack_trace = EXCLUDED.stack_trace,
                    context = EXCLUDED.context,
                    occurrence_count =
                        pipeline_failures.occurrence_count + 1,
                    last_seen = CURRENT_TIMESTAMP,
                    updated_at = CURRENT_TIMESTAMP,
                    status = 'OPEN',
                    resolved_at = NULL
                """,
                (
                    failure_id,
                    failure_key,
                    component,
                    failure_type,
                    severity,
                    error_message,
                    stack_trace,
                    Json(context)
                )
            )

        connection.commit()

        logger.info(
            "Pipeline failure recorded: component=%s type=%s key=%s",
            component,
            failure_type,
            failure_key
        )

        return failure_key

    except Exception:
        connection.rollback()
        logger.exception("Unable to persist pipeline failure")
        raise

    finally:
        connection.close()


def record_pipeline_failure_safely(
    component,
    failure_type,
    exception,
    severity="HIGH",
    context=None,
    increment_occurrence=True
):
    """
    Best-effort failure logging.

    A monitoring database outage must not hide the original
    pipeline exception.
    """

    try:
        return record_pipeline_failure(
            component=component,
            failure_type=failure_type,
            exception=exception,
            severity=severity,
            context=context,
            increment_occurrence=increment_occurrence
        )

    except Exception:
        logger.exception(
            "Failure tracking unavailable for component=%s",
            component
        )

        return None


def resolve_pipeline_failure(failure_key):
    """Mark an incident as resolved."""

    connection = get_metrics_connection()

    try:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                UPDATE pipeline_failures
                SET status = 'RESOLVED',
                    resolved_at = CURRENT_TIMESTAMP,
                    updated_at = CURRENT_TIMESTAMP
                WHERE failure_key = %s
                  AND status = 'OPEN'
                """,
                (failure_key,)
            )

            updated_count = cursor.rowcount

        connection.commit()

        return updated_count > 0

    except Exception:
        connection.rollback()
        raise

    finally:
        connection.close()
