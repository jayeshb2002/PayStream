
import logging

from datetime import datetime, timedelta, timezone

from airflow.models import TaskInstance
from airflow.utils.session import create_session

from src.orchestration.failure_tracker import (
    record_pipeline_failure_safely
)


logger = logging.getLogger(__name__)


def scan_airflow_task_failures(hours=24):
    """
    Discover failed Airflow task instances from the Airflow
    metadata database and persist them in PayStream PostgreSQL.

    Repeated scans do not artificially increment occurrences.
    """

    if hours <= 0:
        raise ValueError("hours must be greater than zero")

    cutoff = datetime.now(timezone.utc) - timedelta(hours=hours)

    discovered = 0
    recorded = 0

    with create_session() as session:
        failed_tasks = (
            session.query(TaskInstance)
            .filter(
                TaskInstance.state == "failed",
                TaskInstance.end_date >= cutoff
            )
            .all()
        )

        # Copy the required fields while the session is open.
        failures = [
            {
                "dag_id": task.dag_id,
                "task_id": task.task_id,
                "run_id": task.run_id,
                "try_number": task.try_number,
                "end_date": task.end_date.isoformat()
                if task.end_date else None
            }
            for task in failed_tasks
        ]

    discovered = len(failures)

    for failure in failures:
        error = RuntimeError(
            "Airflow task execution failed: "
            f"{failure['dag_id']}.{failure['task_id']} "
            f"(run_id={failure['run_id']})"
        )

        result = record_pipeline_failure_safely(
            component="airflow",
            failure_type="AIRFLOW_TASK_FAILURE",
            exception=error,
            severity="HIGH",
            context={
                "dag_id": failure["dag_id"],
                "task_id": failure["task_id"],
                "run_id": failure["run_id"],
                "try_number": failure["try_number"],
                "end_date": failure["end_date"]
            },
            increment_occurrence=False
        )

        if result is not None:
            recorded += 1

    logger.info(
        "Airflow failure scan completed: discovered=%s recorded=%s",
        discovered,
        recorded
    )

    return {
        "discovered_failures": discovered,
        "recorded_failures": recorded
    }
