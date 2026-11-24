
from unittest.mock import MagicMock, patch

import pytest

from src.orchestration.failure_tracker import (
    generate_failure_key,
    record_pipeline_failure,
    record_pipeline_failure_safely,
    resolve_pipeline_failure
)


def test_failure_key_is_stable():
    context = {
        "dag_id": "paystream_test",
        "task_id": "test_task"
    }

    first = generate_failure_key(
        "airflow",
        "AIRFLOW_TASK_FAILURE",
        context
    )

    second = generate_failure_key(
        "airflow",
        "AIRFLOW_TASK_FAILURE",
        context
    )

    assert first == second


def test_failure_key_changes_with_context():
    first = generate_failure_key(
        "airflow",
        "AIRFLOW_TASK_FAILURE",
        {"task_id": "task_one"}
    )

    second = generate_failure_key(
        "airflow",
        "AIRFLOW_TASK_FAILURE",
        {"task_id": "task_two"}
    )

    assert first != second


def test_record_pipeline_failure_success():
    connection = MagicMock()
    cursor = MagicMock()

    connection.cursor.return_value.__enter__.return_value = cursor

    with patch(
        "src.orchestration.failure_tracker.get_metrics_connection",
        return_value=connection
    ):
        key = record_pipeline_failure(
            component="spark_processor",
            failure_type="SPARK_BATCH_FAILURE",
            exception=RuntimeError("Test Spark failure"),
            severity="CRITICAL",
            context={"batch_id": 10}
        )

    assert len(key) == 64
    cursor.execute.assert_called_once()
    connection.commit.assert_called_once()
    connection.close.assert_called_once()


def test_record_pipeline_failure_invalid_severity():
    with pytest.raises(ValueError):
        record_pipeline_failure(
            component="spark_processor",
            failure_type="SPARK_BATCH_FAILURE",
            exception=RuntimeError("Test failure"),
            severity="INVALID"
        )


def test_record_pipeline_failure_empty_component():
    with pytest.raises(ValueError):
        record_pipeline_failure(
            component="",
            failure_type="SPARK_BATCH_FAILURE",
            exception=RuntimeError("Test failure")
        )


def test_safe_failure_recording_does_not_raise():
    with patch(
        "src.orchestration.failure_tracker.record_pipeline_failure",
        side_effect=RuntimeError("Database unavailable")
    ):
        result = record_pipeline_failure_safely(
            component="postgres_sink",
            failure_type="POSTGRES_SINK_BATCH_FAILURE",
            exception=RuntimeError("Sink failed")
        )

    assert result is None


def test_resolve_pipeline_failure():
    connection = MagicMock()
    cursor = MagicMock()
    cursor.rowcount = 1

    connection.cursor.return_value.__enter__.return_value = cursor

    with patch(
        "src.orchestration.failure_tracker.get_metrics_connection",
        return_value=connection
    ):
        resolved = resolve_pipeline_failure("test-failure-key")

    assert resolved is True
    cursor.execute.assert_called_once()
    connection.commit.assert_called_once()
    connection.close.assert_called_once()
