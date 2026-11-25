
from unittest.mock import MagicMock

import pytest

from src.orchestration.anomaly_detection import (
    ANOMALY_DETECTION_SQL,
    run_anomaly_detection,
)


def test_anomaly_detection_executes_and_commits():
    connection = MagicMock()
    cursor = MagicMock()

    connection.cursor.return_value = cursor
    cursor.rowcount = 100

    result = run_anomaly_detection(connection)

    cursor.execute.assert_called_once_with(
        ANOMALY_DETECTION_SQL,
        (5, 3.5, 5, 5),
    )

    connection.commit.assert_called_once()
    connection.rollback.assert_not_called()
    cursor.close.assert_called_once()

    assert result["status"] == "SUCCESS"
    assert result["rows_analyzed"] == 100


def test_anomaly_detection_rolls_back_on_failure():
    connection = MagicMock()
    cursor = MagicMock()

    connection.cursor.return_value = cursor
    cursor.execute.side_effect = RuntimeError("Database failure")

    with pytest.raises(RuntimeError, match="Database failure"):
        run_anomaly_detection(connection)

    connection.rollback.assert_called_once()
    connection.commit.assert_not_called()
    cursor.close.assert_called_once()


def test_invalid_minimum_baseline_size():
    connection = MagicMock()

    with pytest.raises(ValueError):
        run_anomaly_detection(connection, minimum_baseline_size=0)

    connection.cursor.assert_not_called()


def test_invalid_robust_z_threshold():
    connection = MagicMock()

    with pytest.raises(ValueError):
        run_anomaly_detection(connection, robust_z_threshold=0)

    connection.cursor.assert_not_called()


def test_sql_uses_median_and_mad():
    assert "PERCENTILE_CONT(0.5)" in ANOMALY_DETECTION_SQL
    assert "baseline_mad_minor" in ANOMALY_DETECTION_SQL


def test_sql_uses_95th_percentile():
    assert "PERCENTILE_CONT(0.95)" in ANOMALY_DETECTION_SQL


def test_sql_uses_robust_z_score():
    assert "0.6745" in ANOMALY_DETECTION_SQL
    assert "ROBUST_Z_SCORE_THRESHOLD_EXCEEDED" in ANOMALY_DETECTION_SQL


def test_sql_handles_zero_mad():
    assert "baseline_mad_minor > 0" in ANOMALY_DETECTION_SQL


def test_sql_is_idempotent():
    assert "ON CONFLICT (transaction_id)" in ANOMALY_DETECTION_SQL
