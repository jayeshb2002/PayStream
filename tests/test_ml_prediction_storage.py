
from datetime import datetime, timezone
from unittest.mock import MagicMock

import pandas as pd
import pytest

from src.orchestration.ml_prediction_storage import (
    persist_predictions,
)


@pytest.fixture
def predictions():
    return pd.DataFrame(
        {
            "transaction_id": ["TXN-1", "TXN-2"],
            "ml_anomaly_score": [0.35, -0.12],
            "ml_is_anomaly": [True, False],
            "ml_prediction": ["ANOMALY", "NORMAL"],
            "ml_threshold": [0.20, 0.20],
            "model_name": [
                "PayStream Isolation Forest",
                "PayStream Isolation Forest",
            ],
            "model_percentile": [95.0, 95.0],
            "predicted_at": [
                datetime.now(timezone.utc),
                datetime.now(timezone.utc),
            ],
        }
    )


@pytest.fixture
def summary():
    return {
        "total_transactions": 2,
        "anomalies": 1,
        "normal_transactions": 1,
        "threshold": 0.20,
        "anomaly_rate_percent": 50.0,
    }


@pytest.fixture
def connection():
    mock_connection = MagicMock()
    mock_cursor = MagicMock()
    mock_connection.cursor.return_value.__enter__.return_value = mock_cursor
    return mock_connection


def test_persist_predictions_commits_successfully(
    connection,
    predictions,
    summary,
):
    result = persist_predictions(
        connection,
        predictions,
        summary,
    )

    connection.commit.assert_called_once()
    connection.rollback.assert_not_called()

    assert result["total_transactions"] == 2
    assert result["anomalies"] == 1
    assert result["normal_transactions"] == 1
    assert result["anomaly_rate_percent"] == 50.0
    assert result["run_id"]


def test_persist_predictions_inserts_run_and_transactions(
    connection,
    predictions,
    summary,
):
    persist_predictions(
        connection,
        predictions,
        summary,
    )

    cursor = connection.cursor.return_value.__enter__.return_value

    assert cursor.execute.call_count == 1
    cursor.executemany.assert_called_once()

    rows = cursor.executemany.call_args.args[1]

    assert len(rows) == 2
    assert rows[0][1] == "TXN-1"
    assert rows[1][1] == "TXN-2"


def test_persist_predictions_rolls_back_on_failure(
    connection,
    predictions,
    summary,
):
    cursor = connection.cursor.return_value.__enter__.return_value
    cursor.executemany.side_effect = RuntimeError("Database failure")

    with pytest.raises(RuntimeError, match="Database failure"):
        persist_predictions(
            connection,
            predictions,
            summary,
        )

    connection.rollback.assert_called_once()
    connection.commit.assert_not_called()


def test_persist_predictions_rejects_empty_dataset(
    connection,
    summary,
):
    with pytest.raises(ValueError, match="empty"):
        persist_predictions(
            connection,
            pd.DataFrame(),
            summary,
        )

    connection.commit.assert_not_called()


def test_persist_predictions_rejects_duplicate_transactions(
    connection,
    predictions,
    summary,
):
    duplicated = pd.concat(
        [predictions, predictions.iloc[[0]]],
        ignore_index=True,
    )

    with pytest.raises(ValueError, match="Duplicate"):
        persist_predictions(
            connection,
            duplicated,
            summary,
        )


def test_persist_predictions_rejects_summary_mismatch(
    connection,
    predictions,
    summary,
):
    incorrect_summary = {
        **summary,
        "total_transactions": 100,
    }

    with pytest.raises(ValueError, match="count does not match"):
        persist_predictions(
            connection,
            predictions,
            incorrect_summary,
        )


def test_same_run_id_can_be_reused_for_idempotent_retry(
    connection,
    predictions,
    summary,
):
    run_id = "a7c6bdf0-68e2-4e8b-9f53-3f5d6a4b8c21"

    first_result = persist_predictions(
        connection,
        predictions,
        summary,
        run_id=run_id,
    )

    second_result = persist_predictions(
        connection,
        predictions,
        summary,
        run_id=run_id,
    )

    assert first_result["run_id"] == second_result["run_id"]
    assert first_result["run_id"] == run_id
    assert connection.commit.call_count == 2
