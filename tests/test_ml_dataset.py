
from unittest.mock import MagicMock, call, patch

import pytest

from src.orchestration.ml_dataset import (
    ML_DATASET_SQL,
    prepare_ml_dataset,
)


def test_prepare_ml_dataset_success():
    connection = MagicMock()
    cursor = MagicMock()

    connection.cursor.return_value.__enter__.return_value = cursor

    cursor.fetchone.return_value = (100, 8, 2)

    report = prepare_ml_dataset(connection)

    assert report["total_records"] == 100
    assert report["statistical_anomalies"] == 8
    assert report["records_without_anomaly_result"] == 2
    assert report["execution_duration_ms"] >= 0

    assert cursor.execute.call_count == 2
    connection.commit.assert_called_once()
    connection.rollback.assert_not_called()


def test_prepare_ml_dataset_rolls_back_on_failure():
    connection = MagicMock()
    cursor = MagicMock()

    connection.cursor.return_value.__enter__.return_value = cursor

    cursor.execute.side_effect = RuntimeError(
        "Database failure"
    )

    with pytest.raises(RuntimeError):
        prepare_ml_dataset(connection)

    connection.rollback.assert_called_once()
    connection.commit.assert_not_called()


def test_ml_dataset_uses_transaction_id_as_unique_key():
    assert "ON CONFLICT (transaction_id)" in ML_DATASET_SQL


def test_ml_dataset_joins_statistical_anomaly_results():
    assert "LEFT JOIN transaction_anomaly_results" in ML_DATASET_SQL


def test_ml_dataset_uses_existing_feature_columns():
    assert "f.distinct_event_types" in ML_DATASET_SQL
    assert "f.updated_at" in ML_DATASET_SQL
    assert "f.distinct_event_type_count" not in ML_DATASET_SQL
    assert "f.feature_updated_at" not in ML_DATASET_SQL


def test_ml_dataset_does_not_filter_out_unscored_transactions():
    assert "LEFT JOIN transaction_anomaly_results" in ML_DATASET_SQL
    assert "WHERE" in ML_DATASET_SQL
    assert "a.is_anomaly IS NOT NULL" not in ML_DATASET_SQL
