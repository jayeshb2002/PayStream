
from unittest.mock import MagicMock

import pytest

from src.orchestration.transaction_features import (
    FEATURE_ENGINEERING_SQL,
    run_transaction_feature_engineering,
)


def test_feature_engineering_executes_sql_and_commits():
    connection = MagicMock()
    cursor = MagicMock()

    connection.cursor.return_value = cursor
    cursor.rowcount = 10

    result = run_transaction_feature_engineering(connection)

    connection.cursor.assert_called_once()
    cursor.execute.assert_called_once_with(FEATURE_ENGINEERING_SQL)
    connection.commit.assert_called_once()
    connection.rollback.assert_not_called()
    cursor.close.assert_called_once()

    assert result == {
        "status": "SUCCESS",
        "rows_inserted_or_updated": 10,
    }


def test_feature_engineering_rolls_back_on_error():
    connection = MagicMock()
    cursor = MagicMock()

    connection.cursor.return_value = cursor

    cursor.execute.side_effect = RuntimeError("Database failure")

    with pytest.raises(RuntimeError, match="Database failure"):
        run_transaction_feature_engineering(connection)

    connection.rollback.assert_called_once()
    connection.commit.assert_not_called()
    cursor.close.assert_called_once()


def test_sql_uses_transaction_id_as_conflict_key():
    assert "ON CONFLICT (transaction_id)" in FEATURE_ENGINEERING_SQL


def test_sql_uses_latest_event_ranking():
    assert "ROW_NUMBER() OVER" in FEATURE_ENGINEERING_SQL
    assert "event_time DESC" in FEATURE_ENGINEERING_SQL
    assert "sequence_number DESC" in FEATURE_ENGINEERING_SQL


def test_sql_does_not_sum_lifecycle_amounts():
    assert "SUM(amount_minor)" not in FEATURE_ENGINEERING_SQL


def test_sql_calculates_failed_event_count():
    assert "failed_event_count" in FEATURE_ENGINEERING_SQL
    assert "PAYMENT_FAILED" in FEATURE_ENGINEERING_SQL


def test_sql_uses_risk_analytics_feature_schema():
    assert "distinct_event_types" in FEATURE_ENGINEERING_SQL
    assert "updated_at" in FEATURE_ENGINEERING_SQL
    assert "distinct_event_type_count" not in FEATURE_ENGINEERING_SQL
    assert "latest_event_type" not in FEATURE_ENGINEERING_SQL
    assert "feature_updated_at" not in FEATURE_ENGINEERING_SQL
