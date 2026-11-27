
from datetime import datetime, timezone
from unittest.mock import MagicMock

import pandas as pd
import pytest

from src.orchestration.ml_risk_integration import (
    calculate_integrated_risk,
    get_risk_category,
    persist_integrated_risk,
)


@pytest.fixture
def integration_data():
    return pd.DataFrame(
        {
            "transaction_id": ["TXN-1", "TXN-2", "TXN-3", "TXN-4"],
            "risk_score": [20, 40, 65, 90],
            "risk_category": ["LOW", "MEDIUM", "HIGH", "CRITICAL"],
            "ml_run_id": [
                "a7c6bdf0-68e2-4e8b-9f53-3f5d6a4b8c21"
            ] * 4,
            "ml_anomaly_score": [0.05, 0.25, 0.30, 0.40],
            "ml_is_anomaly": [False, True, True, False],
            "ml_prediction": [
                "NORMAL",
                "ANOMALY",
                "ANOMALY",
                "NORMAL",
            ],
        }
    )


@pytest.fixture
def connection():
    mock_connection = MagicMock()
    mock_cursor = MagicMock()
    mock_connection.cursor.return_value.__enter__.return_value = mock_cursor
    return mock_connection


def test_normal_ml_prediction_does_not_increase_risk(
    integration_data,
):
    results = calculate_integrated_risk(integration_data)

    first = results.loc[
        results["transaction_id"] == "TXN-1"
    ].iloc[0]

    assert first["statistical_risk_score"] == 20
    assert first["ml_adjustment_points"] == 0
    assert first["integrated_risk_score"] == 20
    assert first["integrated_risk_category"] == "LOW"


def test_anomalous_ml_prediction_increases_risk(
    integration_data,
):
    results = calculate_integrated_risk(integration_data)

    second = results.loc[
        results["transaction_id"] == "TXN-2"
    ].iloc[0]

    assert second["statistical_risk_score"] == 40
    assert second["ml_adjustment_points"] == 20
    assert second["integrated_risk_score"] == 60
    assert second["integrated_risk_category"] == "HIGH"


def test_integrated_risk_score_never_exceeds_100(
    integration_data,
):
    results = calculate_integrated_risk(integration_data)

    critical = results.loc[
        results["transaction_id"] == "TXN-3"
    ].iloc[0]

    assert critical["integrated_risk_score"] == 85

    capped_data = integration_data.iloc[[2]].copy()
    capped_data.loc[:, "risk_score"] = 95

    capped = calculate_integrated_risk(capped_data)

    assert capped.iloc[0]["integrated_risk_score"] == 100


def test_custom_ml_adjustment_is_supported(integration_data):
    results = calculate_integrated_risk(
        integration_data,
        ml_adjustment_points=10,
    )

    second = results.loc[
        results["transaction_id"] == "TXN-2"
    ].iloc[0]

    assert second["integrated_risk_score"] == 50
    assert second["ml_adjustment_points"] == 10


@pytest.mark.parametrize(
    ("score", "expected"),
    [
        (0, "LOW"),
        (24, "LOW"),
        (25, "MEDIUM"),
        (49, "MEDIUM"),
        (50, "HIGH"),
        (74, "HIGH"),
        (75, "CRITICAL"),
        (100, "CRITICAL"),
    ],
)
def test_risk_category_boundaries(score, expected):
    assert get_risk_category(score) == expected


def test_empty_dataset_is_rejected():
    with pytest.raises(ValueError, match="empty"):
        calculate_integrated_risk(pd.DataFrame())


def test_duplicate_transactions_are_rejected(integration_data):
    duplicate_data = pd.concat(
        [integration_data, integration_data.iloc[[0]]],
        ignore_index=True,
    )

    with pytest.raises(ValueError, match="Duplicate"):
        calculate_integrated_risk(duplicate_data)


def test_invalid_ml_prediction_is_rejected(integration_data):
    integration_data.loc[0, "ml_prediction"] = "UNKNOWN"

    with pytest.raises(ValueError, match="Invalid ML prediction"):
        calculate_integrated_risk(integration_data)


def test_inconsistent_ml_flag_is_rejected(integration_data):
    integration_data.loc[0, "ml_is_anomaly"] = True

    with pytest.raises(ValueError, match="do not match"):
        calculate_integrated_risk(integration_data)


def test_persistence_commits_successfully(
    connection,
    integration_data,
):
    results = calculate_integrated_risk(integration_data)

    summary = persist_integrated_risk(
        connection=connection,
        results=results,
        ml_run_id=integration_data["ml_run_id"].iloc[0],
        duration_ms=125,
    )

    connection.commit.assert_called_once()
    connection.rollback.assert_not_called()

    assert summary["total_transactions"] == 4
    assert summary["ml_anomaly_count"] == 2
    assert summary["adjusted_transaction_count"] == 2


def test_persistence_rolls_back_on_failure(
    connection,
    integration_data,
):
    results = calculate_integrated_risk(integration_data)

    cursor = connection.cursor.return_value.__enter__.return_value
    cursor.executemany.side_effect = RuntimeError("Database failure")

    with pytest.raises(RuntimeError, match="Database failure"):
        persist_integrated_risk(
            connection=connection,
            results=results,
            ml_run_id=integration_data["ml_run_id"].iloc[0],
        )

    connection.rollback.assert_called_once()
    connection.commit.assert_not_called()


def test_persistence_rejects_empty_results(connection, integration_data):
    with pytest.raises(ValueError, match="empty"):
        persist_integrated_risk(
            connection=connection,
            results=pd.DataFrame(),
            ml_run_id=integration_data["ml_run_id"].iloc[0],
        )
