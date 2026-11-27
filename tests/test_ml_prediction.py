
import numpy as np
import pandas as pd
import pytest

from src.orchestration.ml_prediction import (
    generate_predictions,
    load_threshold,
)


class FakeModel:
    """Deterministic model for prediction tests."""

    def decision_function(self, features):
        return np.array([0.5, 0.1, -0.2, -0.5])


@pytest.fixture
def prediction_data():
    return pd.DataFrame(
        {
            "transaction_id": [
                "TXN-1",
                "TXN-2",
                "TXN-3",
                "TXN-4",
            ],
            "payment_rail": ["UPI", "UPI", "IMPS", "IMPS"],
            "currency": ["INR"] * 4,
            "latest_status": ["SUCCESS"] * 4,
            "amount_minor": [1000, 2000, 3000, 4000],
            "event_count": [2, 2, 3, 4],
            "distinct_event_type_count": [2, 2, 2, 3],
            "failed_event_count": [0, 0, 0, 1],
            "lifecycle_duration_seconds": [10.0, 20.0, 30.0, 40.0],
        }
    )


@pytest.fixture
def threshold_config():
    return {
        "model_name": "PayStream Isolation Forest",
        "threshold": 0.2,
        "percentile": 95.0,
        "score_direction": "Higher means more anomalous",
    }


def test_generate_predictions_returns_one_result_per_transaction(
    prediction_data,
    threshold_config,
):
    results = generate_predictions(
        prediction_data,
        FakeModel(),
        threshold_config,
    )

    assert len(results) == 4
    assert results["transaction_id"].tolist() == [
        "TXN-1",
        "TXN-2",
        "TXN-3",
        "TXN-4",
    ]


def test_anomaly_score_direction_and_classification(
    prediction_data,
    threshold_config,
):
    results = generate_predictions(
        prediction_data,
        FakeModel(),
        threshold_config,
    )

    # Negated decision scores: [-0.5, -0.1, 0.2, 0.5]
    assert results["ml_anomaly_score"].tolist() == pytest.approx(
        [-0.5, -0.1, 0.2, 0.5]
    )

    assert results["ml_prediction"].tolist() == [
        "NORMAL",
        "NORMAL",
        "ANOMALY",
        "ANOMALY",
    ]


def test_prediction_uses_configured_threshold(
    prediction_data,
    threshold_config,
):
    threshold_config["threshold"] = 0.6

    results = generate_predictions(
        prediction_data,
        FakeModel(),
        threshold_config,
    )

    assert results["ml_is_anomaly"].sum() == 0


def test_empty_dataset_is_rejected(threshold_config):
    with pytest.raises(ValueError, match="empty"):
        generate_predictions(
            pd.DataFrame(),
            FakeModel(),
            threshold_config,
        )


def test_duplicate_transaction_ids_are_rejected(
    prediction_data,
    threshold_config,
):
    duplicate_data = pd.concat(
        [prediction_data, prediction_data.iloc[[0]]],
        ignore_index=True,
    )

    with pytest.raises(ValueError, match="Duplicate"):
        generate_predictions(
            duplicate_data,
            FakeModel(),
            threshold_config,
        )


def test_null_transaction_ids_are_rejected(
    prediction_data,
    threshold_config,
):
    prediction_data.loc[0, "transaction_id"] = None

    with pytest.raises(ValueError, match="cannot be null"):
        generate_predictions(
            prediction_data,
            FakeModel(),
            threshold_config,
        )


def test_threshold_loader_reads_json(tmp_path):
    threshold_path = tmp_path / "threshold.json"

    threshold_path.write_text(
        """
        {
            "model_name": "PayStream Isolation Forest",
            "threshold": 0.15,
            "percentile": 95.0,
            "score_direction": "Higher means more anomalous"
        }
        """,
        encoding="utf-8",
    )

    config = load_threshold(threshold_path)

    assert config["threshold"] == 0.15
    assert config["percentile"] == 95.0


def test_threshold_loader_rejects_missing_file(tmp_path):
    with pytest.raises(FileNotFoundError):
        load_threshold(tmp_path / "missing.json")



def test_threshold_loader_rejects_invalid_score_direction(tmp_path):
    threshold_path = tmp_path / "threshold.json"

    threshold_path.write_text(
        """
        {
            "threshold": 0.15,
            "percentile": 95.0,
            "score_direction": "Lower means more anomalous"
        }
        """,
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="Unsupported"):
        load_threshold(threshold_path)

