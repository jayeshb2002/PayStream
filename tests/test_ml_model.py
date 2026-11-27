
import numpy as np
import pandas as pd
import pytest

from src.orchestration.ml_model import (
    MODEL_FEATURES,
    build_model,
    train_model,
    validate_training_data,
)


@pytest.fixture
def training_data():
    rng = np.random.default_rng(42)
    rows = []

    for index in range(100):
        rows.append(
            {
                "transaction_id": f"TXN-{index}",
                "payment_rail": (
                    "UPI" if index % 2 == 0 else "IMPS"
                ),
                "currency": "INR",
                "latest_status": "SUCCESS",
                "amount_minor": int(
                    rng.integers(100, 100000)
                ),
                "event_count": int(
                    rng.integers(1, 5)
                ),
                "distinct_event_type_count": int(
                    rng.integers(1, 4)
                ),
                "failed_event_count": 0,
                "lifecycle_duration_seconds": float(
                    rng.integers(1, 120)
                ),
                "statistical_is_anomaly": False,
                "statistical_robust_z_score": None,
            }
        )

    return pd.DataFrame(rows)


def test_validate_training_data_success(training_data):
    validate_training_data(training_data)


def test_validate_training_data_rejects_empty_dataset():
    with pytest.raises(ValueError, match="empty"):
        validate_training_data(pd.DataFrame())


def test_validate_training_data_requires_minimum_rows(training_data):
    with pytest.raises(ValueError, match="At least"):
        validate_training_data(training_data.head(5))


def test_validate_training_data_requires_features(training_data):
    incomplete = training_data.drop(columns=["amount_minor"])

    with pytest.raises(ValueError, match="Missing ML features"):
        validate_training_data(incomplete)


def test_build_model_returns_pipeline():
    model = build_model()

    assert "preprocessor" in model.named_steps
    assert "isolation_forest" in model.named_steps


def test_train_model_saves_artifacts(training_data, tmp_path):
    report = train_model(
        training_data,
        output_dir=str(tmp_path),
    )

    assert report["training_rows"] == 100
    assert report["predicted_anomalies"] >= 0
    assert report["predicted_normal"] >= 0

    assert report["predicted_anomalies"] + report["predicted_normal"] == 100

    assert (tmp_path / "isolation_forest.joblib").exists()
    assert (tmp_path / "isolation_forest_metadata.json").exists()


def test_statistical_results_are_not_model_features():
    assert "statistical_is_anomaly" not in MODEL_FEATURES
    assert "statistical_robust_z_score" not in MODEL_FEATURES
