
import json

import numpy as np
import pandas as pd
import pytest

from src.orchestration.ml_evaluation import (
    calculate_threshold_metrics,
    evaluate_model,
    prepare_features,
    save_evaluation,
)


class FakeModel:
    """Deterministic model for evaluation tests."""

    def decision_function(self, features):
        # Lower decision scores mean more anomalous.
        return np.linspace(0.5, -0.5, len(features))


@pytest.fixture
def evaluation_data():
    rows = []

    for index in range(100):
        rows.append(
            {
                "transaction_id": f"TXN-{index}",
                "payment_rail": "UPI" if index % 2 == 0 else "IMPS",
                "currency": "INR",
                "latest_status": "SUCCESS",
                "amount_minor": 1000 + index * 100,
                "event_count": 2,
                "distinct_event_type_count": 2,
                "failed_event_count": 0,
                "lifecycle_duration_seconds": 10.0 + index,
                "statistical_is_anomaly": index >= 90,
            }
        )

    return pd.DataFrame(rows)


def test_prepare_features_excludes_statistical_columns(evaluation_data):
    features = prepare_features(evaluation_data)

    assert "statistical_is_anomaly" not in features.columns
    assert "amount_minor" in features.columns
    assert "payment_rail" in features.columns


def test_evaluate_model_returns_candidate_thresholds(evaluation_data):
    report = evaluate_model(
        evaluation_data,
        FakeModel(),
    )

    assert report["evaluation_rows"] == 100
    assert len(report["candidate_thresholds"]) == 4
    assert report["selected_threshold"]["percentile"] == 95.0


def test_selected_threshold_flags_expected_upper_tail(evaluation_data):
    report = evaluate_model(
        evaluation_data,
        FakeModel(),
        target_percentile=95.0,
    )

    selected = report["selected_threshold"]

    assert selected["flagged_transactions"] >= 1
    assert selected["flagged_transactions"] <= 10
    assert selected["alert_rate_percent"] > 0


def test_statistical_detector_comparison_is_available(evaluation_data):
    report = evaluate_model(
        evaluation_data,
        FakeModel(),
    )

    selected = report["selected_threshold"]

    assert selected["statistical_anomalies"] == 10
    assert selected["overlap_with_statistical"] >= 0


def test_invalid_percentile_is_rejected(evaluation_data):
    with pytest.raises(ValueError, match="between 0 and 100"):
        evaluate_model(
            evaluation_data,
            FakeModel(),
            target_percentile=100,
        )


def test_empty_dataset_is_rejected():
    with pytest.raises(ValueError, match="empty"):
        prepare_features(pd.DataFrame())


def test_evaluation_artifacts_are_saved(evaluation_data, tmp_path):
    report = evaluate_model(
        evaluation_data,
        FakeModel(),
    )

    paths = save_evaluation(report, str(tmp_path))

    report_path = tmp_path / "ml_evaluation_report.json"
    threshold_path = tmp_path / "ml_threshold.json"

    assert report_path.exists()
    assert threshold_path.exists()

    with report_path.open() as file:
        saved_report = json.load(file)

    with threshold_path.open() as file:
        saved_threshold = json.load(file)

    assert saved_report["evaluation_rows"] == 100
    assert saved_threshold["percentile"] == 95.0
    assert paths["report_path"] == str(report_path)
