
"""Evaluate Isolation Forest scores and optimize the alert threshold."""

from __future__ import annotations

import argparse
import json
import logging
import os
from datetime import datetime, timezone
from pathlib import Path

import joblib
import numpy as np
import pandas as pd
import psycopg2
from dotenv import load_dotenv

from src.orchestration.ml_model import (
    MODEL_FEATURES,
    NUMERIC_FEATURES,
    CATEGORICAL_FEATURES,
    load_training_data,
)

load_dotenv()

logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)

DEFAULT_TARGET_ALERT_PERCENTILE = 95.0

CANDIDATE_PERCENTILES = [90.0, 95.0, 97.5, 99.0]


def get_database_connection():
    return psycopg2.connect(
        host=os.getenv("PAYSTREAM_DB_HOST", "localhost"),
        port=int(os.getenv("PAYSTREAM_DB_PORT", "5433")),
        dbname=os.getenv("PAYSTREAM_DB_NAME", "paystream"),
        user=os.getenv("PAYSTREAM_DB_USER", "paystream"),
        password=os.getenv("PAYSTREAM_DB_PASSWORD"),
        connect_timeout=10,
    )


def prepare_features(data):
    """Prepare model inputs using the same columns as training."""

    if data.empty:
        raise ValueError("Evaluation dataset is empty.")

    missing = [
        column for column in MODEL_FEATURES
        if column not in data.columns
    ]

    if missing:
        raise ValueError(f"Missing model features: {missing}")

    features = data[MODEL_FEATURES].copy()

    for column in NUMERIC_FEATURES:
        features[column] = pd.to_numeric(
            features[column],
            errors="coerce",
        )

    for column in CATEGORICAL_FEATURES:
        features[column] = (
            features[column]
            .fillna("UNKNOWN")
            .astype(str)
        )

    return features


def calculate_threshold_metrics(scores, statistical_flags, percentiles):
    """Calculate alert rates and statistical-detector agreement."""

    results = []

    for percentile in percentiles:
        threshold = float(np.percentile(scores, percentile))
        predictions = scores >= threshold

        flagged_count = int(predictions.sum())
        total_count = len(predictions)

        statistical_count = int(statistical_flags.sum())
        overlap_count = int(
            (predictions & statistical_flags).sum()
        )

        union_count = int(
            (predictions | statistical_flags).sum()
        )

        agreement = (
            overlap_count / union_count
            if union_count
            else 1.0
        )

        results.append(
            {
                "percentile": float(percentile),
                "threshold": threshold,
                "flagged_transactions": flagged_count,
                "total_transactions": total_count,
                "alert_rate_percent": round(
                    flagged_count / total_count * 100, 2
                ),
                "statistical_anomalies": statistical_count,
                "overlap_with_statistical": overlap_count,
                "jaccard_agreement": round(agreement, 4),
            }
        )

    return results


def evaluate_model(
    data,
    model,
    target_percentile=DEFAULT_TARGET_ALERT_PERCENTILE,
):
    """Evaluate model scores and select the configured percentile."""

    if not 0 < target_percentile < 100:
        raise ValueError(
            "Target alert percentile must be between 0 and 100."
        )

    features = prepare_features(data)

    # Isolation Forest decision_function:
    # higher = more normal, lower = more anomalous.
    # Negating it makes higher scores more anomalous.
    anomaly_scores = -model.decision_function(features)

    if not np.isfinite(anomaly_scores).all():
        raise ValueError("Model produced non-finite anomaly scores.")

    statistical_flags = (
        data["statistical_is_anomaly"]
        .fillna(False)
        .astype(bool)
        .to_numpy()
        if "statistical_is_anomaly" in data.columns
        else np.zeros(len(data), dtype=bool)
    )

    candidate_results = calculate_threshold_metrics(
        anomaly_scores,
        statistical_flags,
        CANDIDATE_PERCENTILES,
    )

    selected_threshold = float(
        np.percentile(anomaly_scores, target_percentile)
    )

    predictions = anomaly_scores >= selected_threshold

    selected_result = {
        "percentile": float(target_percentile),
        "threshold": selected_threshold,
        "flagged_transactions": int(predictions.sum()),
        "total_transactions": int(len(predictions)),
        "alert_rate_percent": round(
            float(predictions.mean()) * 100, 2
        ),
        "statistical_anomalies": int(statistical_flags.sum()),
        "overlap_with_statistical": int(
            (predictions & statistical_flags).sum()
        ),
    }

    report = {
        "model_name": "PayStream Isolation Forest",
        "evaluated_at": datetime.now(timezone.utc).isoformat(),
        "evaluation_rows": int(len(features)),
        "score_direction": "Higher score means more anomalous",
        "score_minimum": float(np.min(anomaly_scores)),
        "score_maximum": float(np.max(anomaly_scores)),
        "score_mean": float(np.mean(anomaly_scores)),
        "score_median": float(np.median(anomaly_scores)),
        "candidate_thresholds": candidate_results,
        "selected_threshold": selected_result,
        "evaluation_note": (
            "No ground-truth fraud labels are available. "
            "Statistical anomaly agreement is a comparison signal, "
            "not a measure of fraud detection accuracy. "
            "This evaluation uses the available prepared dataset."
        ),
    }

    return report


def save_evaluation(report, output_dir="models"):
    """Persist evaluation report and selected threshold."""

    directory = Path(output_dir)
    directory.mkdir(parents=True, exist_ok=True)

    report_path = directory / "ml_evaluation_report.json"
    threshold_path = directory / "ml_threshold.json"

    with report_path.open("w", encoding="utf-8") as file:
        json.dump(report, file, indent=4)

    threshold_config = {
        "model_name": report["model_name"],
        "threshold": report["selected_threshold"]["threshold"],
        "percentile": report["selected_threshold"]["percentile"],
        "alert_rate_percent": report["selected_threshold"][
            "alert_rate_percent"
        ],
        "updated_at": report["evaluated_at"],
        "score_direction": "Higher means more anomalous",
    }

    with threshold_path.open("w", encoding="utf-8") as file:
        json.dump(threshold_config, file, indent=4)

    return {
        "report_path": str(report_path),
        "threshold_path": str(threshold_path),
    }


def run_pipeline(
    model_path="models/isolation_forest.joblib",
    output_dir="models",
    target_percentile=DEFAULT_TARGET_ALERT_PERCENTILE,
):
    model_file = Path(model_path)

    if not model_file.exists():
        raise FileNotFoundError(
            f"Trained model not found: {model_file}. "
            "Run Milestone 6.2 first."
        )

    model = joblib.load(model_file)

    connection = get_database_connection()

    try:
        data = load_training_data(connection)
    finally:
        connection.close()

    report = evaluate_model(
        data,
        model,
        target_percentile=target_percentile,
    )

    paths = save_evaluation(report, output_dir)

    report["artifacts"] = paths

    logger.info(
        "ML model evaluation completed: %s",
        report["selected_threshold"],
    )

    return report


def main():
    parser = argparse.ArgumentParser(
        description="Evaluate PayStream Isolation Forest model."
    )

    parser.add_argument(
        "--model-path",
        default="models/isolation_forest.joblib",
    )

    parser.add_argument(
        "--output-dir",
        default="models",
    )

    parser.add_argument(
        "--target-percentile",
        type=float,
        default=DEFAULT_TARGET_ALERT_PERCENTILE,
    )

    args = parser.parse_args()

    report = run_pipeline(
        model_path=args.model_path,
        output_dir=args.output_dir,
        target_percentile=args.target_percentile,
    )

    print("\nPayStream ML Evaluation Summary")
    print("--------------------------------")

    print(f"Evaluation rows: {report['evaluation_rows']}")

    print("\nCandidate thresholds:")

    for candidate in report["candidate_thresholds"]:
        print(
            f"Percentile: {candidate['percentile']} | "
            f"Threshold: {candidate['threshold']:.6f} | "
            f"Alert rate: {candidate['alert_rate_percent']}%"
        )

    selected = report["selected_threshold"]

    print("\nSelected threshold:")
    print(f"Percentile: {selected['percentile']}")
    print(f"Threshold: {selected['threshold']:.6f}")
    print(f"Flagged: {selected['flagged_transactions']}")
    print(f"Alert rate: {selected['alert_rate_percent']}%")

    print("\nSaved artifacts:")
    print(report["artifacts"]["report_path"])
    print(report["artifacts"]["threshold_path"])


if __name__ == "__main__":
    main()
