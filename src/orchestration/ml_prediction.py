
"""Generate transaction-level predictions using Isolation Forest."""

from __future__ import annotations

import argparse
import json
import logging
from datetime import datetime, timezone
from pathlib import Path

import joblib
import numpy as np
import pandas as pd

from src.orchestration.ml_model import (
    MODEL_FEATURES,
    load_training_data,
)
from src.orchestration.ml_evaluation import (
    prepare_features,
    get_database_connection,
)

logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)


def load_model(model_path):
    """Load the trained Isolation Forest model."""

    path = Path(model_path)

    if not path.exists():
        raise FileNotFoundError(
            f"Trained model not found: {path}. "
            "Run Milestone 6.2 first."
        )

    return joblib.load(path)



def load_threshold(threshold_path):
    """Load and validate the selected anomaly threshold."""

    path = Path(threshold_path)

    if not path.exists():
        raise FileNotFoundError(
            f"Threshold configuration not found: {path}. "
            "Run Milestone 6.3 first."
        )

    with path.open("r", encoding="utf-8") as file:
        config = json.load(file)

    if "threshold" not in config:
        raise ValueError("Threshold configuration is missing threshold.")

    if "percentile" not in config:
        raise ValueError("Threshold configuration is missing percentile.")

    threshold = float(config["threshold"])
    percentile = float(config["percentile"])

    if not np.isfinite(threshold):
        raise ValueError("Configured threshold must be finite.")

    if not 0 < percentile < 100:
        raise ValueError("Configured percentile must be between 0 and 100.")

    if config.get("score_direction") != "Higher means more anomalous":
        raise ValueError("Unsupported anomaly score direction.")

    return config



def generate_predictions(data, model, threshold_config):
    """Generate one anomaly prediction for each transaction."""

    if data.empty:
        raise ValueError("Prediction dataset is empty.")

    if "transaction_id" not in data.columns:
        raise ValueError("transaction_id column is required.")

    if data["transaction_id"].isnull().any():
        raise ValueError("Transaction IDs cannot be null.")

    if data["transaction_id"].duplicated().any():
        raise ValueError("Duplicate transaction IDs found.")

    features = prepare_features(data)

    # Isolation Forest decision_function:
    # lower values indicate more anomalous observations.
    # Negating makes higher scores indicate more anomalous.
    anomaly_scores = -model.decision_function(features)

    if not np.isfinite(anomaly_scores).all():
        raise ValueError("Model returned non-finite anomaly scores.")

    threshold = float(threshold_config["threshold"])

    predictions = anomaly_scores >= threshold

    prediction_time = datetime.now(timezone.utc)

    results = pd.DataFrame(
        {
            "transaction_id": data["transaction_id"].astype(str),
            "ml_anomaly_score": anomaly_scores.astype(float),
            "ml_is_anomaly": predictions.astype(bool),
            "ml_prediction": np.where(
                predictions,
                "ANOMALY",
                "NORMAL",
            ),
            "ml_threshold": threshold,
            "model_name": threshold_config.get(
                "model_name",
                "PayStream Isolation Forest",
            ),
            "model_percentile": float(
                threshold_config["percentile"]
            ),
            "predicted_at": prediction_time,
        }
    )

    return results


def run_pipeline(
    model_path="models/isolation_forest.joblib",
    threshold_path="models/ml_threshold.json",
):
    """Load data, predict anomalies and return the results."""

    model = load_model(model_path)
    threshold_config = load_threshold(threshold_path)

    connection = get_database_connection()

    try:
        data = load_training_data(connection)
    finally:
        connection.close()

    predictions = generate_predictions(
        data,
        model,
        threshold_config,
    )

    summary = {
        "total_transactions": len(predictions),
        "anomalies": int(predictions["ml_is_anomaly"].sum()),
        "normal_transactions": int(
            (~predictions["ml_is_anomaly"]).sum()
        ),
        "threshold": float(threshold_config["threshold"]),
        "anomaly_rate_percent": round(
            float(predictions["ml_is_anomaly"].mean()) * 100,
            2,
        ),
    }

    logger.info("ML prediction summary: %s", summary)

    return predictions, summary


def main():
    parser = argparse.ArgumentParser(
        description="Generate PayStream transaction ML predictions."
    )

    parser.add_argument(
        "--model-path",
        default="models/isolation_forest.joblib",
    )

    parser.add_argument(
        "--threshold-path",
        default="models/ml_threshold.json",
    )

    args = parser.parse_args()

    predictions, summary = run_pipeline(
        model_path=args.model_path,
        threshold_path=args.threshold_path,
    )

    print("\nPayStream ML Prediction Summary")
    print("--------------------------------")
    print(f"Total transactions: {summary['total_transactions']}")
    print(f"Anomalies: {summary['anomalies']}")
    print(f"Normal: {summary['normal_transactions']}")
    print(f"Threshold: {summary['threshold']:.6f}")
    print(f"Anomaly rate: {summary['anomaly_rate_percent']}%")

    print("\nHighest anomaly scores:")

    print(
        predictions.sort_values(
            "ml_anomaly_score",
            ascending=False,
        ).head(10).to_string(index=False)
    )


if __name__ == "__main__":
    main()
