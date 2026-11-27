
"""Train an Isolation Forest model for PayStream transaction anomalies."""

from __future__ import annotations

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
from sklearn.compose import ColumnTransformer
from sklearn.ensemble import IsolationForest
from sklearn.impute import SimpleImputer
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import FunctionTransformer, OneHotEncoder

load_dotenv()

logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)

NUMERIC_FEATURES = [
    "amount_minor",
    "event_count",
    "distinct_event_type_count",
    "failed_event_count",
    "lifecycle_duration_seconds",
]

CATEGORICAL_FEATURES = [
    "payment_rail",
    "currency",
    "latest_status",
]

MODEL_FEATURES = NUMERIC_FEATURES + CATEGORICAL_FEATURES

MINIMUM_TRAINING_ROWS = 20
RANDOM_STATE = 42
N_ESTIMATORS = 200


def get_database_connection():
    """Connect to the PayStream PostgreSQL database."""

    return psycopg2.connect(
        host=os.getenv("PAYSTREAM_DB_HOST", "localhost"),
        port=int(os.getenv("PAYSTREAM_DB_PORT", "5433")),
        dbname=os.getenv("PAYSTREAM_DB_NAME", "paystream"),
        user=os.getenv("PAYSTREAM_DB_USER", "paystream"),
        password=os.getenv("PAYSTREAM_DB_PASSWORD"),
        connect_timeout=10,
    )


def load_training_data(connection):
    """Load prepared ML features from PostgreSQL."""

    query =  """
        SELECT
            transaction_id,
            payment_rail,
            currency,
            latest_status,
            amount_minor,
            event_count,
            distinct_event_type_count,
            failed_event_count,
            lifecycle_duration_seconds,
            statistical_is_anomaly

        FROM transaction_ml_dataset

        ORDER BY transaction_id;
    """

    with connection.cursor() as cursor:
        cursor.execute(query)
        rows = cursor.fetchall()
        columns = [item[0] for item in cursor.description]

    return pd.DataFrame(rows, columns=columns)


def validate_training_data(data):
    """Validate the dataset before model training."""

    if data.empty:
        raise ValueError(
            "ML training dataset is empty. Run Milestone 6.1 first."
        )

    missing_columns = [
        column
        for column in MODEL_FEATURES
        if column not in data.columns
    ]

    if missing_columns:
        raise ValueError(
            f"Missing ML features: {missing_columns}"
        )

    if len(data) < MINIMUM_TRAINING_ROWS:
        raise ValueError(
            f"At least {MINIMUM_TRAINING_ROWS} transactions are "
            f"required. Found {len(data)}."
        )

    if data[MODEL_FEATURES].isnull().all(axis=1).any():
        raise ValueError(
            "Some transactions have no usable ML feature values."
        )

    numeric_data = data[NUMERIC_FEATURES].apply(
        pd.to_numeric,
        errors="coerce",
    )

    if (numeric_data < 0).any().any():
        raise ValueError(
            "Numeric ML features cannot contain negative values."
        )

    if not np.isfinite(
        numeric_data.fillna(0).to_numpy(dtype=float)
    ).all():
        raise ValueError(
            "Numeric ML features contain non-finite values."
        )


def build_model():
    """Build the preprocessing and Isolation Forest pipeline."""

    numeric_pipeline = Pipeline(
        steps=[
            ("imputer", SimpleImputer(strategy="median")),
            (
                "log_transform",
                FunctionTransformer(
                    np.log1p,
                    validate=False,
                ),
            ),
        ]
    )

    categorical_pipeline = Pipeline(
        steps=[
            (
                "imputer",
                SimpleImputer(
                    strategy="constant",
                    fill_value="UNKNOWN",
                ),
            ),
            (
                "one_hot",
                OneHotEncoder(handle_unknown="ignore"),
            ),
        ]
    )

    preprocessor = ColumnTransformer(
        transformers=[
            ("numeric", numeric_pipeline, NUMERIC_FEATURES),
            (
                "categorical",
                categorical_pipeline,
                CATEGORICAL_FEATURES,
            ),
        ]
    )

    return Pipeline(
        steps=[
            ("preprocessor", preprocessor),
            (
                "isolation_forest",
                IsolationForest(
                    n_estimators=N_ESTIMATORS,
                    contamination="auto",
                    max_samples="auto",
                    random_state=RANDOM_STATE,
                    n_jobs=-1,
                ),
            ),
        ]
    )


def train_model(data, output_dir="models"):
    """Train and persist the Isolation Forest model."""

    validate_training_data(data)

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

    model = build_model()

    logger.info(
        "Training Isolation Forest using %s transactions",
        len(features),
    )

    model.fit(features)

    # Isolation Forest returns -1 for anomalies and 1 for normal data.
    predictions = model.predict(features)

    # Lower decision_function values are more anomalous.
    decision_scores = model.decision_function(features)

    anomaly_count = int((predictions == -1).sum())
    normal_count = int((predictions == 1).sum())

    model_directory = Path(output_dir)
    model_directory.mkdir(parents=True, exist_ok=True)

    model_path = model_directory / "isolation_forest.joblib"
    metadata_path = model_directory / "isolation_forest_metadata.json"

    joblib.dump(model, model_path)

    metadata = {
        "model_name": "PayStream Isolation Forest",
        "algorithm": "IsolationForest",
        "trained_at": datetime.now(timezone.utc).isoformat(),
        "training_rows": int(len(features)),
        "numeric_features": NUMERIC_FEATURES,
        "categorical_features": CATEGORICAL_FEATURES,
        "excluded_reference_columns": [
            "statistical_is_anomaly",
            "statistical_robust_z_score",
            "statistical_baseline_status",
            "statistical_anomaly_reasons",
        ],
        "n_estimators": N_ESTIMATORS,
        "contamination": "auto",
        "random_state": RANDOM_STATE,
        "predicted_anomalies_in_training_data": anomaly_count,
        "predicted_normal_transactions_in_training_data": normal_count,
        "mean_decision_score": float(np.mean(decision_scores)),
        "minimum_decision_score": float(np.min(decision_scores)),
        "maximum_decision_score": float(np.max(decision_scores)),
        "model_file": model_path.name,
    }

    with metadata_path.open("w", encoding="utf-8") as file:
        json.dump(metadata, file, indent=4)

    report = {
        "training_rows": int(len(features)),
        "predicted_anomalies": anomaly_count,
        "predicted_normal": normal_count,
        "model_path": str(model_path),
        "metadata_path": str(metadata_path),
    }

    logger.info("Model training completed: %s", report)

    return report


def run_pipeline():
    """Load the dataset, train the model and close the connection."""

    connection = get_database_connection()

    try:
        data = load_training_data(connection)
        return train_model(data)
    finally:
        connection.close()


def main():
    report = run_pipeline()

    print("\nPayStream Isolation Forest Training Summary")
    print("-------------------------------------------")
    print(f"Training rows: {report['training_rows']}")
    print(f"Predicted anomalies: {report['predicted_anomalies']}")
    print(f"Predicted normal: {report['predicted_normal']}")
    print(f"Model: {report['model_path']}")
    print(f"Metadata: {report['metadata_path']}")


if __name__ == "__main__":
    main()
