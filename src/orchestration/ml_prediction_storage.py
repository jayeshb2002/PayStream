
"""Persist transaction-level ML predictions in PostgreSQL."""

from __future__ import annotations

import argparse
import logging
import time
import uuid
from datetime import datetime, timezone

from src.orchestration.ml_prediction import run_pipeline
from src.orchestration.ml_evaluation import get_database_connection

logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)


def persist_predictions(
    connection,
    predictions,
    summary,
    run_id=None,
    started_at=None,
    duration_ms=None,
):
    """Persist one prediction run and its transaction-level results.

    Reusing the same run_id makes retries idempotent:
    the same transaction predictions are updated rather than duplicated.
    """

    if predictions.empty:
        raise ValueError("Cannot persist an empty prediction dataset.")

    required_columns = {
        "transaction_id",
        "ml_anomaly_score",
        "ml_is_anomaly",
        "ml_prediction",
        "ml_threshold",
        "model_name",
        "model_percentile",
        "predicted_at",
    }

    missing_columns = required_columns - set(predictions.columns)

    if missing_columns:
        raise ValueError(
            f"Prediction dataset is missing columns: "
            f"{sorted(missing_columns)}"
        )

    if predictions["transaction_id"].isnull().any():
        raise ValueError("Prediction transaction IDs cannot be null.")

    if predictions["transaction_id"].duplicated().any():
        raise ValueError("Duplicate transaction IDs in prediction results.")

    run_id = uuid.UUID(str(run_id)) if run_id else uuid.uuid4()

    started_at = started_at or datetime.now(timezone.utc)
    completed_at = datetime.now(timezone.utc)

    model_names = predictions["model_name"].dropna().unique()

    if len(model_names) != 1:
        raise ValueError("A prediction run must use exactly one model name.")

    thresholds = predictions["ml_threshold"].dropna().unique()
    percentiles = predictions["model_percentile"].dropna().unique()

    if len(thresholds) != 1 or len(percentiles) != 1:
        raise ValueError(
            "A prediction run must use one threshold and percentile."
        )

    model_name = str(model_names[0])
    threshold = float(thresholds[0])
    percentile = float(percentiles[0])

    total = len(predictions)
    anomaly_count = int(predictions["ml_is_anomaly"].sum())
    normal_count = total - anomaly_count

    anomaly_rate = round(
        (anomaly_count / total) * 100,
        2,
    )

    if int(summary["total_transactions"]) != total:
        raise ValueError("Prediction summary count does not match results.")

    if int(summary["anomalies"]) != anomaly_count:
        raise ValueError("Prediction summary anomaly count does not match.")

    if int(summary["normal_transactions"]) != normal_count:
        raise ValueError("Prediction summary normal count does not match.")

    run_sql = """
        INSERT INTO ml_prediction_runs (
            run_id,
            model_name,
            threshold,
            percentile,
            total_transactions,
            anomaly_count,
            normal_count,
            anomaly_rate_percent,
            duration_ms,
            status,
            started_at,
            completed_at
        )
        VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s,
            'COMPLETED', %s, %s
        )
        ON CONFLICT (run_id)
        DO UPDATE SET
            model_name = EXCLUDED.model_name,
            threshold = EXCLUDED.threshold,
            percentile = EXCLUDED.percentile,
            total_transactions = EXCLUDED.total_transactions,
            anomaly_count = EXCLUDED.anomaly_count,
            normal_count = EXCLUDED.normal_count,
            anomaly_rate_percent = EXCLUDED.anomaly_rate_percent,
            duration_ms = EXCLUDED.duration_ms,
            status = EXCLUDED.status,
            started_at = EXCLUDED.started_at,
            completed_at = EXCLUDED.completed_at
    """

    prediction_sql = """
        INSERT INTO transaction_ml_predictions (
            run_id,
            transaction_id,
            ml_anomaly_score,
            ml_is_anomaly,
            ml_prediction,
            ml_threshold,
            model_name,
            model_percentile,
            predicted_at
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (run_id, transaction_id)
        DO UPDATE SET
            ml_anomaly_score = EXCLUDED.ml_anomaly_score,
            ml_is_anomaly = EXCLUDED.ml_is_anomaly,
            ml_prediction = EXCLUDED.ml_prediction,
            ml_threshold = EXCLUDED.ml_threshold,
            model_name = EXCLUDED.model_name,
            model_percentile = EXCLUDED.model_percentile,
            predicted_at = EXCLUDED.predicted_at
    """

    prediction_rows = [
        (
            str(run_id),
            str(row.transaction_id),
            float(row.ml_anomaly_score),
            bool(row.ml_is_anomaly),
            str(row.ml_prediction),
            float(row.ml_threshold),
            str(row.model_name),
            float(row.model_percentile),
            row.predicted_at.to_pydatetime()
            if hasattr(row.predicted_at, "to_pydatetime")
            else row.predicted_at,
        )
        for row in predictions.itertuples(index=False)
    ]

    try:
        with connection.cursor() as cursor:
            cursor.execute(
                run_sql,
                (
                    str(run_id),
                    model_name,
                    threshold,
                    percentile,
                    total,
                    anomaly_count,
                    normal_count,
                    anomaly_rate,
                    duration_ms,
                    started_at,
                    completed_at,
                ),
            )

            cursor.executemany(
                prediction_sql,
                prediction_rows,
            )

        connection.commit()

    except Exception:
        connection.rollback()
        logger.exception("Failed to persist ML prediction run %s", run_id)
        raise

    logger.info(
        "Persisted ML prediction run %s: %s transactions, %s anomalies",
        run_id,
        total,
        anomaly_count,
    )

    return {
        "run_id": str(run_id),
        "total_transactions": total,
        "anomalies": anomaly_count,
        "normal_transactions": normal_count,
        "anomaly_rate_percent": anomaly_rate,
        "threshold": threshold,
        "model_name": model_name,
    }


def run_and_persist(
    model_path="models/isolation_forest.joblib",
    threshold_path="models/ml_threshold.json",
):
    """Generate predictions and persist them in one database transaction."""

    started_at = datetime.now(timezone.utc)
    start_time = time.perf_counter()

    predictions, summary = run_pipeline(
        model_path=model_path,
        threshold_path=threshold_path,
    )

    duration_ms = int(
        (time.perf_counter() - start_time) * 1000
    )

    connection = get_database_connection()

    try:
        result = persist_predictions(
            connection=connection,
            predictions=predictions,
            summary=summary,
            started_at=started_at,
            duration_ms=duration_ms,
        )
    finally:
        connection.close()

    return result


def main():
    parser = argparse.ArgumentParser(
        description="Generate and persist PayStream ML predictions."
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

    result = run_and_persist(
        model_path=args.model_path,
        threshold_path=args.threshold_path,
    )

    print("\nML Prediction Persistence Summary")
    print("---------------------------------")
    print(f"Run ID: {result['run_id']}")
    print(f"Model: {result['model_name']}")
    print(f"Transactions: {result['total_transactions']}")
    print(f"Anomalies: {result['anomalies']}")
    print(f"Normal: {result['normal_transactions']}")
    print(f"Anomaly rate: {result['anomaly_rate_percent']}%")
    print(f"Threshold: {result['threshold']}")


if __name__ == "__main__":
    main()
