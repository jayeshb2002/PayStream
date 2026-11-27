
"""Combine statistical risk scores with Isolation Forest predictions."""

from __future__ import annotations

import argparse
import logging
import time
import uuid
from datetime import datetime, timezone

import pandas as pd

from src.orchestration.ml_evaluation import get_database_connection

logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)

# Keep these aligned with the category boundaries in risk_scoring.py.
RISK_CATEGORY_BOUNDARIES = {
    "LOW": 25,
    "MEDIUM": 50,
    "HIGH": 75,
}

DEFAULT_ML_ADJUSTMENT_POINTS = 20


def get_risk_category(score):
    """Map an integrated score to its risk category."""

    if score < RISK_CATEGORY_BOUNDARIES["LOW"]:
        return "LOW"

    if score < RISK_CATEGORY_BOUNDARIES["MEDIUM"]:
        return "MEDIUM"

    if score < RISK_CATEGORY_BOUNDARIES["HIGH"]:
        return "HIGH"

    return "CRITICAL"


def calculate_integrated_risk(
    data,
    ml_adjustment_points=DEFAULT_ML_ADJUSTMENT_POINTS,
):
    """Combine existing statistical risk with ML anomaly predictions."""

    if data.empty:
        raise ValueError("Risk integration dataset is empty.")

    required_columns = {
        "transaction_id",
        "risk_score",
        "risk_category",
        "ml_anomaly_score",
        "ml_is_anomaly",
        "ml_prediction",
        "ml_run_id",
    }

    missing = required_columns - set(data.columns)

    if missing:
        raise ValueError(
            f"Missing integration columns: {sorted(missing)}"
        )

    if data["transaction_id"].isnull().any():
        raise ValueError("Transaction IDs cannot be null.")

    if data["transaction_id"].duplicated().any():
        raise ValueError("Duplicate transaction IDs found.")

    if data[list(required_columns - {"transaction_id", "risk_category", "ml_prediction", "ml_run_id"})].isnull().any().any():
        raise ValueError("Risk integration contains null numeric values.")

    if ml_adjustment_points < 0:
        raise ValueError("ML adjustment points cannot be negative.")

    results = data.copy()

    results["statistical_risk_score"] = pd.to_numeric(
        results["risk_score"],
        errors="raise",
    )

    if not results["statistical_risk_score"].between(0, 100).all():
        raise ValueError("Statistical risk scores must be between 0 and 100.")

    if not results["ml_prediction"].isin(["NORMAL", "ANOMALY"]).all():
        raise ValueError("Invalid ML prediction category.")

    if not (
        results["ml_is_anomaly"].astype(bool)
        == results["ml_prediction"].eq("ANOMALY")
    ).all():
        raise ValueError("ML prediction and anomaly flag do not match.")

    results["ml_adjustment_points"] = (
        results["ml_is_anomaly"].astype(bool).astype(int)
        * ml_adjustment_points
    )

    results["integrated_risk_score"] = (
        results["statistical_risk_score"]
        + results["ml_adjustment_points"]
    ).clip(upper=100)

    results["integrated_risk_category"] = (
        results["integrated_risk_score"].apply(get_risk_category)
    )

    results["integration_reason"] = results.apply(
        lambda row: (
            "Isolation Forest classified this transaction as anomalous; "
            f"{ml_adjustment_points} points added to the existing "
            "statistical risk score."
            if row["ml_is_anomaly"]
            else
            "Isolation Forest classified this transaction as normal; "
            "the existing statistical risk score was retained."
        ),
        axis=1,
    )

    results["ml_anomaly_score"] = pd.to_numeric(
        results["ml_anomaly_score"],
        errors="raise",
    )

    return results[
        [
            "transaction_id",
            "ml_run_id",
            "statistical_risk_score",
            "risk_category",
            "ml_anomaly_score",
            "ml_is_anomaly",
            "ml_prediction",
            "ml_adjustment_points",
            "integrated_risk_score",
            "integrated_risk_category",
            "integration_reason",
        ]
    ].rename(
        columns={
            "risk_category": "statistical_risk_category",
        }
    )


def load_integration_data(connection):
    """Load latest statistical risks and predictions from one ML run."""

    query = """
        WITH latest_ml_run AS (
            SELECT run_id
            FROM ml_prediction_runs
            WHERE status = 'COMPLETED'
            ORDER BY completed_at DESC
            LIMIT 1
        )
        SELECT
            risk.transaction_id,
            risk.risk_score,
            risk.risk_category,
            prediction.run_id AS ml_run_id,
            prediction.ml_anomaly_score,
            prediction.ml_is_anomaly,
            prediction.ml_prediction
        FROM latest_transaction_risk risk
        INNER JOIN transaction_ml_predictions prediction
            ON prediction.transaction_id = risk.transaction_id
        INNER JOIN latest_ml_run latest
            ON prediction.run_id = latest.run_id
    """

    data = pd.read_sql_query(query, connection)

    if data.empty:
        raise ValueError(
            "No matching statistical risk and ML prediction records. "
            "Run Milestone 5.5 and Milestone 6.5 first."
        )

    return data


def persist_integrated_risk(
    connection,
    results,
    ml_run_id,
    run_id=None,
    started_at=None,
    duration_ms=None,
    ml_adjustment_points=DEFAULT_ML_ADJUSTMENT_POINTS,
):
    """Persist an integrated risk run and its transaction results."""

    if results.empty:
        raise ValueError("Cannot persist an empty integration result.")

    if results["transaction_id"].duplicated().any():
        raise ValueError("Duplicate transaction IDs in integration results.")

    run_id = uuid.UUID(str(run_id)) if run_id else uuid.uuid4()
    ml_run_id = uuid.UUID(str(ml_run_id))

    started_at = started_at or datetime.now(timezone.utc)
    completed_at = datetime.now(timezone.utc)

    total = len(results)
    anomaly_count = int(results["ml_is_anomaly"].sum())
    adjusted_count = int((results["ml_adjustment_points"] > 0).sum())

    run_sql = """
        INSERT INTO integrated_risk_analysis_runs (
            run_id,
            ml_run_id,
            total_transactions,
            ml_anomaly_count,
            adjusted_transaction_count,
            ml_adjustment_points,
            duration_ms,
            started_at,
            completed_at
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (run_id)
        DO UPDATE SET
            ml_run_id = EXCLUDED.ml_run_id,
            total_transactions = EXCLUDED.total_transactions,
            ml_anomaly_count = EXCLUDED.ml_anomaly_count,
            adjusted_transaction_count = EXCLUDED.adjusted_transaction_count,
            ml_adjustment_points = EXCLUDED.ml_adjustment_points,
            duration_ms = EXCLUDED.duration_ms,
            started_at = EXCLUDED.started_at,
            completed_at = EXCLUDED.completed_at
    """

    result_sql = """
        INSERT INTO transaction_integrated_risk_results (
            run_id,
            transaction_id,
            ml_run_id,
            statistical_risk_score,
            statistical_risk_category,
            ml_anomaly_score,
            ml_is_anomaly,
            ml_prediction,
            ml_adjustment_points,
            integrated_risk_score,
            integrated_risk_category,
            integration_reason
        )
        VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        )
        ON CONFLICT (run_id, transaction_id)
        DO UPDATE SET
            ml_run_id = EXCLUDED.ml_run_id,
            statistical_risk_score = EXCLUDED.statistical_risk_score,
            statistical_risk_category = EXCLUDED.statistical_risk_category,
            ml_anomaly_score = EXCLUDED.ml_anomaly_score,
            ml_is_anomaly = EXCLUDED.ml_is_anomaly,
            ml_prediction = EXCLUDED.ml_prediction,
            ml_adjustment_points = EXCLUDED.ml_adjustment_points,
            integrated_risk_score = EXCLUDED.integrated_risk_score,
            integrated_risk_category = EXCLUDED.integrated_risk_category,
            integration_reason = EXCLUDED.integration_reason
    """

    rows = [
        (
            str(run_id),
            str(row.transaction_id),
            str(row.ml_run_id),
            float(row.statistical_risk_score),
            str(row.statistical_risk_category),
            float(row.ml_anomaly_score),
            bool(row.ml_is_anomaly),
            str(row.ml_prediction),
            int(row.ml_adjustment_points),
            float(row.integrated_risk_score),
            str(row.integrated_risk_category),
            str(row.integration_reason),
        )
        for row in results.itertuples(index=False)
    ]

    try:
        with connection.cursor() as cursor:
            cursor.execute(
                run_sql,
                (
                    str(run_id),
                    str(ml_run_id),
                    total,
                    anomaly_count,
                    adjusted_count,
                    ml_adjustment_points,
                    duration_ms,
                    started_at,
                    completed_at,
                ),
            )

            cursor.executemany(result_sql, rows)

        connection.commit()

    except Exception:
        connection.rollback()
        logger.exception("Failed to persist integrated risk results.")
        raise

    return {
        "run_id": str(run_id),
        "ml_run_id": str(ml_run_id),
        "total_transactions": total,
        "ml_anomaly_count": anomaly_count,
        "adjusted_transaction_count": adjusted_count,
    }


def run_pipeline(
    ml_adjustment_points=DEFAULT_ML_ADJUSTMENT_POINTS,
):
    """Load, combine and persist statistical and ML risk results."""

    started_at = datetime.now(timezone.utc)
    start_time = time.perf_counter()

    connection = get_database_connection()

    try:
        data = load_integration_data(connection)

        results = calculate_integrated_risk(
            data,
            ml_adjustment_points=ml_adjustment_points,
        )

        duration_ms = int(
            (time.perf_counter() - start_time) * 1000
        )

        ml_run_ids = results["ml_run_id"].dropna().unique()

        if len(ml_run_ids) != 1:
            raise ValueError(
                "Integration must use predictions from exactly one ML run."
            )

        summary = persist_integrated_risk(
            connection=connection,
            results=results,
            ml_run_id=ml_run_ids[0],
            started_at=started_at,
            duration_ms=duration_ms,
            ml_adjustment_points=ml_adjustment_points,
        )

        summary["duration_ms"] = duration_ms

        logger.info("Integrated risk summary: %s", summary)

        return summary

    finally:
        connection.close()


def main():
    parser = argparse.ArgumentParser(
        description="Integrate ML predictions with statistical risk scores."
    )

    parser.add_argument(
        "--ml-adjustment-points",
        type=int,
        default=DEFAULT_ML_ADJUSTMENT_POINTS,
    )

    args = parser.parse_args()

    summary = run_pipeline(
        ml_adjustment_points=args.ml_adjustment_points,
    )

    print("\nIntegrated Risk Analysis Summary")
    print("--------------------------------")
    for key, value in summary.items():
        print(f"{key}: {value}")


if __name__ == "__main__":
    main()
