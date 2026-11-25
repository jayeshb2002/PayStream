
"""Statistical transaction anomaly detection for PayStream."""

from __future__ import annotations

import logging
import os
from typing import Any

import psycopg2
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)

MINIMUM_BASELINE_SIZE = 5
ROBUST_Z_THRESHOLD = 3.5

ANOMALY_DETECTION_SQL = """
WITH rail_baselines AS (
    SELECT
        payment_rail,
        currency,
        COUNT(*)::INTEGER AS baseline_transaction_count,

        PERCENTILE_CONT(0.5)
            WITHIN GROUP (ORDER BY amount_minor)
            AS baseline_median_minor,

        PERCENTILE_CONT(0.95)
            WITHIN GROUP (ORDER BY amount_minor)
            AS baseline_p95_minor

    FROM transaction_features

    GROUP BY payment_rail, currency
),

absolute_deviations AS (
    SELECT
        f.transaction_id,
        f.payment_rail,
        f.currency,
        f.amount_minor,

        b.baseline_transaction_count,
        b.baseline_median_minor,
        b.baseline_p95_minor,

        ABS(
            f.amount_minor - b.baseline_median_minor
        ) AS absolute_deviation

    FROM transaction_features f

    JOIN rail_baselines b
      ON f.payment_rail = b.payment_rail
     AND f.currency = b.currency
),

mad_baselines AS (
    SELECT
        payment_rail,
        currency,

        PERCENTILE_CONT(0.5)
            WITHIN GROUP (ORDER BY absolute_deviation)
            AS baseline_mad_minor

    FROM absolute_deviations

    GROUP BY payment_rail, currency
),

scored_transactions AS (
    SELECT
        d.transaction_id,
        d.payment_rail,
        d.currency,
        d.amount_minor,

        d.baseline_transaction_count,
        d.baseline_median_minor,
        m.baseline_mad_minor,
        d.baseline_p95_minor,

        CASE
            WHEN m.baseline_mad_minor > 0
            THEN
                0.6745 *
                (d.amount_minor - d.baseline_median_minor)
                / m.baseline_mad_minor
            ELSE NULL
        END AS robust_z_score,

        CASE
            WHEN d.baseline_transaction_count >= %s
             AND m.baseline_mad_minor > 0
             AND d.amount_minor > d.baseline_median_minor
             AND (
                0.6745 *
                (d.amount_minor - d.baseline_median_minor)
                / m.baseline_mad_minor
             ) >= %s
            THEN TRUE
            ELSE FALSE
        END AS is_robust_z_anomaly,

        CASE
            WHEN d.baseline_transaction_count >= %s
             AND d.amount_minor > d.baseline_p95_minor
            THEN TRUE
            ELSE FALSE
        END AS is_percentile_anomaly

    FROM absolute_deviations d

    JOIN mad_baselines m
      ON d.payment_rail = m.payment_rail
     AND d.currency = m.currency
),

classified_transactions AS (
    SELECT
        *,

        (
            is_robust_z_anomaly
            OR is_percentile_anomaly
        ) AS is_anomaly,

        CASE
            WHEN baseline_transaction_count < %s
            THEN 'INSUFFICIENT_DATA'
            ELSE 'READY'
        END AS baseline_status,

        ARRAY_REMOVE(
            ARRAY[
                CASE
                    WHEN is_robust_z_anomaly
                    THEN 'ROBUST_Z_SCORE_THRESHOLD_EXCEEDED'
                END,

                CASE
                    WHEN is_percentile_anomaly
                    THEN 'AMOUNT_ABOVE_95TH_PERCENTILE'
                END
            ]::TEXT[],
            NULL
        ) AS anomaly_reasons

    FROM scored_transactions
)

INSERT INTO transaction_anomaly_results (
    transaction_id,
    payment_rail,
    currency,
    amount_minor,
    baseline_transaction_count,
    baseline_median_minor,
    baseline_mad_minor,
    baseline_p95_minor,
    robust_z_score,
    is_robust_z_anomaly,
    is_percentile_anomaly,
    is_anomaly,
    baseline_status,
    anomaly_reasons,
    analyzed_at
)

SELECT
    transaction_id,
    payment_rail,
    currency,
    amount_minor,
    baseline_transaction_count,
    baseline_median_minor,
    baseline_mad_minor,
    baseline_p95_minor,
    robust_z_score,
    is_robust_z_anomaly,
    is_percentile_anomaly,
    is_anomaly,
    baseline_status,
    anomaly_reasons,
    CURRENT_TIMESTAMP

FROM classified_transactions

ON CONFLICT (transaction_id)
DO UPDATE SET

    payment_rail = EXCLUDED.payment_rail,
    currency = EXCLUDED.currency,
    amount_minor = EXCLUDED.amount_minor,

    baseline_transaction_count =
        EXCLUDED.baseline_transaction_count,

    baseline_median_minor =
        EXCLUDED.baseline_median_minor,

    baseline_mad_minor =
        EXCLUDED.baseline_mad_minor,

    baseline_p95_minor =
        EXCLUDED.baseline_p95_minor,

    robust_z_score = EXCLUDED.robust_z_score,

    is_robust_z_anomaly =
        EXCLUDED.is_robust_z_anomaly,

    is_percentile_anomaly =
        EXCLUDED.is_percentile_anomaly,

    is_anomaly = EXCLUDED.is_anomaly,

    baseline_status = EXCLUDED.baseline_status,

    anomaly_reasons = EXCLUDED.anomaly_reasons,

    analyzed_at = CURRENT_TIMESTAMP;
"""


def get_database_connection():
    """Create a PostgreSQL connection."""

    return psycopg2.connect(
        host=os.getenv("PAYSTREAM_DB_HOST", "localhost"),
        port=int(os.getenv("PAYSTREAM_DB_PORT", "5433")),
        dbname=os.getenv("PAYSTREAM_DB_NAME", "paystream"),
        user=os.getenv("PAYSTREAM_DB_USER", "paystream"),
        password=os.getenv("PAYSTREAM_DB_PASSWORD", "password"),
        connect_timeout=10,
    )


def run_anomaly_detection(
    connection,
    minimum_baseline_size: int = MINIMUM_BASELINE_SIZE,
    robust_z_threshold: float = ROBUST_Z_THRESHOLD,
) -> dict[str, Any]:
    """Calculate statistical baselines and classify transaction anomalies."""

    if minimum_baseline_size < 1:
        raise ValueError("minimum_baseline_size must be at least 1")

    if robust_z_threshold <= 0:
        raise ValueError("robust_z_threshold must be positive")

    cursor = connection.cursor()

    try:
        cursor.execute(
            ANOMALY_DETECTION_SQL,
            (
                minimum_baseline_size,
                robust_z_threshold,
                minimum_baseline_size,
                minimum_baseline_size,
            ),
        )

        affected_rows = cursor.rowcount

        connection.commit()

        logger.info(
            "Anomaly detection completed. "
            "Rows inserted or updated: %s",
            affected_rows,
        )

        return {
            "status": "SUCCESS",
            "rows_analyzed": affected_rows,
            "minimum_baseline_size": minimum_baseline_size,
            "robust_z_threshold": robust_z_threshold,
        }

    except Exception:
        connection.rollback()
        logger.exception("Transaction anomaly detection failed")
        raise

    finally:
        cursor.close()


def run_pipeline() -> dict[str, Any]:
    connection = None

    try:
        connection = get_database_connection()
        return run_anomaly_detection(connection)

    finally:
        if connection is not None:
            connection.close()


def main():
    import argparse

    parser = argparse.ArgumentParser(
        description="PayStream statistical anomaly detection"
    )

    parser.add_argument(
        "--minimum-baseline-size",
        type=int,
        default=MINIMUM_BASELINE_SIZE,
    )

    parser.add_argument(
        "--robust-z-threshold",
        type=float,
        default=ROBUST_Z_THRESHOLD,
    )

    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
    )

    connection = None

    try:
        connection = get_database_connection()

        result = run_anomaly_detection(
            connection,
            minimum_baseline_size=args.minimum_baseline_size,
            robust_z_threshold=args.robust_z_threshold,
        )

        print("\nPayStream Anomaly Detection")
        print("---------------------------")
        for key, value in result.items():
            print(f"{key}: {value}")

    finally:
        if connection is not None:
            connection.close()


if __name__ == "__main__":
    main()
