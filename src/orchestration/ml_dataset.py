
"""Prepare the ML training dataset for PayStream."""

from __future__ import annotations

import argparse
import logging
import os
import time

import psycopg2
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)


ML_DATASET_SQL = """
    INSERT INTO transaction_ml_dataset (
        transaction_id,
        payment_rail,
        currency,
        latest_status,
        amount_minor,
        event_count,
        distinct_event_type_count,
        failed_event_count,
        lifecycle_duration_seconds,
        statistical_is_anomaly,
        statistical_robust_z_score,
        statistical_baseline_status,
        statistical_anomaly_reasons,
        source_feature_updated_at,
        dataset_updated_at
    )
    SELECT
        f.transaction_id,
        f.payment_rail,
        f.currency,
        f.latest_status,
        f.amount_minor,
        f.event_count,
        f.distinct_event_types,
        f.failed_event_count,
        f.lifecycle_duration_seconds,

        a.is_anomaly,
        a.robust_z_score,
        a.baseline_status,
        a.anomaly_reasons,

        f.updated_at,
        NOW()

    FROM transaction_features f

    LEFT JOIN transaction_anomaly_results a
        ON a.transaction_id = f.transaction_id

    WHERE
        f.transaction_id IS NOT NULL
        AND f.payment_rail IS NOT NULL
        AND f.currency IS NOT NULL
        AND f.amount_minor IS NOT NULL
        AND f.event_count IS NOT NULL
        AND f.distinct_event_types IS NOT NULL
        AND f.failed_event_count IS NOT NULL

    ON CONFLICT (transaction_id)
    DO UPDATE SET
        payment_rail = EXCLUDED.payment_rail,
        currency = EXCLUDED.currency,
        latest_status = EXCLUDED.latest_status,
        amount_minor = EXCLUDED.amount_minor,
        event_count = EXCLUDED.event_count,
        distinct_event_type_count =
            EXCLUDED.distinct_event_type_count,
        failed_event_count = EXCLUDED.failed_event_count,
        lifecycle_duration_seconds =
            EXCLUDED.lifecycle_duration_seconds,
        statistical_is_anomaly =
            EXCLUDED.statistical_is_anomaly,
        statistical_robust_z_score =
            EXCLUDED.statistical_robust_z_score,
        statistical_baseline_status =
            EXCLUDED.statistical_baseline_status,
        statistical_anomaly_reasons =
            EXCLUDED.statistical_anomaly_reasons,
        source_feature_updated_at =
            EXCLUDED.source_feature_updated_at,
        dataset_updated_at = NOW();
"""


def get_database_connection():
    """Create a connection to the PayStream PostgreSQL database."""

    return psycopg2.connect(
        host=os.getenv("PAYSTREAM_DB_HOST", "localhost"),
        port=int(os.getenv("PAYSTREAM_DB_PORT", "5433")),
        dbname=os.getenv("PAYSTREAM_DB_NAME", "paystream"),
        user=os.getenv("PAYSTREAM_DB_USER", "paystream"),
        password=os.getenv("PAYSTREAM_DB_PASSWORD"),
        connect_timeout=10,
    )


def prepare_ml_dataset(connection):
    """Build and persist the ML dataset."""

    started_at = time.perf_counter()

    try:
        with connection.cursor() as cursor:
            cursor.execute(ML_DATASET_SQL)

            cursor.execute(
                """
                SELECT
                    COUNT(*) AS total_records,
                    COUNT(*) FILTER (
                        WHERE statistical_is_anomaly = TRUE
                    ) AS statistical_anomalies,
                    COUNT(*) FILTER (
                        WHERE statistical_is_anomaly IS NULL
                    ) AS records_without_anomaly_result
                FROM transaction_ml_dataset;
                """
            )

            result = cursor.fetchone()

        connection.commit()

        duration_ms = round(
            (time.perf_counter() - started_at) * 1000
        )

        report = {
            "total_records": int(result[0]),
            "statistical_anomalies": int(result[1]),
            "records_without_anomaly_result": int(result[2]),
            "execution_duration_ms": duration_ms,
        }

        logger.info("ML dataset preparation completed: %s", report)

        return report

    except Exception:
        connection.rollback()
        logger.exception("ML dataset preparation failed")
        raise


def run_pipeline():
    """Run ML dataset preparation."""

    connection = get_database_connection()

    try:
        return prepare_ml_dataset(connection)
    finally:
        connection.close()


def main():
    parser = argparse.ArgumentParser(
        description="Prepare the PayStream ML training dataset."
    )

    parser.parse_args()

    report = run_pipeline()

    print("\nML Dataset Preparation Summary")
    print("--------------------------------")
    print(f"Total records: {report['total_records']}")
    print(
        "Statistical anomalies: "
        f"{report['statistical_anomalies']}"
    )
    print(
        "Records without anomaly result: "
        f"{report['records_without_anomaly_result']}"
    )
    print(
        "Execution duration: "
        f"{report['execution_duration_ms']} ms"
    )


if __name__ == "__main__":
    main()
