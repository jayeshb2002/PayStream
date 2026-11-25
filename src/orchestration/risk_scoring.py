
"""Explainable transaction risk scoring engine for PayStream."""

from __future__ import annotations

from datetime import datetime, timezone
from time import perf_counter
from uuid import uuid4

from src.orchestration.risk_storage import persist_risk_results

import logging
import os
from collections import Counter
from typing import Any

import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)

# Initial business-rule thresholds.
FAILED_STATUS_VALUES = {
    "FAILED",
    "FAILURE",
    "DECLINED",
    "REJECTED",
}

MULTIPLE_FAILURE_THRESHOLD = 2
EXCESSIVE_EVENT_THRESHOLD = 5
LONG_LIFECYCLE_THRESHOLD_SECONDS = 300

# Risk weights.
RISK_WEIGHTS = {
    "FAILED_TRANSACTION": 35,
    "MULTIPLE_FAILED_EVENTS": 20,
    "ROBUST_Z_SCORE_ANOMALY": 25,
    "PERCENTILE_AMOUNT_ANOMALY": 15,
    "EXCESSIVE_LIFECYCLE_EVENTS": 10,
    "LONG_TRANSACTION_LIFECYCLE": 10,
}


TRANSACTION_RISK_SQL = """
SELECT
    f.transaction_id,
    f.payment_rail,
    f.currency,
    f.amount_minor,

    f.event_count,
    f.distinct_event_types,
    f.failed_event_count,

    f.latest_status,

    f.lifecycle_duration_seconds,

    COALESCE(a.is_robust_z_anomaly, FALSE)
        AS is_robust_z_anomaly,

    COALESCE(a.is_percentile_anomaly, FALSE)
        AS is_percentile_anomaly,

    COALESCE(a.is_anomaly, FALSE)
        AS is_anomaly,

    a.robust_z_score,
    a.baseline_median_minor,
    a.baseline_p95_minor,
    a.baseline_status

FROM transaction_features f

LEFT JOIN transaction_anomaly_results a
    ON f.transaction_id = a.transaction_id

ORDER BY f.last_event_time DESC;
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


def get_risk_category(score: int) -> str:
    """Convert a risk score into a risk category."""

    if not 0 <= score <= 100:
        raise ValueError("Risk score must be between 0 and 100")

    if score >= 70:
        return "CRITICAL"

    if score >= 40:
        return "HIGH"

    if score >= 20:
        return "MEDIUM"

    return "LOW"


def calculate_transaction_risk(
    transaction: dict[str, Any],
) -> dict[str, Any]:
    """
    Calculate an explainable risk score for one transaction.

    The same transaction input always produces the same result.
    """

    score = 0
    risk_flags = []

    latest_status = str(
        transaction.get("latest_status") or ""
    ).strip().upper()

    failed_event_count = int(
        transaction.get("failed_event_count") or 0
    )

    event_count = int(
        transaction.get("event_count") or 0
    )

    lifecycle_duration = float(
        transaction.get("lifecycle_duration_seconds") or 0
    )

    def add_flag(
        flag: str,
        description: str,
    ):
        nonlocal score

        points = RISK_WEIGHTS[flag]

        score += points

        risk_flags.append({
            "flag": flag,
            "points": points,
            "description": description,
        })

    # Rule 1: Latest transaction status is unsuccessful.
    if latest_status in FAILED_STATUS_VALUES:
        add_flag(
            "FAILED_TRANSACTION",
            f"Latest transaction status is {latest_status}.",
        )

    # Rule 2: Multiple failed lifecycle events.
    if failed_event_count >= MULTIPLE_FAILURE_THRESHOLD:
        add_flag(
            "MULTIPLE_FAILED_EVENTS",
            (
                f"Transaction contains {failed_event_count} "
                "failed lifecycle events."
            ),
        )

    # Rule 3: Robust Z-score anomaly.
    if transaction.get("is_robust_z_anomaly"):
        add_flag(
            "ROBUST_Z_SCORE_ANOMALY",
            "Transaction amount exceeds the robust Z-score threshold.",
        )

    # Rule 4: Amount above the 95th percentile.
    if transaction.get("is_percentile_anomaly"):
        add_flag(
            "PERCENTILE_AMOUNT_ANOMALY",
            "Transaction amount is above its payment-rail 95th percentile.",
        )

    # Rule 5: Excessive lifecycle events.
    if event_count > EXCESSIVE_EVENT_THRESHOLD:
        add_flag(
            "EXCESSIVE_LIFECYCLE_EVENTS",
            (
                f"Transaction contains {event_count} lifecycle events, "
                f"exceeding the threshold of "
                f"{EXCESSIVE_EVENT_THRESHOLD}."
            ),
        )

    # Rule 6: Long transaction lifecycle.
    if lifecycle_duration > LONG_LIFECYCLE_THRESHOLD_SECONDS:
        add_flag(
            "LONG_TRANSACTION_LIFECYCLE",
            (
                f"Transaction lifecycle took "
                f"{lifecycle_duration:.2f} seconds, "
                f"exceeding the threshold of "
                f"{LONG_LIFECYCLE_THRESHOLD_SECONDS} seconds."
            ),
        )

    final_score = min(score, 100)

    return {
        "transaction_id": transaction["transaction_id"],
        "risk_score": final_score,
        "risk_category": get_risk_category(final_score),
        "risk_flags": risk_flags,
        "risk_flag_count": len(risk_flags),
        "is_flagged": final_score >= 20,
    }


def score_transactions(
    transactions: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Calculate risk scores for a collection of transactions."""

    return [
        calculate_transaction_risk(transaction)
        for transaction in transactions
    ]


def fetch_transaction_features(connection) -> list[dict[str, Any]]:
    """Read transaction features and their anomaly indicators."""

    with connection.cursor(cursor_factory=RealDictCursor) as cursor:
        cursor.execute(TRANSACTION_RISK_SQL)
        rows = cursor.fetchall()

    return [dict(row) for row in rows]


def run_risk_scoring(connection) -> dict[str, Any]:
    """Execute risk scoring for all available transactions."""

    transactions = fetch_transaction_features(connection)

    results = score_transactions(transactions)

    category_counts = Counter(
        result["risk_category"]
        for result in results
    )

    flagged_count = sum(
        1 for result in results
        if result["is_flagged"]
    )

    summary = {
        "status": "SUCCESS",
        "total_transactions": len(results),
        "flagged_transactions": flagged_count,
        "risk_category_counts": {
            category: category_counts.get(category, 0)
            for category in (
                "LOW",
                "MEDIUM",
                "HIGH",
                "CRITICAL",
            )
        },
        "results": results,
    }

    logger.info(
        "Risk scoring completed: total=%s flagged=%s categories=%s",
        summary["total_transactions"],
        summary["flagged_transactions"],
        summary["risk_category_counts"],
    )

    return summary



def run_pipeline() -> dict[str, Any]:
    """
    Execute risk scoring and persist its results.

    Each execution receives a unique run_id so that historical
    analysis results are preserved.
    """

    connection = None
    started_at = datetime.now(timezone.utc)
    start_time = perf_counter()
    run_id = str(uuid4())

    try:
        connection = get_database_connection()

        # Step 1: Calculate risk scores.
        result = run_risk_scoring(connection)

        # Step 2: Calculate execution duration.
        execution_duration_ms = int(
            (perf_counter() - start_time) * 1000
        )

        # Step 3: Persist run summary and transaction results.
        storage_summary = persist_risk_results(
            connection=connection,
            run_id=run_id,
            results=result["results"],
            started_at=started_at,
            execution_duration_ms=execution_duration_ms,
        )

        # Step 4: Return the combined result.
        result.update(storage_summary)

        logger.info(
            "Risk analysis run %s completed successfully",
            run_id,
        )

        return result

    except Exception:
        logger.exception(
            "Risk scoring pipeline failed for run %s",
            run_id,
        )
        raise

    finally:
        if connection is not None:
            connection.close()



def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
    )

    result = run_pipeline()

    print("\nPayStream Transaction Risk Scoring")
    print("----------------------------------")
    print(f"Status: {result['status']}")
    print(f"Total transactions: {result['total_transactions']}")
    print(f"Flagged transactions: {result['flagged_transactions']}")
    print(f"Run ID: {result['run_id']}")
    print(
        f"Execution duration: "
        f"{result['execution_duration_ms']} ms"
    )

    print("\nRisk Category Distribution")

    for category, count in result["risk_category_counts"].items():
        print(f"{category}: {count}")

    print("\nTop 10 Highest-Risk Transactions")

    highest_risk = sorted(
        result["results"],
        key=lambda item: item["risk_score"],
        reverse=True,
    )[:10]

    for transaction in highest_risk:
        print(
            f"{transaction['transaction_id']} | "
            f"Score: {transaction['risk_score']} | "
            f"Category: {transaction['risk_category']}"
        )

        for flag in transaction["risk_flags"]:
            print(
                f"  - {flag['flag']}: "
                f"{flag['description']}"
            )


if __name__ == "__main__":
    main()
