
from __future__ import annotations

import logging
import math
import statistics
import time
import uuid

from collections import defaultdict
from datetime import datetime, timezone
from typing import Any

import psycopg2

from psycopg2.extras import execute_values


logger = logging.getLogger(__name__)


FEATURE_QUERY = """
SELECT
    transaction_id,
    payment_rail,
    amount_minor,
    currency,
    sender_bank,
    receiver_bank,
    latest_status,
    event_count,
    distinct_event_types,
    failed_event_count,
    lifecycle_duration_seconds
FROM transaction_features
ORDER BY transaction_id
"""


def percentile(values: list[int], percentage: float) -> float:
    """Calculate an interpolated percentile."""

    if not values:
        return 0.0

    ordered = sorted(values)

    position = (len(ordered) - 1) * percentage

    lower = math.floor(position)
    upper = math.ceil(position)

    if lower == upper:
        return float(ordered[lower])

    fraction = position - lower

    return (
        ordered[lower] * (1 - fraction)
        + ordered[upper] * fraction
    )


def build_rail_baselines(
    features: list[dict[str, Any]],
) -> dict[str, dict[str, float]]:
    """Calculate amount baselines independently for each payment rail."""

    grouped = defaultdict(list)

    for feature in features:
        grouped[feature["payment_rail"]].append(
            int(feature["amount_minor"])
        )

    baselines = {}

    for rail, amounts in grouped.items():
        median_amount = statistics.median(amounts)

        deviations = [
            abs(amount - median_amount)
            for amount in amounts
        ]

        mad = statistics.median(deviations)

        baselines[rail] = {
            "median": float(median_amount),
            "mad": float(mad),
            "p95": percentile(amounts, 0.95),
        }

    return baselines


def calculate_risk(
    feature: dict[str, Any],
    baseline: dict[str, float],
) -> dict[str, Any]:
    """Calculate an explainable risk score for one transaction."""

    amount = int(feature["amount_minor"])

    median_amount = baseline["median"]
    mad = baseline["mad"]
    p95 = baseline["p95"]

    flags = []
    score = 0
    robust_z_score = 0.0

    # Robust statistical anomaly detection.
    if mad > 0:
        robust_z_score = (
            0.6745 * (amount - median_amount) / mad
        )

    amount_outlier = robust_z_score >= 3.5

    # Fallback for datasets where most amounts are identical.
    if mad == 0 and amount > p95:
        amount_outlier = True

    if amount > p95:
        score += 25
        flags.append("AMOUNT_ABOVE_RAIL_P95")

    if amount_outlier:
        score += 25
        flags.append("STATISTICAL_AMOUNT_OUTLIER")

    failed_count = int(feature["failed_event_count"])

    if failed_count >= 2:
        score += 25
        flags.append("MULTIPLE_FAILED_EVENTS")

    latest_status = str(
        feature["latest_status"]
    ).upper()

    if latest_status in {
        "FAILED",
        "DECLINED",
        "REJECTED",
    }:
        score += 20
        flags.append("LATEST_STATUS_FAILED")

    event_count = int(feature["event_count"])

    if event_count > 4:
        score += 15
        flags.append("UNUSUALLY_MANY_EVENTS")

    duration = int(
        feature["lifecycle_duration_seconds"]
    )

    if duration > 1800:
        score += 15
        flags.append("LONG_PROCESSING_DURATION")

    score = min(score, 100)

    if score >= 75:
        category = "CRITICAL"
    elif score >= 50:
        category = "HIGH"
    elif score >= 25:
        category = "MEDIUM"
    else:
        category = "LOW"

    explanation = (
        "No configured risk rules were triggered."
        if not flags
        else "Triggered rules: " + ", ".join(flags)
    )

    return {
        "transaction_id": feature["transaction_id"],
        "payment_rail": feature["payment_rail"],
        "amount_minor": amount,
        "latest_status": latest_status,
        "risk_score": float(score),
        "risk_category": category,
        "is_anomaly": bool(amount_outlier),
        "robust_z_score": round(
            robust_z_score, 4
        ),
        "risk_flags": flags,
        "explanation": explanation,
    }


def load_features(connection) -> list[dict[str, Any]]:
    with connection.cursor() as cursor:
        cursor.execute(FEATURE_QUERY)

        columns = [
            description[0]
            for description in cursor.description
        ]

        return [
            dict(zip(columns, row))
            for row in cursor.fetchall()
        ]


def persist_risk_scores(
    connection,
    run_id: str,
    results: list[dict[str, Any]],
) -> None:

    sql = """
        INSERT INTO transaction_risk_scores (
            run_id,
            transaction_id,
            risk_score,
            risk_category,
            is_anomaly,
            robust_z_score,
            risk_flags,
            explanation,
            amount_minor,
            payment_rail,
            latest_status,
            analysed_at
        )
        VALUES %s
        ON CONFLICT (run_id, transaction_id)
        DO UPDATE SET
            risk_score = EXCLUDED.risk_score,
            risk_category = EXCLUDED.risk_category,
            is_anomaly = EXCLUDED.is_anomaly,
            robust_z_score = EXCLUDED.robust_z_score,
            risk_flags = EXCLUDED.risk_flags,
            explanation = EXCLUDED.explanation,
            amount_minor = EXCLUDED.amount_minor,
            payment_rail = EXCLUDED.payment_rail,
            latest_status = EXCLUDED.latest_status,
            analysed_at = EXCLUDED.analysed_at
    """

    values = [
        (
            run_id,
            result["transaction_id"],
            result["risk_score"],
            result["risk_category"],
            result["is_anomaly"],
            result["robust_z_score"],
            result["risk_flags"],
            result["explanation"],
            result["amount_minor"],
            result["payment_rail"],
            result["latest_status"],
            datetime.now(timezone.utc),
        )
        for result in results
    ]

    if values:
        with connection.cursor() as cursor:
            execute_values(cursor, sql, values)


def update_run_summary(
    connection,
    run_id: str,
    results: list[dict[str, Any]],
    duration_ms: int,
) -> None:

    counts = {
        category: sum(
            1 for result in results
            if result["risk_category"] == category
        )
        for category in (
            "LOW",
            "MEDIUM",
            "HIGH",
            "CRITICAL",
        )
    }

    anomaly_count = sum(
        1 for result in results
        if result["is_anomaly"]
    )

    with connection.cursor() as cursor:
        cursor.execute(
            """
            UPDATE risk_analysis_runs
            SET
                total_transactions = %s,
                analysed_transactions = %s,
                low_risk_count = %s,
                medium_risk_count = %s,
                high_risk_count = %s,
                critical_risk_count = %s,
                anomaly_count = %s,
                status = 'COMPLETED',
                execution_duration_ms = %s,
                completed_at = CURRENT_TIMESTAMP
            WHERE run_id = %s
            """,
            (
                (
                    len(results),
                    len(results),
                    counts["LOW"],
                    counts["MEDIUM"],
                    counts["HIGH"],
                    counts["CRITICAL"],
                    anomaly_count,
                    duration_ms,
                    run_id,
                )
            ),
        )


def run_risk_analysis(connection) -> dict[str, Any]:
    """Execute one complete risk analysis run."""

    run_id = str(uuid.uuid4())
    started = time.perf_counter()

    try:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO risk_analysis_runs (
                    run_id,
                    status,
                    started_at
                )
                VALUES (%s, 'RUNNING', CURRENT_TIMESTAMP)
                """,
                (run_id,),
            )

        connection.commit()

        features = load_features(connection)

        if not features:
            duration_ms = int(
                (time.perf_counter() - started) * 1000
            )

            update_run_summary(
                connection,
                run_id,
                [],
                duration_ms,
            )

            connection.commit()

            return {
                "run_id": run_id,
                "total_transactions": 0,
                "analysed_transactions": 0,
                "status": "COMPLETED",
            }

        baselines = build_rail_baselines(features)

        results = [
            calculate_risk(
                feature,
                baselines[feature["payment_rail"]],
            )
            for feature in features
        ]

        persist_risk_scores(
            connection,
            run_id,
            results,
        )

        duration_ms = int(
            (time.perf_counter() - started) * 1000
        )

        update_run_summary(
            connection,
            run_id,
            results,
            duration_ms,
        )

        connection.commit()

        report = {
            "run_id": run_id,
            "total_transactions": len(features),
            "analysed_transactions": len(results),
            "anomaly_count": sum(
                result["is_anomaly"]
                for result in results
            ),
            "high_risk_count": sum(
                result["risk_category"] in {
                    "HIGH",
                    "CRITICAL",
                }
                for result in results
            ),
            "execution_duration_ms": duration_ms,
            "status": "COMPLETED",
        }

        logger.info("Risk analysis completed: %s", report)

        return report

    except Exception:
        connection.rollback()

        try:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    UPDATE risk_analysis_runs
                    SET
                        status = 'FAILED',
                        completed_at = CURRENT_TIMESTAMP
                    WHERE run_id = %s
                    """,
                    (run_id,),
                )

            connection.commit()

        except Exception:
            connection.rollback()
            logger.exception(
                "Could not persist failed risk analysis run"
            )

        logger.exception("Risk analysis failed")

        raise
