
"""PostgreSQL persistence for PayStream transaction risk results."""

import logging
from typing import Any
from uuid import UUID

from psycopg2.extras import Json

logger = logging.getLogger(__name__)

DELETE_RISK_RESULTS_SQL = """
DELETE FROM transaction_risk_results
WHERE run_id = %s;
"""

INSERT_RISK_RUN_SQL = """
INSERT INTO risk_analysis_runs (
    run_id,
    total_transactions,
    flagged_transactions,
    low_risk_count,
    medium_risk_count,
    high_risk_count,
    critical_risk_count,
    execution_duration_ms,
    status,
    started_at,
    completed_at
)
VALUES (
    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW()
)
ON CONFLICT (run_id)
DO UPDATE SET
    total_transactions = EXCLUDED.total_transactions,
    flagged_transactions = EXCLUDED.flagged_transactions,
    low_risk_count = EXCLUDED.low_risk_count,
    medium_risk_count = EXCLUDED.medium_risk_count,
    high_risk_count = EXCLUDED.high_risk_count,
    critical_risk_count = EXCLUDED.critical_risk_count,
    execution_duration_ms = EXCLUDED.execution_duration_ms,
    status = EXCLUDED.status,
    completed_at = NOW();
"""


INSERT_RISK_RESULT_SQL = """
INSERT INTO transaction_risk_results (
    run_id,
    transaction_id,
    risk_score,
    risk_category,
    risk_flags,
    risk_flag_count,
    is_flagged
)
VALUES (%s, %s, %s, %s, %s, %s, %s)

ON CONFLICT (run_id, transaction_id)
DO UPDATE SET
    risk_score = EXCLUDED.risk_score,
    risk_category = EXCLUDED.risk_category,
    risk_flags = EXCLUDED.risk_flags,
    risk_flag_count = EXCLUDED.risk_flag_count,
    is_flagged = EXCLUDED.is_flagged,
    analyzed_at = NOW();
"""


def validate_risk_result(result: dict[str, Any]) -> None:
    """Validate a risk result before writing it to PostgreSQL."""

    transaction_id = result.get("transaction_id")

    if not transaction_id or not str(transaction_id).strip():
        raise ValueError("transaction_id cannot be empty")

    score = result.get("risk_score")

    if isinstance(score, bool) or not isinstance(score, int):
        raise ValueError("risk_score must be an integer")

    if not 0 <= score <= 100:
        raise ValueError("risk_score must be between 0 and 100")

    if result.get("risk_category") not in {
        "LOW",
        "MEDIUM",
        "HIGH",
        "CRITICAL",
    }:
        raise ValueError("Invalid risk category")

    flags = result.get("risk_flags")

    if not isinstance(flags, list):
        raise ValueError("risk_flags must be a list")

    if result.get("risk_flag_count") != len(flags):
        raise ValueError("risk_flag_count does not match risk_flags")

    if not isinstance(result.get("is_flagged"), bool):
        raise ValueError("is_flagged must be a boolean")


def build_run_summary(
    results: list[dict[str, Any]],
) -> dict[str, int]:
    """Calculate transaction counts by risk category."""

    summary = {
        "total_transactions": len(results),
        "flagged_transactions": 0,
        "low_risk_count": 0,
        "medium_risk_count": 0,
        "high_risk_count": 0,
        "critical_risk_count": 0,
    }

    category_mapping = {
        "LOW": "low_risk_count",
        "MEDIUM": "medium_risk_count",
        "HIGH": "high_risk_count",
        "CRITICAL": "critical_risk_count",
    }

    for result in results:
        validate_risk_result(result)

        category = result["risk_category"]
        summary[category_mapping[category]] += 1

        if result["is_flagged"]:
            summary["flagged_transactions"] += 1

    return summary


def persist_risk_results(
    connection,
    run_id: str,
    results: list[dict[str, Any]],
    started_at,
    execution_duration_ms: int,
) -> dict[str, Any]:
    """
    Persist the run summary and transaction results atomically.

    A failure in any insert rolls back the entire transaction.
    """

    try:
        parsed_run_id = UUID(str(run_id))

        if (
            isinstance(execution_duration_ms, bool)
            or not isinstance(execution_duration_ms, int)
            or execution_duration_ms < 0
        ):
            raise ValueError(
                "execution_duration_ms must be a non-negative integer"
            )

        summary = build_run_summary(results)

        with connection.cursor() as cursor:
            cursor.execute(
                INSERT_RISK_RUN_SQL,
                (
                    str(parsed_run_id),
                    summary["total_transactions"],
                    summary["flagged_transactions"],
                    summary["low_risk_count"],
                    summary["medium_risk_count"],
                    summary["high_risk_count"],
                    summary["critical_risk_count"],
                    execution_duration_ms,
                    "COMPLETED",
                    started_at,
                ),
            )

            rows = [
                (
                    str(parsed_run_id),
                    result["transaction_id"],
                    result["risk_score"],
                    result["risk_category"],
                    Json(result["risk_flags"]),
                    result["risk_flag_count"],
                    result["is_flagged"],
                )
                for result in results
            ]

            if rows:
                cursor.execute(
                    DELETE_RISK_RESULTS_SQL,
                    (str(parsed_run_id),),
                )
                cursor.executemany(
                    INSERT_RISK_RESULT_SQL,
                    rows,
                )

        connection.commit()

        logger.info(
            "Persisted risk analysis run %s: %s transactions",
            parsed_run_id,
            summary["total_transactions"],
        )

        return {
            "run_id": str(parsed_run_id),
            **summary,
            "execution_duration_ms": execution_duration_ms,
            "status": "COMPLETED",
        }

    except Exception:
        connection.rollback()

        logger.exception(
            "Failed to persist risk analysis run %s",
            run_id,
        )

        raise
