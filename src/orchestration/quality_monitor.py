
import logging
import os
import uuid

from datetime import datetime, timezone
from decimal import Decimal

import psycopg2

from src.orchestration.data_quality import run_data_quality_checks


logger = logging.getLogger(__name__)

DEFAULT_QUALITY_THRESHOLD = 95.0


def get_database_connection():
    return psycopg2.connect(
        host=os.environ["PAYSTREAM_DB_HOST"],
        port=int(os.getenv("PAYSTREAM_DB_PORT", "5432")),
        dbname=os.environ["PAYSTREAM_DB_NAME"],
        user=os.environ["PAYSTREAM_DB_USER"],
        password=os.environ["PAYSTREAM_DB_PASSWORD"],
        connect_timeout=5
    )


def calculate_quality_score(report):
    total_checks = report["total_checks"]

    if total_checks == 0:
        return 0.0

    return round(
        (report["passed_checks"] / total_checks) * 100,
        2
    )


def persist_quality_results(
    connection,
    run_id,
    report,
    quality_score,
    threshold_score,
    status,
    started_at,
    completed_at
):
    """Persist quality summary and individual check results."""

    with connection.cursor() as cursor:

        cursor.execute(
            """
            INSERT INTO data_quality_runs (
                run_id,
                total_records,
                total_checks,
                passed_checks,
                failed_checks,
                quality_score,
                threshold_score,
                status,
                execution_duration_ms,
                started_at,
                completed_at
            )
            VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
            )
            """,
            (
                str(run_id),
                report["total_records"],
                report["total_checks"],
                report["passed_checks"],
                report["failed_checks_count"],
                quality_score,
                threshold_score,
                status,
                report["execution_duration_ms"],
                started_at,
                completed_at
            )
        )

        for check_name, result in report["check_results"].items():
            cursor.execute(
                """
                INSERT INTO data_quality_check_results (
                    run_id,
                    check_name,
                    failed_records,
                    status
                )
                VALUES (%s, %s, %s, %s)
                """,
                (
                    str(run_id),
                    check_name,
                    result["failed_records"],
                    result["status"]
                )
            )

    connection.commit()


def run_quality_monitor(threshold_score=None):
    """
    Execute data quality checks and persist monitoring results.

    Threshold determines whether a run is PASSED or WARNING.
    A FAILED run indicates that data quality checks themselves
    could not be executed successfully.
    """

    if threshold_score is None:
        threshold_score = float(
            os.getenv(
                "DATA_QUALITY_THRESHOLD",
                str(DEFAULT_QUALITY_THRESHOLD)
            )
        )

    if not 0 <= threshold_score <= 100:
        raise ValueError("Quality threshold must be between 0 and 100")

    run_id = uuid.uuid4()
    started_at = datetime.now(timezone.utc)

    connection = get_database_connection()

    try:
        report = run_data_quality_checks(connection)

        quality_score = calculate_quality_score(report)

        if quality_score >= threshold_score:
            status = "PASSED"
        else:
            status = "WARNING"

        completed_at = datetime.now(timezone.utc)

        persist_quality_results(
            connection=connection,
            run_id=run_id,
            report=report,
            quality_score=quality_score,
            threshold_score=threshold_score,
            status=status,
            started_at=started_at,
            completed_at=completed_at
        )

        result = {
            "run_id": str(run_id),
            "total_records": report["total_records"],
            "total_checks": report["total_checks"],
            "passed_checks": report["passed_checks"],
            "failed_checks_count": report["failed_checks_count"],
            "quality_score": quality_score,
            "threshold_score": threshold_score,
            "execution_duration_ms": report["execution_duration_ms"],
            "status": status,
            "failed_checks": report["failed_checks"]
        }

        logger.info(
            "Data quality run completed: %s",
            result
        )

        if status == "WARNING":
            logger.warning(
                "Data quality score %.2f%% is below threshold %.2f%%",
                quality_score,
                threshold_score
            )

        return result

    except Exception:
        connection.rollback()
        logger.exception("Data quality monitoring failed")
        raise

    finally:
        connection.close()
