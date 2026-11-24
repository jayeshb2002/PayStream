
from unittest.mock import MagicMock

from src.orchestration.quality_monitor import (
    calculate_quality_score,
    persist_quality_results
)


def test_quality_score_all_checks_pass():
    report = {
        "total_checks": 13,
        "passed_checks": 13
    }

    assert calculate_quality_score(report) == 100.0


def test_quality_score_one_check_fails():
    report = {
        "total_checks": 13,
        "passed_checks": 12
    }

    assert calculate_quality_score(report) == 92.31


def test_quality_score_no_checks():
    report = {
        "total_checks": 0,
        "passed_checks": 0
    }

    assert calculate_quality_score(report) == 0.0


def test_persist_quality_results():
    connection = MagicMock()
    cursor = MagicMock()

    connection.cursor.return_value.__enter__.return_value = cursor

    report = {
        "total_records": 300,
        "total_checks": 13,
        "passed_checks": 12,
        "failed_checks_count": 1,
        "execution_duration_ms": 250,
        "check_results": {
            "invalid_amounts": {
                "failed_records": 2,
                "status": "FAILED"
            },
            "invalid_currency": {
                "failed_records": 0,
                "status": "PASSED"
            }
        }
    }

    from datetime import datetime, timezone
    import uuid

    run_id = uuid.uuid4()
    now = datetime.now(timezone.utc)

    persist_quality_results(
        connection=connection,
        run_id=run_id,
        report=report,
        quality_score=92.31,
        threshold_score=95.0,
        status="WARNING",
        started_at=now,
        completed_at=now
    )

    assert cursor.execute.call_count == 3
    connection.commit.assert_called_once()
