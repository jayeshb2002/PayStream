
import pytest

from src.orchestration.risk_storage import (
    build_run_summary,
    persist_risk_results,
    validate_risk_result,
)


def make_result(
    transaction_id="TXN-001",
    score=0,
    category="LOW",
    flags=None,
    is_flagged=False,
):
    flags = flags or []

    return {
        "transaction_id": transaction_id,
        "risk_score": score,
        "risk_category": category,
        "risk_flags": flags,
        "risk_flag_count": len(flags),
        "is_flagged": is_flagged,
    }


def test_build_run_summary():
    results = [
        make_result("TXN-001", 10, "LOW"),
        make_result("TXN-002", 30, "MEDIUM", is_flagged=True),
        make_result("TXN-003", 50, "HIGH", is_flagged=True),
        make_result("TXN-004", 80, "CRITICAL", is_flagged=True),
    ]

    summary = build_run_summary(results)

    assert summary["total_transactions"] == 4
    assert summary["flagged_transactions"] == 3
    assert summary["low_risk_count"] == 1
    assert summary["medium_risk_count"] == 1
    assert summary["high_risk_count"] == 1
    assert summary["critical_risk_count"] == 1


def test_empty_results_summary():
    summary = build_run_summary([])

    assert summary["total_transactions"] == 0
    assert summary["flagged_transactions"] == 0
    assert summary["low_risk_count"] == 0


def test_validate_valid_result():
    result = make_result(
        score=35,
        category="MEDIUM",
        flags=[
            {
                "flag": "FAILED_TRANSACTION",
                "points": 35,
                "description": "Transaction failed.",
            }
        ],
        is_flagged=True,
    )

    validate_risk_result(result)


def test_reject_score_above_100():
    result = make_result(score=110)

    with pytest.raises(ValueError):
        validate_risk_result(result)


def test_reject_invalid_category():
    result = make_result(category="UNKNOWN")

    with pytest.raises(ValueError):
        validate_risk_result(result)


def test_reject_missing_transaction_id():
    result = make_result(transaction_id="")

    with pytest.raises(ValueError):
        validate_risk_result(result)


def test_reject_invalid_risk_flags():
    result = make_result()
    result["risk_flags"] = "not-a-list"

    with pytest.raises(ValueError):
        validate_risk_result(result)


def test_reject_mismatched_flag_count():
    result = make_result(
        flags=[
            {
                "flag": "FAILED_TRANSACTION",
                "points": 35,
                "description": "Transaction failed.",
            }
        ]
    )

    result["risk_flag_count"] = 0

    with pytest.raises(ValueError):
        validate_risk_result(result)


def test_persist_risk_results_commits_transaction():
    class FakeCursor:
        def __init__(self):
            self.executed = []
            self.executemany_calls = []

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc_value, traceback):
            return False

        def execute(self, query, params=None):
            self.executed.append((query, params))

        def executemany(self, query, rows):
            self.executemany_calls.append((query, rows))

    class FakeConnection:
        def __init__(self):
            self.fake_cursor = FakeCursor()
            self.committed = False
            self.rolled_back = False

        def cursor(self):
            return self.fake_cursor

        def commit(self):
            self.committed = True

        def rollback(self):
            self.rolled_back = True

    connection = FakeConnection()

    results = [
        make_result("TXN-001", 10, "LOW"),
        make_result(
            "TXN-002",
            35,
            "MEDIUM",
            flags=[
                {
                    "flag": "FAILED_TRANSACTION",
                    "points": 35,
                    "description": "Transaction failed.",
                }
            ],
            is_flagged=True,
        ),
    ]

    summary = persist_risk_results(
        connection=connection,
        run_id="12345678-1234-5678-1234-567812345678",
        results=results,
        started_at="2026-11-25T00:00:00+00:00",
        execution_duration_ms=250,
    )

    assert connection.committed is True
    assert connection.rolled_back is False
    assert summary["status"] == "COMPLETED"
    assert summary["total_transactions"] == 2
    assert summary["flagged_transactions"] == 1

    # Run header insert and existing-result cleanup.
    assert len(connection.fake_cursor.executed) == 2

    # Two transaction results were submitted.
    assert len(connection.fake_cursor.executemany_calls) == 1
    assert len(
        connection.fake_cursor.executemany_calls[0][1]
    ) == 2


def test_persist_failure_rolls_back():
    class FakeCursor:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc_value, traceback):
            return False

        def execute(self, query, params=None):
            raise RuntimeError("Database failure")

    class FakeConnection:
        def __init__(self):
            self.rolled_back = False

        def cursor(self):
            return FakeCursor()

        def commit(self):
            raise AssertionError("Should not commit")

        def rollback(self):
            self.rolled_back = True

    connection = FakeConnection()

    with pytest.raises(RuntimeError):
        persist_risk_results(
            connection=connection,
            run_id="12345678-1234-5678-1234-567812345678",
            results=[],
            started_at="2026-11-25T00:00:00+00:00",
            execution_duration_ms=100,
        )

    assert connection.rolled_back is True
