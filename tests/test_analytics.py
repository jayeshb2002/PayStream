
from datetime import date

import pytest

from src.orchestration.analytics import (
    generate_daily_analytics,
    generate_bank_analytics,
    _validate_target_date
)


class FakeCursor:
    def __init__(self, connection):
        self.connection = connection
        self.rowcount = 4

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        return False

    def execute(self, query, params):
        self.connection.executed_queries.append(query)
        self.connection.executed_params.append(params)


class FakeConnection:
    def __init__(self):
        self.executed_queries = []
        self.executed_params = []
        self.committed = False
        self.rolled_back = False

    def cursor(self):
        return FakeCursor(self)

    def commit(self):
        self.committed = True

    def rollback(self):
        self.rolled_back = True


def test_daily_analytics_executes_and_commits():
    connection = FakeConnection()

    result = generate_daily_analytics(
        connection,
        "2026-11-17"
    )

    assert result["type"] == "daily"
    assert result["rows_upserted"] == 4
    assert connection.committed is True
    assert len(connection.executed_queries) == 1
    assert connection.executed_params == [
        (date(2026, 11, 17), date(2026, 11, 17))
    ]


def test_bank_analytics_executes_and_commits():
    connection = FakeConnection()

    result = generate_bank_analytics(
        connection,
        "2026-11-17"
    )

    assert result["type"] == "bank"
    assert result["rows_upserted"] == 4
    assert connection.committed is True
    assert len(connection.executed_queries) == 1


def test_target_date_accepts_date_object():
    target = date(2026, 11, 17)

    assert _validate_target_date(target) == target


def test_target_date_rejects_invalid_string():
    with pytest.raises(ValueError):
        _validate_target_date("invalid-date")
