
from datetime import date, datetime
from unittest.mock import MagicMock

import pytest

from src.orchestration.analytics import (
    validate_target_date,
    refresh_daily_analytics,
    refresh_bank_analytics
)


def make_connection():
    connection = MagicMock()
    cursor = MagicMock()

    connection.cursor.return_value.__enter__.return_value = cursor
    connection.cursor.return_value.__exit__.return_value = False

    cursor.rowcount = 3

    return connection, cursor


def test_validate_target_date_string():
    result = validate_target_date("2026-11-16")

    assert result == date(2026, 11, 16)


def test_validate_target_date_object():
    result = validate_target_date(date(2026, 11, 16))

    assert result == date(2026, 11, 16)


def test_validate_target_date_invalid():
    with pytest.raises(ValueError):
        validate_target_date("16-11-2026")


def test_validate_target_date_rejects_datetime():
    with pytest.raises(ValueError):
        validate_target_date(datetime(2026, 11, 16))


def test_daily_analytics_success():
    connection, cursor = make_connection()

    result = refresh_daily_analytics(
        connection,
        "2026-11-16"
    )

    assert result == {
        "analytics_date": "2026-11-16",
        "daily_rows": 3
    }

    assert cursor.execute.call_count == 2
    connection.commit.assert_called_once()
    connection.rollback.assert_not_called()


def test_bank_analytics_success():
    connection, cursor = make_connection()

    result = refresh_bank_analytics(
        connection,
        "2026-11-16"
    )

    assert result == {
        "analytics_date": "2026-11-16",
        "bank_rows": 3
    }

    assert cursor.execute.call_count == 2
    connection.commit.assert_called_once()


def test_daily_analytics_rollback_on_failure():
    connection, cursor = make_connection()

    cursor.execute.side_effect = RuntimeError("Database error")

    with pytest.raises(RuntimeError):
        refresh_daily_analytics(
            connection,
            "2026-11-16"
        )

    connection.rollback.assert_called_once()
    connection.commit.assert_not_called()
