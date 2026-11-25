
import pytest

from src.orchestration.risk_scoring import (
    TRANSACTION_RISK_SQL,
    calculate_transaction_risk,
    get_risk_category,
    score_transactions,
)


def make_transaction(**overrides):
    transaction = {
        "transaction_id": "TXN-001",
        "latest_status": "SUCCESS",
        "failed_event_count": 0,
        "event_count": 3,
        "lifecycle_duration_seconds": 60,
        "is_robust_z_anomaly": False,
        "is_percentile_anomaly": False,
    }

    transaction.update(overrides)

    return transaction


def test_normal_transaction_has_low_risk():
    result = calculate_transaction_risk(
        make_transaction()
    )

    assert result["risk_score"] == 0
    assert result["risk_category"] == "LOW"
    assert result["is_flagged"] is False
    assert result["risk_flags"] == []


def test_failed_transaction_adds_35_points():
    result = calculate_transaction_risk(
        make_transaction(latest_status="FAILED")
    )

    assert result["risk_score"] == 35
    assert result["risk_category"] == "MEDIUM"


def test_multiple_failed_events_add_20_points():
    result = calculate_transaction_risk(
        make_transaction(failed_event_count=2)
    )

    assert result["risk_score"] == 20
    assert result["risk_category"] == "MEDIUM"


def test_robust_z_anomaly_adds_25_points():
    result = calculate_transaction_risk(
        make_transaction(is_robust_z_anomaly=True)
    )

    assert result["risk_score"] == 25
    assert result["risk_category"] == "MEDIUM"


def test_percentile_anomaly_adds_15_points():
    result = calculate_transaction_risk(
        make_transaction(is_percentile_anomaly=True)
    )

    assert result["risk_score"] == 15
    assert result["risk_category"] == "LOW"


def test_excessive_lifecycle_events_add_10_points():
    result = calculate_transaction_risk(
        make_transaction(event_count=6)
    )

    assert result["risk_score"] == 10


def test_long_lifecycle_adds_10_points():
    result = calculate_transaction_risk(
        make_transaction(lifecycle_duration_seconds=400)
    )

    assert result["risk_score"] == 10


def test_combined_risk_score_is_capped_at_100():
    result = calculate_transaction_risk(
        make_transaction(
            latest_status="FAILED",
            failed_event_count=3,
            is_robust_z_anomaly=True,
            is_percentile_anomaly=True,
            event_count=8,
            lifecycle_duration_seconds=600,
        )
    )

    assert result["risk_score"] == 100
    assert result["risk_category"] == "CRITICAL"


@pytest.mark.parametrize(
    "score,expected",
    [
        (0, "LOW"),
        (19, "LOW"),
        (20, "MEDIUM"),
        (39, "MEDIUM"),
        (40, "HIGH"),
        (69, "HIGH"),
        (70, "CRITICAL"),
        (100, "CRITICAL"),
    ],
)
def test_risk_category_boundaries(score, expected):
    assert get_risk_category(score) == expected


def test_invalid_risk_score():
    with pytest.raises(ValueError):
        get_risk_category(101)


def test_risk_explanations_are_generated():
    result = calculate_transaction_risk(
        make_transaction(
            latest_status="FAILED",
            is_robust_z_anomaly=True,
        )
    )

    assert result["risk_flag_count"] == 2
    assert len(result["risk_flags"]) == 2

    assert all(
        flag["description"]
        for flag in result["risk_flags"]
    )


def test_multiple_transactions_are_scored():
    transactions = [
        make_transaction(transaction_id="TXN-001"),
        make_transaction(
            transaction_id="TXN-002",
            latest_status="FAILED",
        ),
    ]

    results = score_transactions(transactions)

    assert len(results) == 2
    assert results[0]["risk_category"] == "LOW"
    assert results[1]["risk_category"] == "MEDIUM"


def test_risk_query_uses_existing_feature_columns():
    assert "f.distinct_event_types" in TRANSACTION_RISK_SQL
    assert "f.distinct_event_type_count" not in TRANSACTION_RISK_SQL
    assert "f.latest_event_type" not in TRANSACTION_RISK_SQL
