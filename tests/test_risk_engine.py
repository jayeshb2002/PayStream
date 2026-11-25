
from src.orchestration.risk_engine import (
    build_rail_baselines,
    calculate_risk,
    percentile,
    run_risk_analysis,
)


def make_feature(**overrides):
    feature = {
        "transaction_id": "TXN001",
        "payment_rail": "IMPS",
        "amount_minor": 100000,
        "currency": "INR",
        "sender_bank": "BANK_A",
        "receiver_bank": "BANK_B",
        "latest_status": "COMPLETED",
        "event_count": 3,
        "distinct_event_types": 3,
        "failed_event_count": 0,
        "lifecycle_duration_seconds": 120,
    }

    feature.update(overrides)

    return feature


def test_percentile():
    assert percentile([10, 20, 30, 40, 50], 0.5) == 30


def test_build_rail_baselines():
    features = [
        make_feature(amount_minor=100),
        make_feature(amount_minor=200),
        make_feature(amount_minor=300),
    ]

    baselines = build_rail_baselines(features)

    assert "IMPS" in baselines
    assert baselines["IMPS"]["median"] == 200
    assert baselines["IMPS"]["mad"] == 100


def test_normal_transaction_has_low_risk():
    feature = make_feature()

    baseline = {
        "median": 100000,
        "mad": 10000,
        "p95": 200000,
    }

    result = calculate_risk(feature, baseline)

    assert result["risk_score"] == 0
    assert result["risk_category"] == "LOW"
    assert result["is_anomaly"] is False


def test_multiple_failed_events_increase_risk():
    feature = make_feature(
        failed_event_count=2
    )

    baseline = {
        "median": 100000,
        "mad": 10000,
        "p95": 200000,
    }

    result = calculate_risk(feature, baseline)

    assert "MULTIPLE_FAILED_EVENTS" in result["risk_flags"]
    assert result["risk_score"] >= 25


def test_failed_latest_status_increases_risk():
    feature = make_feature(
        latest_status="FAILED"
    )

    baseline = {
        "median": 100000,
        "mad": 10000,
        "p95": 200000,
    }

    result = calculate_risk(feature, baseline)

    assert "LATEST_STATUS_FAILED" in result["risk_flags"]


def test_long_processing_duration_increases_risk():
    feature = make_feature(
        lifecycle_duration_seconds=3600
    )

    baseline = {
        "median": 100000,
        "mad": 10000,
        "p95": 200000,
    }

    result = calculate_risk(feature, baseline)

    assert "LONG_PROCESSING_DURATION" in result["risk_flags"]


def test_high_amount_is_flagged():
    feature = make_feature(
        amount_minor=1000000
    )

    baseline = {
        "median": 100000,
        "mad": 10000,
        "p95": 200000,
    }

    result = calculate_risk(feature, baseline)

    assert "AMOUNT_ABOVE_RAIL_P95" in result["risk_flags"]
    assert result["is_anomaly"] is True


def test_risk_score_is_capped_at_100():
    feature = make_feature(
        amount_minor=1000000,
        failed_event_count=3,
        latest_status="FAILED",
        event_count=8,
        lifecycle_duration_seconds=5000,
    )

    baseline = {
        "median": 100000,
        "mad": 10000,
        "p95": 200000,
    }

    result = calculate_risk(feature, baseline)

    assert result["risk_score"] <= 100
    assert result["risk_category"] == "CRITICAL"


def test_risk_analysis_persists_run_id_as_string(monkeypatch):
    class FakeCursor:
        def __init__(self, executed):
            self.executed = executed

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc_value, traceback):
            return False

        def execute(self, query, parameters):
            self.executed.append(parameters)

    class FakeConnection:
        def __init__(self):
            self.executed = []

        def cursor(self):
            return FakeCursor(self.executed)

        def commit(self):
            pass

        def rollback(self):
            pass

    connection = FakeConnection()
    monkeypatch.setattr(
        "src.orchestration.risk_engine.load_features",
        lambda connection: [],
    )

    report = run_risk_analysis(connection)

    assert report["status"] == "COMPLETED"
    assert report["run_id"] == connection.executed[0][0]
    assert isinstance(connection.executed[0][0], str)
    assert connection.executed[1][-1] == connection.executed[0][0]
