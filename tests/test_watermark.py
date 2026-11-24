import pytest

from src.stream_processor import instrument_batch, watermark_interval_sql


def test_minutes_watermark_to_sql_interval():
    assert watermark_interval_sql("10 minutes") == "INTERVAL 10 MINUTES"


def test_seconds_watermark_to_sql_interval():
    assert watermark_interval_sql("30 seconds") == "INTERVAL 30 SECONDS"


def test_rejects_invalid_watermark():
    with pytest.raises(ValueError):
        watermark_interval_sql("ten minutes")


def test_instrument_batch_forwards_callback_arguments(monkeypatch):
    calls = []

    class FakeBatch:
        def count(self):
            return 3

    def callback(batch_df, batch_id, topic, bootstrap_servers):
        calls.append((batch_df, batch_id, topic, bootstrap_servers))

    recorded = {}

    def record_metrics(**kwargs):
        recorded.update(kwargs)

    monkeypatch.setattr(
        "src.stream_processor.record_batch_metrics_safely",
        record_metrics,
    )

    wrapped = instrument_batch(
        callback,
        "records_rejected",
        "payment-events-dlq",
        "localhost:29092",
    )
    batch = FakeBatch()
    wrapped(batch, 7)

    assert calls == [(batch, 7, "payment-events-dlq", "localhost:29092")]
    assert recorded["metrics"] == {"records_rejected": 3}
