import pytest

from src.stream_processor import watermark_interval_sql


def test_minutes_watermark_to_sql_interval():
    assert watermark_interval_sql("10 minutes") == "INTERVAL 10 MINUTES"


def test_seconds_watermark_to_sql_interval():
    assert watermark_interval_sql("30 seconds") == "INTERVAL 30 SECONDS"


def test_rejects_invalid_watermark():
    with pytest.raises(ValueError):
        watermark_interval_sql("ten minutes")
