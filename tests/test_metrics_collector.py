
import pytest

from src.orchestration import metrics_collector as collector


def test_empty_component_raises():
    with pytest.raises(ValueError):
        collector.record_batch_metrics(
            "",
            0,
            {"records_processed": 10},
        )


def test_negative_batch_id_raises():
    with pytest.raises(ValueError):
        collector.record_batch_metrics(
            "spark_processor",
            -1,
            {"records_processed": 10},
        )


def test_negative_metric_raises():
    with pytest.raises(ValueError):
        collector.record_batch_metrics(
            "spark_processor",
            0,
            {"records_processed": -10},
        )


def test_non_integer_metric_raises():
    with pytest.raises(ValueError):
        collector.record_batch_metrics(
            "spark_processor",
            0,
            {"records_processed": 10.5},
        )


def test_negative_duration_raises():
    with pytest.raises(ValueError):
        collector.record_batch_metrics(
            "spark_processor",
            0,
            {"records_processed": 10},
            processing_duration_ms=-1,
        )


def test_empty_metrics_does_not_connect(monkeypatch):

    def unexpected_connection():
        raise AssertionError("Database connection should not open")

    monkeypatch.setattr(
        collector,
        "get_metrics_connection",
        unexpected_connection,
    )

    collector.record_batch_metrics(
        "spark_processor",
        0,
        {},
    )


def test_safe_writer_does_not_raise(monkeypatch):

    def failing_writer(**kwargs):
        raise RuntimeError("Simulated database failure")

    monkeypatch.setattr(
        collector,
        "record_batch_metrics",
        failing_writer,
    )

    collector.record_batch_metrics_safely(
        "spark_processor",
        0,
        {"records_processed": 10},
    )
