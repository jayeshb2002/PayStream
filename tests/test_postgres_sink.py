from argparse import Namespace

import pytest

pytest.importorskip("pyspark")
pytest.importorskip("psycopg2")

from src.postgres_sink import DELETE_STAGING_SQL, MERGE_SQL, jdbc_url, psycopg_config


def sample_args():
    return Namespace(
        db_host="localhost",
        db_port=5432,
        db_name="paystream",
        db_user="paystream",
        db_password="test-password",
    )


def test_jdbc_url_uses_configured_postgres_connection():
    assert jdbc_url(sample_args()) == "jdbc:postgresql://localhost:5432/paystream"


def test_psycopg_config_contains_connection_fields():
    config = psycopg_config(sample_args())
    assert config["host"] == "localhost"
    assert config["port"] == 5432
    assert config["dbname"] == "paystream"
    assert config["user"] == "paystream"
    assert config["password"] == "test-password"


def test_merge_is_idempotent_on_event_id():
    assert "ON CONFLICT (event_id) DO NOTHING" in MERGE_SQL
    assert "DISTINCT ON (event_id)" in MERGE_SQL
    assert "WHERE spark_batch_id = %s" in MERGE_SQL
    assert "DELETE FROM payment_events_staging WHERE spark_batch_id = %s" == DELETE_STAGING_SQL
