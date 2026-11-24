
"""PayStream infrastructure health checks."""

import logging
import os
import socket
from datetime import datetime, timezone

import psycopg2
from urllib.parse import urlparse

logger = logging.getLogger(__name__)


def check_kafka():
    """Verify Kafka broker connectivity through TCP."""

    host = os.getenv("KAFKA_HOST", "kafka")
    port = int(os.getenv("KAFKA_PORT", "9092"))

    start_time = datetime.now(timezone.utc)

    try:
        with socket.create_connection((host, port), timeout=5):
            latency_ms = (
                datetime.now(timezone.utc) - start_time
            ).total_seconds() * 1000

            result = {
                "service": "kafka",
                "status": "HEALTHY",
                "host": host,
                "port": port,
                "latency_ms": round(latency_ms, 2),
                "checked_at": datetime.now(timezone.utc).isoformat(),
            }

            logger.info("Kafka health check: %s", result)
            return result

    except Exception as exc:
        logger.exception("Kafka health check failed")

        raise RuntimeError(
            f"Kafka health check failed for {host}:{port}: {exc}"
        ) from exc


def check_paystream_postgres():
    """Verify PayStream PostgreSQL connectivity and query execution."""

    config = {
        "host": os.getenv("PAYSTREAM_DB_HOST", "postgres"),
        "port": int(os.getenv("PAYSTREAM_DB_PORT", "5432")),
        "dbname": os.environ["PAYSTREAM_DB_NAME"],
        "user": os.environ["PAYSTREAM_DB_USER"],
        "password": os.environ["PAYSTREAM_DB_PASSWORD"],
        "connect_timeout": 5,
    }

    start_time = datetime.now(timezone.utc)

    try:
        with psycopg2.connect(**config) as conn:
            with conn.cursor() as cursor:
                cursor.execute("SELECT 1")
                result = cursor.fetchone()

                if result[0] != 1:
                    raise RuntimeError("PostgreSQL SELECT 1 returned unexpected result")

        latency_ms = (
            datetime.now(timezone.utc) - start_time
        ).total_seconds() * 1000

        health = {
            "service": "paystream_postgres",
            "status": "HEALTHY",
            "database": config["dbname"],
            "latency_ms": round(latency_ms, 2),
            "checked_at": datetime.now(timezone.utc).isoformat(),
        }

        logger.info("PayStream PostgreSQL health check: %s", health)
        return health

    except Exception as exc:
        logger.exception("PayStream PostgreSQL health check failed")

        raise RuntimeError(
            f"PayStream PostgreSQL health check failed: {exc}"
        ) from exc


def check_airflow_metadata_database():
    """Verify Airflow metadata database and scheduler heartbeat."""

    connection_uri = os.environ.get(
        "AIRFLOW__DATABASE__SQL_ALCHEMY_CONN"
    )

    if not connection_uri:
        raise RuntimeError(
            "AIRFLOW__DATABASE__SQL_ALCHEMY_CONN is not configured"
        )

    # psycopg2 accepts the PostgreSQL URI, but not SQLAlchemy's
    # postgresql+psycopg2 dialect prefix.
    connection_uri = connection_uri.replace(
        "postgresql+psycopg2://",
        "postgresql://",
        1,
    )

    start_time = datetime.now(timezone.utc)

    try:
        with psycopg2.connect(
            connection_uri,
            connect_timeout=5,
        ) as conn:
            with conn.cursor() as cursor:
                cursor.execute("SELECT 1")
                query_result = cursor.fetchone()

                if query_result[0] != 1:
                    raise RuntimeError(
                        "Airflow metadata database query failed"
                    )

                # Airflow records scheduler heartbeats in the job table.
                cursor.execute("""
                    SELECT latest_heartbeat
                    FROM job
                    WHERE job_type = 'SchedulerJob'
                    ORDER BY latest_heartbeat DESC
                    LIMIT 1
                """)

                scheduler_row = cursor.fetchone()

                if scheduler_row is None:
                    raise RuntimeError(
                        "No scheduler heartbeat found in Airflow metadata DB"
                    )

                heartbeat = scheduler_row[0]

                if heartbeat.tzinfo is None:
                    heartbeat = heartbeat.replace(
                        tzinfo=timezone.utc
                    )

                heartbeat_age_seconds = (
                    datetime.now(timezone.utc) - heartbeat
                ).total_seconds()

                if heartbeat_age_seconds > 300:
                    raise RuntimeError(
                        "Airflow scheduler heartbeat is stale: "
                        f"{heartbeat_age_seconds:.1f} seconds"
                    )

        latency_ms = (
            datetime.now(timezone.utc) - start_time
        ).total_seconds() * 1000

        health = {
            "service": "airflow",
            "status": "HEALTHY",
            "scheduler_heartbeat_age_seconds": round(
                heartbeat_age_seconds, 2
            ),
            "latency_ms": round(latency_ms, 2),
            "checked_at": datetime.now(timezone.utc).isoformat(),
        }

        logger.info("Airflow health check: %s", health)
        return health

    except Exception as exc:
        logger.exception("Airflow metadata health check failed")

        raise RuntimeError(
            f"Airflow health check failed: {exc}"
        ) from exc
