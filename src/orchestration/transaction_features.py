
"""Transaction-level feature engineering for PayStream."""

from __future__ import annotations

import argparse
import logging
import os
from typing import Any

import psycopg2
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)

FEATURE_ENGINEERING_SQL = """
WITH ranked_events AS (
    SELECT
        event_id,
        transaction_id,
        event_type,
        payment_rail,
        amount_minor,
        currency,
        sender_bank,
        receiver_bank,
        status,
        event_time,
        sequence_number,

        ROW_NUMBER() OVER (
            PARTITION BY transaction_id
            ORDER BY
                event_time DESC,
                sequence_number DESC,
                event_id DESC
        ) AS event_rank

    FROM payment_events
),

transaction_aggregates AS (
    SELECT
        transaction_id,

        COUNT(*)::INTEGER AS event_count,

        COUNT(DISTINCT event_type)::INTEGER
            AS distinct_event_types,

        COUNT(*) FILTER (
            WHERE UPPER(status) IN ('FAILED', 'FAILURE')
               OR UPPER(event_type) = 'PAYMENT_FAILED'
        )::INTEGER AS failed_event_count,

        MIN(event_time) AS first_event_time,
        MAX(event_time) AS last_event_time

    FROM payment_events
    GROUP BY transaction_id
),

latest_events AS (
    SELECT
        transaction_id,
        payment_rail,
        currency,
        amount_minor,
        sender_bank,
        receiver_bank,
        status AS latest_status

    FROM ranked_events
    WHERE event_rank = 1
),

features AS (
    SELECT
        a.transaction_id,

        l.payment_rail,
        l.currency,
        l.amount_minor,

        l.sender_bank,
        l.receiver_bank,

        a.event_count,
        a.distinct_event_types,
        a.failed_event_count,

        l.latest_status,

        a.first_event_time,
        a.last_event_time,

        GREATEST(
            EXTRACT(
                EPOCH FROM (a.last_event_time - a.first_event_time)
            ),
            0
        ) AS lifecycle_duration_seconds

    FROM transaction_aggregates a

    JOIN latest_events l
        ON a.transaction_id = l.transaction_id
)

INSERT INTO transaction_features (
    transaction_id,
    payment_rail,
    currency,
    amount_minor,
    sender_bank,
    receiver_bank,
    event_count,
    distinct_event_types,
    failed_event_count,
    latest_status,
    first_event_time,
    last_event_time,
    lifecycle_duration_seconds,
    updated_at
)

SELECT
    transaction_id,
    payment_rail,
    currency,
    amount_minor,
    sender_bank,
    receiver_bank,
    event_count,
    distinct_event_types,
    failed_event_count,
    latest_status,
    first_event_time,
    last_event_time,
    lifecycle_duration_seconds::BIGINT,
    CURRENT_TIMESTAMP

FROM features

ON CONFLICT (transaction_id)
DO UPDATE SET

    payment_rail = EXCLUDED.payment_rail,
    currency = EXCLUDED.currency,
    amount_minor = EXCLUDED.amount_minor,

    sender_bank = EXCLUDED.sender_bank,
    receiver_bank = EXCLUDED.receiver_bank,

    event_count = EXCLUDED.event_count,
    distinct_event_types = EXCLUDED.distinct_event_types,
    failed_event_count = EXCLUDED.failed_event_count,

    latest_status = EXCLUDED.latest_status,

    first_event_time = EXCLUDED.first_event_time,
    last_event_time = EXCLUDED.last_event_time,

    lifecycle_duration_seconds =
        EXCLUDED.lifecycle_duration_seconds,

    updated_at = CURRENT_TIMESTAMP;
"""


def get_database_connection():
    """Create a PostgreSQL connection using PayStream environment variables."""

    return psycopg2.connect(
        host=os.getenv("PAYSTREAM_DB_HOST", "localhost"),
        port=int(os.getenv("PAYSTREAM_DB_PORT", "5433")),
        dbname=os.getenv("PAYSTREAM_DB_NAME", "paystream"),
        user=os.getenv("PAYSTREAM_DB_USER", "paystream"),
        password=os.getenv("PAYSTREAM_DB_PASSWORD", "password"),
        connect_timeout=10,
    )


def run_transaction_feature_engineering(connection) -> dict[str, Any]:
    """
    Aggregate payment lifecycle events into transaction-level features.

    The operation is idempotent because transaction_id is the primary key
    and the query uses PostgreSQL ON CONFLICT DO UPDATE.
    """

    cursor = connection.cursor()

    try:
        cursor.execute(FEATURE_ENGINEERING_SQL)

        affected_rows = cursor.rowcount

        connection.commit()

        logger.info(
            "Transaction feature engineering completed. "
            "Rows inserted or updated: %s",
            affected_rows,
        )

        return {
            "status": "SUCCESS",
            "rows_inserted_or_updated": affected_rows,
        }

    except Exception:
        connection.rollback()

        logger.exception(
            "Transaction feature engineering failed"
        )

        raise

    finally:
        cursor.close()


def run_pipeline() -> dict[str, Any]:
    """Create connection, execute feature engineering and close connection."""

    connection = None

    try:
        connection = get_database_connection()

        return run_transaction_feature_engineering(connection)

    finally:
        if connection is not None:
            connection.close()


def main():
    parser = argparse.ArgumentParser(
        description="PayStream transaction feature engineering"
    )

    parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
    )

    result = run_pipeline()

    print("\nTransaction Feature Engineering Result")
    print("--------------------------------------")
    print(f"Status: {result['status']}")
    print(
        "Rows inserted or updated: "
        f"{result['rows_inserted_or_updated']}"
    )


if __name__ == "__main__":
    main()
