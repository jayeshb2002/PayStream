
from __future__ import annotations

import logging

from psycopg2.extensions import connection

logger = logging.getLogger(__name__)


FEATURE_SQL = """
WITH event_aggregates AS (
    SELECT
        transaction_id,

        MAX(payment_rail) AS payment_rail,
        MAX(amount_minor) AS amount_minor,
        MAX(currency) AS currency,

        MAX(sender_bank) AS sender_bank,
        MAX(receiver_bank) AS receiver_bank,

        MIN(event_time) AS first_event_time,
        MAX(event_time) AS last_event_time,

        COUNT(*)::INTEGER AS event_count,

        COUNT(DISTINCT event_type)::INTEGER
            AS distinct_event_types,

        COUNT(*) FILTER (
            WHERE UPPER(status) IN (
                'FAILED',
                'DECLINED',
                'REJECTED'
            )
        )::INTEGER AS failed_event_count

    FROM payment_events

    GROUP BY transaction_id
),

latest_events AS (
    SELECT DISTINCT ON (transaction_id)
        transaction_id,
        status AS latest_status

    FROM payment_events

    ORDER BY
        transaction_id,
        sequence_number DESC,
        event_time DESC,
        event_id DESC
),

features AS (
    SELECT
        a.transaction_id,
        a.payment_rail,
        a.amount_minor,
        a.currency,

        a.sender_bank,
        a.receiver_bank,

        a.first_event_time,
        a.last_event_time,

        l.latest_status,

        a.event_count,
        a.distinct_event_types,
        a.failed_event_count,

        GREATEST(
            0,
            EXTRACT(
                EPOCH FROM (
                    a.last_event_time - a.first_event_time
                )
            )::BIGINT
        ) AS lifecycle_duration_seconds

    FROM event_aggregates a

    JOIN latest_events l
        ON a.transaction_id = l.transaction_id
)

INSERT INTO transaction_features (
    transaction_id,
    payment_rail,
    amount_minor,
    currency,
    sender_bank,
    receiver_bank,
    first_event_time,
    last_event_time,
    latest_status,
    event_count,
    distinct_event_types,
    failed_event_count,
    lifecycle_duration_seconds,
    updated_at
)

SELECT
    transaction_id,
    payment_rail,
    amount_minor,
    currency,
    sender_bank,
    receiver_bank,
    first_event_time,
    last_event_time,
    latest_status,
    event_count,
    distinct_event_types,
    failed_event_count,
    lifecycle_duration_seconds,
    CURRENT_TIMESTAMP

FROM features

ON CONFLICT (transaction_id)
DO UPDATE SET

    payment_rail = EXCLUDED.payment_rail,
    amount_minor = EXCLUDED.amount_minor,
    currency = EXCLUDED.currency,

    sender_bank = EXCLUDED.sender_bank,
    receiver_bank = EXCLUDED.receiver_bank,

    first_event_time = EXCLUDED.first_event_time,
    last_event_time = EXCLUDED.last_event_time,

    latest_status = EXCLUDED.latest_status,

    event_count = EXCLUDED.event_count,
    distinct_event_types = EXCLUDED.distinct_event_types,
    failed_event_count = EXCLUDED.failed_event_count,

    lifecycle_duration_seconds =
        EXCLUDED.lifecycle_duration_seconds,

    updated_at = CURRENT_TIMESTAMP;
"""


def rebuild_transaction_features(
    connection: connection,
) -> int:
    """
    Rebuild transaction-level features from payment lifecycle events.

    Returns the total number of transaction feature records.
    """

    try:
        with connection.cursor() as cursor:
            cursor.execute(FEATURE_SQL)

            cursor.execute(
                "SELECT COUNT(*) FROM transaction_features"
            )

            result = cursor.fetchone()

            total = int(result[0])

        connection.commit()

        logger.info(
            "Transaction feature engineering completed. "
            "Total transactions: %s",
            total,
        )

        return total

    except Exception:
        connection.rollback()

        logger.exception(
            "Transaction feature engineering failed"
        )

        raise
