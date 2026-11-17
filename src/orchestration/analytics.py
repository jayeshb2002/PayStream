
from datetime import date, datetime

DAILY_DELETE_SQL = """
    DELETE FROM daily_payment_analytics_snapshot
    WHERE analytics_date = %s
"""

BANK_DELETE_SQL = """
    DELETE FROM bank_payment_analytics_snapshot
    WHERE analytics_date = %s
"""

# Each transaction may have multiple lifecycle events.
# Keep its latest status and its earliest event timestamp.
TRANSACTION_CTE = """
    WITH ranked_events AS (
        SELECT
            transaction_id,
            event_id,
            payment_rail,
            sender_bank,
            status,
            amount_minor,
            event_time,

            MIN(event_time) OVER (
                PARTITION BY transaction_id
            ) AS initiated_at,

            ROW_NUMBER() OVER (
                PARTITION BY transaction_id
                ORDER BY
                    event_time DESC,
                    sequence_number DESC,
                    event_id DESC
            ) AS row_num

        FROM payment_events
    ),

    final_transactions AS (
        SELECT
            transaction_id,
            payment_rail,
            sender_bank,
            status,
            amount_minor,
            initiated_at

        FROM ranked_events

        WHERE row_num = 1
          AND initiated_at >= (
              %s::date::timestamp AT TIME ZONE 'UTC'
          )
          AND initiated_at < (
              (%s::date + INTERVAL '1 day')
              ::timestamp AT TIME ZONE 'UTC'
          )
    )
"""

DAILY_INSERT_SQL = TRANSACTION_CTE + """
    INSERT INTO daily_payment_analytics_snapshot (
        analytics_date,
        payment_rail,
        total_transactions,
        successful_transactions,
        failed_transactions,
        pending_transactions,
        total_amount_minor,
        successful_amount_minor,
        success_rate
    )

    SELECT
        (initiated_at AT TIME ZONE 'UTC')::date,
        payment_rail,

        COUNT(*),

        COUNT(*) FILTER (
            WHERE status = 'SUCCESS'
        ),

        COUNT(*) FILTER (
            WHERE status = 'FAILED'
        ),

        COUNT(*) FILTER (
            WHERE status NOT IN ('SUCCESS', 'FAILED')
        ),

        COALESCE(SUM(amount_minor), 0),

        COALESCE(
            SUM(amount_minor) FILTER (
                WHERE status = 'SUCCESS'
            ),
            0
        ),

        ROUND(
            100.0 * COUNT(*) FILTER (
                WHERE status = 'SUCCESS'
            ) / NULLIF(COUNT(*), 0),
            2
        )

    FROM final_transactions

    GROUP BY
        (initiated_at AT TIME ZONE 'UTC')::date,
        payment_rail

    ON CONFLICT (analytics_date, payment_rail)
    DO UPDATE SET
        total_transactions = EXCLUDED.total_transactions,
        successful_transactions = EXCLUDED.successful_transactions,
        failed_transactions = EXCLUDED.failed_transactions,
        pending_transactions = EXCLUDED.pending_transactions,
        total_amount_minor = EXCLUDED.total_amount_minor,
        successful_amount_minor = EXCLUDED.successful_amount_minor,
        success_rate = EXCLUDED.success_rate,
        updated_at = CURRENT_TIMESTAMP
"""

BANK_INSERT_SQL = TRANSACTION_CTE + """
    INSERT INTO bank_payment_analytics_snapshot (
        analytics_date,
        sender_bank,
        payment_rail,
        total_transactions,
        successful_transactions,
        failed_transactions,
        total_amount_minor,
        successful_amount_minor,
        success_rate
    )

    SELECT
        (initiated_at AT TIME ZONE 'UTC')::date,
        sender_bank,
        payment_rail,

        COUNT(*),

        COUNT(*) FILTER (
            WHERE status = 'SUCCESS'
        ),

        COUNT(*) FILTER (
            WHERE status = 'FAILED'
        ),

        COALESCE(SUM(amount_minor), 0),

        COALESCE(
            SUM(amount_minor) FILTER (
                WHERE status = 'SUCCESS'
            ),
            0
        ),

        ROUND(
            100.0 * COUNT(*) FILTER (
                WHERE status = 'SUCCESS'
            ) / NULLIF(COUNT(*), 0),
            2
        )

    FROM final_transactions

    GROUP BY
        (initiated_at AT TIME ZONE 'UTC')::date,
        sender_bank,
        payment_rail

    ON CONFLICT (
        analytics_date,
        sender_bank,
        payment_rail
    )
    DO UPDATE SET
        total_transactions = EXCLUDED.total_transactions,
        successful_transactions = EXCLUDED.successful_transactions,
        failed_transactions = EXCLUDED.failed_transactions,
        total_amount_minor = EXCLUDED.total_amount_minor,
        successful_amount_minor = EXCLUDED.successful_amount_minor,
        success_rate = EXCLUDED.success_rate,
        updated_at = CURRENT_TIMESTAMP
"""


def validate_target_date(target_date):
    if isinstance(target_date, datetime):
        raise ValueError("target_date must be a date, not datetime")

    if isinstance(target_date, date):
        return target_date

    if isinstance(target_date, str):
        try:
            return date.fromisoformat(target_date)
        except ValueError as exc:
            raise ValueError(
                "target_date must use YYYY-MM-DD format"
            ) from exc

    raise ValueError(
        "target_date must be a date or YYYY-MM-DD string"
    )


def refresh_daily_analytics(connection, target_date):
    target_date = validate_target_date(target_date)

    try:
        with connection.cursor() as cursor:
            cursor.execute(DAILY_DELETE_SQL, (target_date,))

            cursor.execute(
                DAILY_INSERT_SQL,
                (target_date, target_date)
            )

            inserted_rows = cursor.rowcount

        connection.commit()

        return {
            "analytics_date": target_date.isoformat(),
            "daily_rows": inserted_rows
        }

    except Exception:
        connection.rollback()
        raise


def refresh_bank_analytics(connection, target_date):
    target_date = validate_target_date(target_date)

    try:
        with connection.cursor() as cursor:
            cursor.execute(BANK_DELETE_SQL, (target_date,))

            cursor.execute(
                BANK_INSERT_SQL,
                (target_date, target_date)
            )

            inserted_rows = cursor.rowcount

        connection.commit()

        return {
            "analytics_date": target_date.isoformat(),
            "bank_rows": inserted_rows
        }

    except Exception:
        connection.rollback()
        raise
