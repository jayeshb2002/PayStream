
from datetime import date


DAILY_ANALYTICS_SQL = """
    INSERT INTO daily_payment_analytics_snapshot (
        analytics_date,
        payment_rail,
        total_transactions,
        successful_transactions,
        failed_transactions,
        pending_transactions,
        total_amount_minor,
        successful_amount_minor,
        success_rate,
        updated_at
    )
    SELECT
        %s::date,
        payment_rail,
        COUNT(*),
        COUNT(*) FILTER (WHERE status = 'SUCCESS'),
        COUNT(*) FILTER (WHERE status = 'FAILED'),
        COUNT(*) FILTER (
            WHERE status NOT IN ('SUCCESS', 'FAILED')
        ),
        COALESCE(SUM(amount_minor), 0),
        COALESCE(
            SUM(amount_minor) FILTER (WHERE status = 'SUCCESS'),
            0
        ),
        ROUND(
            100.0 * COUNT(*) FILTER (WHERE status = 'SUCCESS')
            / NULLIF(COUNT(*), 0),
            2
        ),
        CURRENT_TIMESTAMP
    FROM payment_events
    WHERE (event_time AT TIME ZONE 'UTC')::date = %s::date
    GROUP BY payment_rail

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


BANK_ANALYTICS_SQL = """
    INSERT INTO bank_payment_analytics_snapshot (
        analytics_date,
        sender_bank,
        payment_rail,
        total_transactions,
        successful_transactions,
        failed_transactions,
        total_amount_minor,
        successful_amount_minor,
        success_rate,
        updated_at
    )
    SELECT
        %s::date,
        sender_bank,
        payment_rail,
        COUNT(*),
        COUNT(*) FILTER (WHERE status = 'SUCCESS'),
        COUNT(*) FILTER (WHERE status = 'FAILED'),
        COALESCE(SUM(amount_minor), 0),
        COALESCE(
            SUM(amount_minor) FILTER (WHERE status = 'SUCCESS'),
            0
        ),
        ROUND(
            100.0 * COUNT(*) FILTER (WHERE status = 'SUCCESS')
            / NULLIF(COUNT(*), 0),
            2
        ),
        CURRENT_TIMESTAMP
    FROM payment_events
    WHERE (event_time AT TIME ZONE 'UTC')::date = %s::date
    GROUP BY sender_bank, payment_rail

    ON CONFLICT (analytics_date, sender_bank, payment_rail)
    DO UPDATE SET
        total_transactions = EXCLUDED.total_transactions,
        successful_transactions = EXCLUDED.successful_transactions,
        failed_transactions = EXCLUDED.failed_transactions,
        total_amount_minor = EXCLUDED.total_amount_minor,
        successful_amount_minor = EXCLUDED.successful_amount_minor,
        success_rate = EXCLUDED.success_rate,
        updated_at = CURRENT_TIMESTAMP
"""


def _validate_target_date(target_date):
    if isinstance(target_date, str):
        target_date = date.fromisoformat(target_date)

    if not isinstance(target_date, date):
        raise ValueError("target_date must be a date or YYYY-MM-DD string")

    return target_date


def generate_daily_analytics(connection, target_date):
    target_date = _validate_target_date(target_date)

    try:
        with connection.cursor() as cursor:
            cursor.execute(
                DAILY_ANALYTICS_SQL,
                (target_date, target_date)
            )
            affected_rows = cursor.rowcount

        connection.commit()

        return {
            "analytics_date": target_date.isoformat(),
            "type": "daily",
            "rows_upserted": affected_rows
        }

    except Exception:
        connection.rollback()
        raise


def generate_bank_analytics(connection, target_date):
    target_date = _validate_target_date(target_date)

    try:
        with connection.cursor() as cursor:
            cursor.execute(
                BANK_ANALYTICS_SQL,
                (target_date, target_date)
            )
            affected_rows = cursor.rowcount

        connection.commit()

        return {
            "analytics_date": target_date.isoformat(),
            "type": "bank",
            "rows_upserted": affected_rows
        }

    except Exception:
        connection.rollback()
        raise
