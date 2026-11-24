
import time


def get_data_quality_checks():
    """Return all SQL-based payment data quality checks."""

    return {
        "null_or_empty_event_ids": """
            SELECT COUNT(*)
            FROM payment_events
            WHERE event_id IS NULL OR BTRIM(event_id) = ''
        """,

        "null_or_empty_transaction_ids": """
            SELECT COUNT(*)
            FROM payment_events
            WHERE transaction_id IS NULL
               OR BTRIM(transaction_id) = ''
        """,

        "invalid_event_types": """
            SELECT COUNT(*)
            FROM payment_events
            WHERE event_type IS NULL
               OR event_type NOT IN (
                   'PAYMENT_INITIATED',
                   'PAYMENT_STATUS_UPDATED',
                   'PAYMENT_COMPLETED',
                   'PAYMENT_FAILED'
               )
        """,

        "invalid_payment_rails": """
            SELECT COUNT(*)
            FROM payment_events
            WHERE payment_rail IS NULL
               OR payment_rail NOT IN ('UPI', 'IMPS', 'RUPAY', 'NEFT')
        """,

        "invalid_statuses": """
            SELECT COUNT(*)
            FROM payment_events
            WHERE status IS NULL
               OR status NOT IN (
                   'INITIATED',
                   'PROCESSING',
                   'SUCCESS',
                   'FAILED'
               )
        """,

        "invalid_amounts": """
            SELECT COUNT(*)
            FROM payment_events
            WHERE amount_minor IS NULL OR amount_minor <= 0
        """,

        "invalid_currency": """
            SELECT COUNT(*)
            FROM payment_events
            WHERE currency IS NULL OR currency <> 'INR'
        """,

        "invalid_bank_names": """
            SELECT COUNT(*)
            FROM payment_events
            WHERE sender_bank IS NULL
               OR receiver_bank IS NULL
               OR LENGTH(BTRIM(sender_bank)) NOT BETWEEN 2 AND 32
               OR LENGTH(BTRIM(receiver_bank)) NOT BETWEEN 2 AND 32
        """,

        "invalid_producer_ids": """
            SELECT COUNT(*)
            FROM payment_events
            WHERE producer_id IS NULL OR BTRIM(producer_id) = ''
        """,

        "invalid_sequence_numbers": """
            SELECT COUNT(*)
            FROM payment_events
            WHERE sequence_number IS NULL OR sequence_number < 1
        """,

        "invalid_schema_versions": """
            SELECT COUNT(*)
            FROM payment_events
            WHERE schema_version IS NULL OR schema_version < 1
        """,

        "null_timestamps": """
            SELECT COUNT(*)
            FROM payment_events
            WHERE event_time IS NULL OR ingestion_time IS NULL
        """,

        "duplicate_event_ids": """
            SELECT COUNT(*)
            FROM (
                SELECT event_id
                FROM payment_events
                GROUP BY event_id
                HAVING COUNT(*) > 1
            ) duplicates
        """
    }


def run_data_quality_checks(connection):
    """
    Run all SQL data quality checks.

    Returns a structured report containing:
    - Total records
    - Individual check results
    - Passed and failed check counts
    - Execution duration
    """

    started_at = time.perf_counter()
    checks = get_data_quality_checks()

    check_results = {}

    with connection.cursor() as cursor:
        cursor.execute("SELECT COUNT(*) FROM payment_events")
        total_records = cursor.fetchone()[0]

        for check_name, query in checks.items():
            cursor.execute(query)
            failed_records = cursor.fetchone()[0]

            check_results[check_name] = {
                "failed_records": failed_records,
                "status": "FAILED" if failed_records > 0 else "PASSED"
            }

    failed_checks = {
        name: result["failed_records"]
        for name, result in check_results.items()
        if result["status"] == "FAILED"
    }

    total_checks = len(checks)
    passed_checks = total_checks - len(failed_checks)

    execution_duration_ms = int(
        (time.perf_counter() - started_at) * 1000
    )

    return {
        "total_records": total_records,
        "total_checks": total_checks,
        "passed_checks": passed_checks,
        "failed_checks_count": len(failed_checks),
        "failed_checks": failed_checks,
        "check_results": check_results,
        "execution_duration_ms": execution_duration_ms,
        "status": "FAILED" if failed_checks else "PASSED"
    }
