import os
import psycopg2


def check_paystream_database():

    required = [
        "PAYSTREAM_DB_HOST",
        "PAYSTREAM_DB_NAME",
        "PAYSTREAM_DB_USER",
        "PAYSTREAM_DB_PASSWORD"
    ]

    missing = [key for key in required if not os.getenv(key)]

    if missing:
        raise RuntimeError(
            f"Missing environment variables: {missing}"
        )

    conn = psycopg2.connect(
        host=os.environ["PAYSTREAM_DB_HOST"],
        port=int(os.getenv("PAYSTREAM_DB_PORT", "5432")),
        dbname=os.environ["PAYSTREAM_DB_NAME"],
        user=os.environ["PAYSTREAM_DB_USER"],
        password=os.environ["PAYSTREAM_DB_PASSWORD"],
        connect_timeout=5
    )

    try:
        with conn.cursor() as cursor:

            cursor.execute("""
                SELECT
                    current_database(),
                    current_user,
                    CURRENT_TIMESTAMP
            """)

            result = cursor.fetchone()

        return {
            "database": result[0],
            "user": result[1],
            "server_time": result[2].isoformat()
        }

    finally:
        conn.close()