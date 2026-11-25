
from __future__ import annotations

import logging
import os

import psycopg2

from dotenv import load_dotenv

from src.orchestration.feature_engineering import (
    rebuild_transaction_features,
)

from src.orchestration.risk_engine import (
    run_risk_analysis,
)


logger = logging.getLogger(__name__)


def get_database_connection():
    load_dotenv()

    return psycopg2.connect(
        host=os.getenv("PAYSTREAM_DB_HOST", "localhost"),
        port=int(os.getenv("PAYSTREAM_DB_PORT", "5433")),
        dbname=os.getenv("PAYSTREAM_DB_NAME", "paystream"),
        user=os.getenv("PAYSTREAM_DB_USER", "paystream"),
        password=os.getenv("PAYSTREAM_DB_PASSWORD"),
        connect_timeout=10,
    )


def run_risk_pipeline():
    connection = get_database_connection()

    try:
        total_transactions = rebuild_transaction_features(
            connection
        )

        logger.info(
            "Feature engineering completed: %s transactions",
            total_transactions,
        )

        report = run_risk_analysis(connection)

        logger.info(
            "Risk pipeline completed: %s",
            report,
        )

        return report

    finally:
        connection.close()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    result = run_risk_pipeline()

    print(result)
