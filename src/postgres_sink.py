"""PayStream Milestone 3: Kafka -> Spark Structured Streaming -> PostgreSQL.

Run with spark-submit from the PayStream repository root. This application
consumes the clean topic produced by Milestone 2 and persists events using a
staging-table + PostgreSQL ON CONFLICT merge pattern.
"""

from __future__ import annotations

import argparse
import logging
import os
from typing import Any

import psycopg2
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

LOG = logging.getLogger("paystream.postgres_sink")

EVENT_SCHEMA = StructType(
    [
        StructField("event_id", StringType(), True),
        StructField("transaction_id", StringType(), True),
        StructField("event_type", StringType(), True),
        StructField("payment_rail", StringType(), True),
        StructField("amount_minor", LongType(), True),
        StructField("currency", StringType(), True),
        StructField("sender_bank", StringType(), True),
        StructField("receiver_bank", StringType(), True),
        StructField("status", StringType(), True),
        StructField("event_time", TimestampType(), True),
        StructField("ingestion_time", TimestampType(), True),
        StructField("producer_id", StringType(), True),
        StructField("sequence_number", IntegerType(), True),
        StructField("schema_version", IntegerType(), True),
        # Milestone 2 appends this field to the processed-event JSON.
        StructField("spark_batch_id", LongType(), True),
    ]
)

EVENT_COLUMNS = [
    "event_id",
    "transaction_id",
    "event_type",
    "payment_rail",
    "amount_minor",
    "currency",
    "sender_bank",
    "receiver_bank",
    "status",
    "event_time",
    "ingestion_time",
    "producer_id",
    "sequence_number",
    "schema_version",
]

MERGE_SQL = """
INSERT INTO payment_events (
    event_id, transaction_id, event_type, payment_rail, amount_minor,
    currency, sender_bank, receiver_bank, status, event_time,
    ingestion_time, producer_id, sequence_number, schema_version,
    source_batch_id
)
SELECT DISTINCT ON (event_id)
    event_id, transaction_id, event_type, payment_rail, amount_minor,
    currency, sender_bank, receiver_bank, status, event_time,
    ingestion_time, producer_id, sequence_number, schema_version,
    spark_batch_id
FROM payment_events_staging
WHERE spark_batch_id = %s
ORDER BY event_id, sink_received_at DESC
ON CONFLICT (event_id) DO NOTHING
"""

DELETE_STAGING_SQL = "DELETE FROM payment_events_staging WHERE spark_batch_id = %s"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="PayStream Spark-to-PostgreSQL sink")
    parser.add_argument("--bootstrap-servers", default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092"))
    parser.add_argument("--source-topic", default="payment-events-processed")
    parser.add_argument("--starting-offsets", choices=["earliest", "latest"], default="earliest")
    parser.add_argument("--checkpoint", default="./data/checkpoints/milestone-3/postgres-sink")
    parser.add_argument("--trigger-seconds", type=int, default=5)
    parser.add_argument("--max-offsets-per-trigger", type=int, default=5000)
    parser.add_argument("--db-host", default=os.getenv("POSTGRES_HOST", "localhost"))
    parser.add_argument("--db-port", type=int, default=int(os.getenv("POSTGRES_PORT", "5432")))
    parser.add_argument("--db-name", default=os.getenv("POSTGRES_DB", "paystream"))
    parser.add_argument("--db-user", default=os.getenv("POSTGRES_USER", "paystream"))
    parser.add_argument("--db-password", default=os.getenv("POSTGRES_PASSWORD", "paystream_dev_password"))
    parser.add_argument("--jdbc-partitions", type=int, default=3)
    return parser.parse_args()


def jdbc_url(args: argparse.Namespace) -> str:
    return f"jdbc:postgresql://{args.db_host}:{args.db_port}/{args.db_name}"


def psycopg_config(args: argparse.Namespace) -> dict[str, Any]:
    return {
        "host": args.db_host,
        "port": args.db_port,
        "dbname": args.db_name,
        "user": args.db_user,
        "password": args.db_password,
        "connect_timeout": 10,
    }


def build_spark() -> SparkSession:
    return (
        SparkSession.builder.appName("PayStream-Milestone-3-PostgreSQL-Sink")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "3")
        .getOrCreate()
    )


def merge_staging_batch(batch_id: int, args: argparse.Namespace) -> None:
    """Atomically merge one staging batch and clean its staging rows."""
    connection = psycopg2.connect(**psycopg_config(args))
    try:
        with connection:
            with connection.cursor() as cursor:
                cursor.execute(MERGE_SQL, (batch_id,))
                inserted = cursor.rowcount
                cursor.execute(DELETE_STAGING_SQL, (batch_id,))
                deleted = cursor.rowcount
        LOG.info(
            "Batch %s merged into PostgreSQL: inserted=%s, staging_rows_deleted=%s",
            batch_id,
            inserted,
            deleted,
        )
    finally:
        connection.close()


def write_batch(batch_df: DataFrame, batch_id: int, args: argparse.Namespace) -> None:
    """Stage a micro-batch, then idempotently merge it into the target table."""
    if batch_df.isEmpty():
        LOG.info("Batch %s is empty; skipping", batch_id)
        return

    # A Kafka sink can replay messages after failures. Remove repeated event IDs
    # inside this micro-batch before writing to staging; the database primary key
    # handles duplicates across different micro-batches/restarts.
    staged = (
        batch_df.dropDuplicates(["event_id"])
        .select(*EVENT_COLUMNS)
        .withColumn("spark_batch_id", F.lit(batch_id).cast("long"))
    )

    jdbc_properties = {
        "user": args.db_user,
        "password": args.db_password,
        "driver": "org.postgresql.Driver",
        "batchsize": "1000",
        "numPartitions": str(args.jdbc_partitions),
    }

    (
        staged.coalesce(args.jdbc_partitions)
        .write.format("jdbc")
        .option("url", jdbc_url(args))
        .option("dbtable", "payment_events_staging")
        .options(**jdbc_properties)
        .mode("append")
        .save()
    )

    # Merge only after Spark's JDBC write completes successfully. If the process
    # fails before this transaction, Spark can replay the batch and the staging
    # rows are safely collapsed by event_id during the merge.
    merge_staging_batch(batch_id, args)


def start(args: argparse.Namespace) -> None:
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    source = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", args.bootstrap_servers)
        .option("subscribe", args.source_topic)
        .option("startingOffsets", args.starting_offsets)
        .option("maxOffsetsPerTrigger", args.max_offsets_per_trigger)
        .option("failOnDataLoss", "false")
        .load()
        .select(F.col("value").cast("string").alias("raw_value"))
    )

    parsed = source.withColumn("event", F.from_json("raw_value", EVENT_SCHEMA))
    events = parsed.select("event.*")

    # The processed topic should contain only valid Milestone 2 output. Fail the
    # micro-batch visibly rather than silently discarding an unparsable message.
    def process_batch(batch_df: DataFrame, batch_id: int) -> None:
        malformed_count = batch_df.filter(F.col("event_id").isNull()).count()
        if malformed_count:
            raise ValueError(
                f"Batch {batch_id} contains {malformed_count} malformed processed-topic messages. "
                "Inspect the source topic before restarting the sink."
            )
        write_batch(batch_df, batch_id, args)

    query = (
        events.writeStream.queryName("paystream-postgres-sink")
        .foreachBatch(process_batch)
        .option("checkpointLocation", args.checkpoint)
        .trigger(processingTime=f"{args.trigger_seconds} seconds")
        .start()
    )

    LOG.info(
        "PostgreSQL sink started: kafka_topic=%s jdbc_url=%s",
        args.source_topic,
        jdbc_url(args),
    )
    try:
        query.awaitTermination()
    finally:
        query.stop()
        spark.stop()


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
    )
    start(parse_args())
