"""PayStream Kafka -> Spark Structured Streaming -> Kafka pipeline.

Run from the PayStream repository root with spark-submit.
The job intentionally uses two streaming queries so malformed records can be
routed independently from valid, deduplicated records.
"""

from __future__ import annotations

import time
from src.orchestration.metrics_collector import record_batch_metrics_safely

from src.orchestration.failure_tracker import (
    record_pipeline_failure_safely
)

import argparse
import logging
import os
import re

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

LOG = logging.getLogger("paystream")

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
    ]
)

VALID_EVENT_TYPES = [
    "PAYMENT_INITIATED",
    "PAYMENT_STATUS_UPDATED",
    "PAYMENT_COMPLETED",
    "PAYMENT_FAILED",
]
VALID_RAILS = ["UPI", "IMPS", "RUPAY", "NEFT"]
VALID_STATUSES = ["INITIATED", "PROCESSING", "SUCCESS", "FAILED"]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="PayStream Spark streaming processor")
    parser.add_argument("--bootstrap-servers", default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092"))
    parser.add_argument("--source-topic", default="payment-events")
    parser.add_argument("--processed-topic", default="payment-events-processed")
    parser.add_argument("--dlq-topic", default="payment-events-dlq")
    parser.add_argument("--late-topic", default="payment-events-late")
    parser.add_argument("--checkpoint-root", default="./data/checkpoints/milestone-2")
    parser.add_argument("--watermark", default="10 minutes")
    parser.add_argument("--max-offsets-per-trigger", type=int, default=5000)
    parser.add_argument("--starting-offsets", choices=["earliest", "latest"], default="earliest")
    parser.add_argument("--trigger-seconds", type=int, default=5)
    parser.add_argument("--shuffle-partitions", type=int, default=3)
    return parser.parse_args()


def build_spark():
    return (
        SparkSession.builder
        .appName("PayStreamStreamProcessor")
        .config(
            "spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.5"
        )
        .getOrCreate()
    )


def with_validation_columns(parsed: DataFrame) -> DataFrame:
    """Attach a human-readable validation error; null means valid."""
    checks = [
        (F.col("event_id").isNull() | (F.length(F.trim("event_id")) == 0), "missing_event_id"),
        (F.col("transaction_id").isNull() | (F.length(F.trim("transaction_id")) == 0), "missing_transaction_id"),
        (~F.col("event_type").isin(VALID_EVENT_TYPES) | F.col("event_type").isNull(), "invalid_event_type"),
        (~F.col("payment_rail").isin(VALID_RAILS) | F.col("payment_rail").isNull(), "invalid_payment_rail"),
        (F.col("amount_minor").isNull() | (F.col("amount_minor") < 0), "invalid_amount_minor"),
        (F.col("currency").isNull() | (F.length(F.trim("currency")) != 3), "invalid_currency"),
        (~F.col("status").isin(VALID_STATUSES) | F.col("status").isNull(), "invalid_status"),
        (F.col("event_time").isNull(), "missing_or_invalid_event_time"),
        (F.col("ingestion_time").isNull(), "missing_or_invalid_ingestion_time"),
        (F.col("producer_id").isNull(), "missing_producer_id"),
        (F.col("sequence_number").isNull() | (F.col("sequence_number") < 0), "invalid_sequence_number"),
        (F.col("schema_version").isNull() | (F.col("schema_version") < 1), "invalid_schema_version"),
    ]
    error_expr = F.concat_ws(
        ",",
        *[F.when(condition, F.lit(reason)) for condition, reason in checks],
    )
    return parsed.withColumn(
        "error_reason",
        F.when(F.col("event_id").isNull(), F.lit("invalid_json_or_missing_fields")).otherwise(error_expr),
    )


def watermark_interval_sql(watermark: str) -> str:
    """Convert Spark duration text (for example, 10 minutes) to SQL interval syntax."""
    match = re.fullmatch(r"\s*(\d+)\s+(seconds?|minutes?|hours?|days?)\s*", watermark, re.IGNORECASE)
    if not match:
        raise ValueError("Watermark must look like '10 minutes' or '30 seconds'")
    amount, unit = match.groups()
    return f"INTERVAL {amount} {unit.upper()}"


def kafka_write(df: DataFrame, topic: str, bootstrap_servers: str) -> None:
    """Write a batch to Kafka. Kafka sink writes are at-least-once on retries."""
    if df.isEmpty():
        return
    (
        df.select(
            F.col("event_id").cast("string").alias("key"),
            F.to_json(F.struct(*[F.col(c) for c in df.columns])).alias("value"),
        )
        .selectExpr("CAST(key AS BINARY)", "CAST(value AS BINARY)")
        .withColumn("topic", F.lit(topic))
        .write.format("kafka")
        .option("kafka.bootstrap.servers", bootstrap_servers)
        .save()
    )


def write_invalid_batch(batch_df: DataFrame, batch_id: int, topic: str, bootstrap_servers: str) -> None:
    record_count = batch_df.count()
    LOG.warning(
        "DEBUG: Invalid batch %s received %s records",
        batch_id,
        record_count,
    )

    if record_count == 0:
        return
    if batch_df.isEmpty():
        return
    LOG.warning("Batch %s: routing invalid records to %s", batch_id, topic)
    output = batch_df.select(
        F.coalesce(F.col("event_id"), F.lit("unknown")).cast("string").alias("key"),
        F.to_json(
            F.struct(
                F.col("error_reason"),
                F.col("raw_value"),
                F.col("source_topic"),
                F.col("source_partition"),
                F.col("source_offset"),
                F.current_timestamp().alias("processed_at"),
            )
        ).alias("value"),
    )
    (output.selectExpr("CAST(key AS BINARY)", "CAST(value AS BINARY)")
        .withColumn("topic", F.lit(topic))
        .write.format("kafka")
        .option("kafka.bootstrap.servers", bootstrap_servers)
        .save())


def write_processed_batch(batch_df: DataFrame, batch_id: int, args: argparse.Namespace) -> None:
    record_count = batch_df.count()
    LOG.warning(
        "DEBUG: Processed batch %s received %s records",
        batch_id,
        record_count,
    )

    if record_count == 0:
        return
    if batch_df.isEmpty():
        return
    output = batch_df.select(
        F.col("event_id").cast("string").alias("key"),
        F.to_json(
            F.struct(*[F.col(c) for c in batch_df.columns], F.lit(batch_id).alias("spark_batch_id"))
        ).alias("value"),
    )
    (output.selectExpr("CAST(key AS BINARY)", "CAST(value AS BINARY)")
        .withColumn("topic", F.lit(args.processed_topic))
        .write.format("kafka")
        .option("kafka.bootstrap.servers", args.bootstrap_servers)
        .save())
    LOG.info("Wrote processed batch %s", batch_id)


def write_late_batch(batch_df: DataFrame, batch_id: int, args: argparse.Namespace) -> None:
    record_count = batch_df.count()
    LOG.warning(
        "DEBUG: Late batch %s received %s records",
        batch_id,
        record_count,
    )

    if record_count == 0:
        return
    if batch_df.isEmpty():
        return
    output = batch_df.select(
        F.col("event_id").cast("string").alias("key"),
        F.to_json(
            F.struct(
                *[F.col(c) for c in batch_df.columns],
                F.lit("event_arrived_after_watermark_delay").alias("late_reason"),
                F.lit(batch_id).alias("spark_batch_id"),
            )
        ).alias("value"),
    )
    (output.selectExpr("CAST(key AS BINARY)", "CAST(value AS BINARY)")
        .withColumn("topic", F.lit(args.late_topic))
        .write.format("kafka")
        .option("kafka.bootstrap.servers", args.bootstrap_servers)
        .save())
    LOG.info("Wrote late-event batch %s", batch_id)



def instrument_batch(callback, metric_name):
    def wrapped(batch_df, batch_id):
        start_time = time.perf_counter()

        try:
            record_count = batch_df.count()

            callback(batch_df, batch_id)

            duration_ms = int(
                (time.perf_counter() - start_time) * 1000
            )

            record_batch_metrics_safely(
                component="spark_processor",
                batch_id=batch_id,
                metrics={metric_name: record_count},
                processing_duration_ms=duration_ms
            )

        except Exception as exc:
            record_pipeline_failure_safely(
                component="spark_processor",
                failure_type="SPARK_BATCH_FAILURE",
                exception=exc,
                severity="CRITICAL",
                context={
                    "batch_id": batch_id,
                    "metric_name": metric_name
                }
            )

            raise

    return wrapped



def start(args: argparse.Namespace) -> None:
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", args.bootstrap_servers)
        .option("subscribe", args.source_topic)
        .option("startingOffsets", args.starting_offsets)
        .option("maxOffsetsPerTrigger", args.max_offsets_per_trigger)
        .option("failOnDataLoss", "false")
        .load()
        .select(
            F.col("value").cast("string").alias("raw_value"),
            F.col("topic").alias("source_topic"),
            F.col("partition").alias("source_partition"),
            F.col("offset").alias("source_offset"),
        )
    )

    parsed = raw.withColumn("event", F.from_json("raw_value", EVENT_SCHEMA)).select(
        "raw_value", "source_topic", "source_partition", "source_offset", "event.*"
    )
    checked = with_validation_columns(parsed)
    invalid = checked.filter(F.col("error_reason") != "")
    valid = checked.filter(F.col("error_reason") == "").drop("error_reason", "raw_value", "source_topic", "source_partition", "source_offset")

    # Query 1: malformed / semantically invalid data -> DLQ.
    invalid_query = (
        invalid.writeStream.queryName("paystream-invalid-to-dlq")
        .foreachBatch(
            instrument_batch(write_invalid_batch, "records_rejected")
        )
        .option("checkpointLocation", os.path.join(args.checkpoint_root, "invalid"))
        .trigger(processingTime=f"{args.trigger_seconds} seconds")
        .start()
    )

    # Split delayed arrivals BEFORE event-time deduplication. Otherwise Spark's
    # watermark operator can discard very old events before our late topic sees them.
    delay_interval = F.expr(watermark_interval_sql(args.watermark))
    late_events = valid.filter(F.col("ingestion_time") > F.col("event_time") + delay_interval)
    on_time_events = valid.filter(F.col("ingestion_time") <= F.col("event_time") + delay_interval)

    # Query 2: late arrivals -> deduplicate using ingestion time -> late topic.
    late_deduplicated = (
        late_events.withWatermark("ingestion_time", args.watermark)
        .dropDuplicatesWithinWatermark(["event_id"])
    )
    late_query = (
        late_deduplicated.writeStream.queryName("paystream-late-events")
        .foreachBatch(
            instrument_batch(write_late_batch, "records_late")
        )      
        .option("checkpointLocation", os.path.join(args.checkpoint_root, "late"))
        .trigger(processingTime=f"{args.trigger_seconds} seconds")
        .start()
    )

    # Query 3: on-time valid events -> event-time deduplication -> processed topic.
    deduplicated = (
        on_time_events.withWatermark("event_time", args.watermark)
        .dropDuplicatesWithinWatermark(["event_id"])
    )
    valid_query = (
        deduplicated.writeStream.queryName("paystream-valid-events")
        .foreachBatch(
            instrument_batch(write_processed_batch, "records_processed")
        )      
        .option("checkpointLocation", os.path.join(args.checkpoint_root, "valid"))
        .trigger(processingTime=f"{args.trigger_seconds} seconds")
        .start()
    )

    LOG.info("PayStream Spark processor started; reading topic %s", args.source_topic)
    try:
        spark.streams.awaitAnyTermination()
    finally:
        for query in spark.streams.active:
            query.stop()
        spark.stop()


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
    )
    arguments = parse_args()
    os.environ["SPARK_SHUFFLE_PARTITIONS"] = str(arguments.shuffle_partitions)
    start(arguments)
