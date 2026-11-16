# Milestone 2 implementation notes

## New data flow

`payment-events` (Kafka) -> Spark JSON parsing and validation ->
- invalid records -> `payment-events-dlq`
- valid records -> event-time watermark + event_id deduplication ->
  - normal arrival delay -> `payment-events-processed`
  - arrival delay greater than watermark -> `payment-events-late`

## New files

- `src/stream_processor.py`: streaming application.
- `README_M2.md`: setup, run, test, verify, restart, and troubleshooting guide.

## Why there are two output classifications

A duplicate is an event whose `event_id` has already been observed within the watermark state. A late event is one whose `ingestion_time` is sufficiently later than its `event_time`. These are different conditions. The application uses Spark's watermark to bound deduplication state, and the explicit timestamp difference to classify delayed arrivals.

## Delivery semantics

The source offsets and deduplication state are checkpointed by Spark. The Kafka output sink is not transactionally committed with Spark's checkpoint, so retries can duplicate output records. Downstream consumers should use `event_id` as an idempotency key until a stronger sink strategy is implemented.
