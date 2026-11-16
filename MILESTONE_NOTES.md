# PayStream milestone notes

This file consolidates the design and implementation notes for all three milestones of the project.

## Milestone 1: Kafka ingestion foundation

### Goal
Create a working local payment-event pipeline with Kafka as the event backbone.

### What was added
- `src/generator.py` generates realistic payment lifecycle events.
- `src/models.py` defines the strict Pydantic schema for a payment event.
- `src/producer.py` publishes synthetic payment events to Kafka.
- `src/consumer.py` inspects Kafka records and validates them as they are consumed.
- `src/config.py` centralizes Kafka defaults.
- `schemas/payment_event.schema.json` documents the event contract.

### Key decisions
- one lifecycle uses a shared `transaction_id`
- each event has a unique `event_id`
- `amount_minor` stores money as integer paise, which avoids float-based rounding issues
- `event_time` and `ingestion_time` must be timezone-aware timestamps
- Kafka message keys use `transaction_id` to preserve ordering by transaction

### Simulated edge cases
- duplicate messages
- malformed payloads
- late timestamps
- invalid schema fields

These behaviors help build confidence in downstream validation and recovery logic.

## Milestone 2: Spark validation and routing

### Goal
Process Kafka data with Spark Structured Streaming, validating messages and routing based on quality.

### What was added
- `src/stream_processor.py` implements a read-stream -> validate -> route pipeline
- the processor reads from `payment-events`
- valid events are sent to `payment-events-processed`
- invalid records are sent to `payment-events-dlq`
- late events are sent to `payment-events-late`

### Why there are separate output topics
A duplicate is different from a late event:
- A duplicate has the same `event_id` as something already seen in the deduplication window.
- A late event is one whose event time is older than the current watermark threshold.

The application keeps the distinction explicit so downstream systems can react differently
for data quality, replay, and event-time processing issues.

### Semantic takeaway
The source offsets and Spark checkpointing provide recoverability, but Kafka output writes are not guaranteed to be perfectly transactionally aligned with the Spark checkpoint. Downstream systems should treat `event_id` as the idempotency key.

## Milestone 3: PostgreSQL storage and analytics

### Goal
Persist clean processed payment events in PostgreSQL and expose analytics views.

### What was added
- `src/postgres_sink.py` consumes processed Kafka events and writes them to Postgres
- the single `docker-compose.yml` file runs the local Kafka and PostgreSQL services together
- `sql/001_init.sql` creates the database schema, indexes, and analytics views
- `payment_events_staging` acts as temporary micro-batch storage
- `payment_events` is the permanent store
- analytics views summarize results by day and by bank

### Database design
- Kafka consumer writes batches into `payment_events_staging`
- a Python/psycopg2 transaction merges those rows into the final table
- `event_id` is used for idempotent upserts
- staging rows are deleted after merge to ensure a clean batch cycle

### Analytics outputs
- `daily_payment_analytics`
- `bank_payment_analytics`

These views make the data easier to query without requiring a separate reporting layer.

## Cross-milestone lessons

1. Start with a validated event contract before scaling the pipeline.
2. Kafka is excellent for decoupling producers from consumers, but it needs explicit replay and idempotency logic.
3. Spark helps with validation, watermarking, and routing at stream time.
4. Storage and analytics should be idempotent and replay-safe.
5. Local development should never depend on production credentials or persistent secrets.

## Recommended next steps

- add a real retry worker for failed Kafka messages
- integrate a schema registry to centralize contract evolution
- add monitoring for Kafka lag and invalid message rates
- expand the analytics layer to include failure breakdowns by bank or rail
- move from local-only demos to a cloud-ready deployment pattern
