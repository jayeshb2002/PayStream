# PayStream

PayStream is a local payment-event streaming project that demonstrates a full event pipeline using Kafka, Python, Spark Structured Streaming, and PostgreSQL. It is structured as three milestones: event generation and ingestion, validation and streaming processing, and database persistence with analytics.

## Architecture overview

```text
Python Producer
      |
      v
Kafka Topics
      |
      v
Spark Structured Streaming
      |
      +--> valid events --> payment-events-processed
      |
      +--> invalid events --> payment-events-dlq
      |
      +--> late events --> payment-events-late
      |
      v
PostgreSQL sink + analytics views
```

## Milestone 1: Kafka ingestion foundation

This milestone establishes the base pipeline for generating and ingesting payment events.

Features:
- synthetic payment lifecycle generation
- Kafka producer for publishing events
- Pydantic-based event validation
- Kafka consumer for inspection and debugging
- schema definition in JSON
- simulated duplicates, malformed records, and late events

Core files:
- `src/generator.py`
- `src/models.py`
- `src/producer.py`
- `src/consumer.py`
- `src/config.py`
- `schemas/payment_event.schema.json`

## Milestone 2: streaming validation and routing

This milestone adds a Spark Structured Streaming layer that reads from Kafka, validates payloads, and routes results into different downstream topics.

Features:
- JSON parsing from Kafka payloads
- validation checks for missing fields, bad enums, invalid timestamps, and bad schema versions
- watermarking for late-data handling
- deduplication by `event_id`
- routing of valid, invalid, and late records to separate Kafka topics

Core files:
- `src/stream_processor.py`
- `src/config.py`
- `schemas/payment_event.schema.json`

## Milestone 3: PostgreSQL storage and analytics

This milestone stores processed events in PostgreSQL and makes the pipeline analytically useful.

Features:
- Spark JDBC sink to PostgreSQL
- staging table + transactional merge into a permanent table
- idempotent inserts using `event_id`
- daily and bank-wise analytics views
- operational support for local Postgres with Docker Compose

Core files:
- `src/postgres_sink.py`
- `docker-compose.yml`
- `sql/001_init.sql`
- `src/stream_processor.py`

## Technology stack

- Python 3.11+
- Kafka (via Docker and KRaft)
- Confluent Kafka Python client
- Pydantic for event validation
- Spark Structured Streaming
- PostgreSQL
- pytest for automated checks

## Prerequisites

Before starting the project, ensure the following are installed and running:

- Docker Desktop or Docker Engine
- Python 3.11+
- Java (required for Spark)
- Access to local ports for Kafka and PostgreSQL

Check:

```bash
docker --version
docker compose version
python3 --version
java -version
```

## Local setup

### 1. Create the Python environment

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
pip install -r requirements.txt
```

### 2. Start the local services

Use the single compose file for both Kafka and PostgreSQL:

```bash
docker compose up -d
docker compose ps
chmod +x scripts/create_topics.sh
./scripts/create_topics.sh
```

The default Kafka bootstrap address is:
- host: `localhost:29092`
- inside Docker: `kafka:9092`

PostgreSQL runs on:
- host: `localhost:${POSTGRES_PORT}`
- inside Docker: `postgres:5432`

If you need a local environment file, create or edit `.env` with:

```env
POSTGRES_DB=paystream
POSTGRES_USER=paystream
POSTGRES_PASSWORD=paystream_dev_password
POSTGRES_HOST=localhost
POSTGRES_PORT=5433
```

### 3. Generate payment lifecycle events

```bash
python -m src.producer --count 100
python -m src.producer --count 1000 --rate 20
```

To simulate noisy traffic:

```bash
python -m src.producer \
  --count 1000 \
  --rate 20 \
  --duplicate-rate 0.02 \
  --malformed-rate 0.01 \
  --late-rate 0.03 \
  --late-delay-minutes 10
```

### 4. Consume and inspect Kafka messages

Open a second terminal:

```bash
source .venv/bin/activate
python -m src.consumer
```

Additional examples:

```bash
python -m src.consumer --max-messages 30
python -m src.consumer --group-id paystream-inspector-2
```

### 5. Run the Spark streaming processor

```bash
PYSPARK_PYTHON=python3 spark-submit \
  --master 'local[*]' \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.5 \
  src/stream_processor.py
```

This application consumes `payment-events`, validates the messages, and routes valid records to `payment-events-processed` while sending invalid and late records to their respective DLQ/late topics.

### 6. Start the PostgreSQL sink

The project now uses the same compose file for all services, so you do not need a separate overlay file. If the database is new, the init script will run automatically when the container starts.

If you need to re-run the schema manually:

```bash
docker compose exec -T postgres psql -U paystream -d paystream < sql/001_init.sql
```

Now start the sink:

```bash
set -a
source .env
set +a

spark-submit \
  --master 'local[*]' \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.5,org.postgresql:postgresql:42.7.5 \
  src/postgres_sink.py
```

## Event contract

The event schema is defined in `schemas/payment_event.schema.json` and enforced by `PaymentEvent` in `src/models.py`.

Key rules:
- `event_id` is unique per event
- `transaction_id` groups events for one lifecycle
- `amount_minor` stores money as integer paise
- timestamps must be timezone-aware
- `currency` is restricted to `INR`
- `event_type`, `payment_rail`, and `status` are validated against enums

## Kafka topics

- `payment-events` — source topic with payment events
- `payment-events-processed` — valid events after Spark validation
- `payment-events-dlq` — invalid or unrecoverable messages
- `payment-events-late` — late-arriving events beyond the watermark
- `payment-events-retry` — reserved for retry flow
- `payment-events-audit` — reserved for audit trail processing

## Testing

Run the full test suite:

```bash
pytest -q
```

Or run the milestone-specific PostgreSQL sink tests:

```bash
pytest -v tests/test_postgres_sink.py
```

## Stopping services

```bash
docker compose down
```

To remove persistent Kafka/Postgres data volumes too:

```bash
docker compose down -v
```

## Notes

- This repository is for local development and demo purposes.
- `.env`, `.env.*`, and generated state should never be committed to Git.
- Kafka/Spark/Postgres are intentionally decoupled so each stage can be inspected independently.
- The design uses idempotent inserts by `event_id` to keep replays safe at the sink layer.

## Next steps

- add real DLQ consumers and retry workflows
- introduce schema registry integration
- expand analytics with bank, rail, and failure dashboards
- add end-to-end monitoring and alerting
