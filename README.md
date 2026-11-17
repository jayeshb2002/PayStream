# PayStream

PayStream is a local payment-event streaming project that demonstrates a full event pipeline using Kafka, Python, Spark Structured Streaming, and PostgreSQL, with Apache Airflow added for orchestration. The pipeline milestones cover event generation and ingestion, validation and streaming processing, database persistence and analytics, and Airflow integration.

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

## Milestone 4.1: Airflow orchestration

Milestone 4.1 adds Apache Airflow for workflow orchestration without replacing or modifying the Kafka, Spark, or PayStream PostgreSQL components. Airflow uses a separate PostgreSQL 16 metadata database, `LocalExecutor`, and the `paystream_m4_smoke_test` starter DAG.

Airflow services are in the main Compose file but use the `airflow` profile, so the existing default `docker compose up` workflow continues to start only Kafka and PayStream PostgreSQL.

### Configure Airflow

Append these settings to the existing, Git-ignored `.env` file. Keep its PayStream PostgreSQL settings; do not replace the file. Change the example credentials and secret before starting Airflow.

```env
AIRFLOW_DB_USER=airflow
AIRFLOW_DB_PASSWORD=change_this_metadata_password
AIRFLOW_DB_NAME=airflow
AIRFLOW_ADMIN_USERNAME=admin
AIRFLOW_ADMIN_PASSWORD=change_this_admin_password
AIRFLOW_WEBSERVER_SECRET_KEY=change_this_to_a_long_random_secret
```

Create the local Airflow directories:

```bash
mkdir -p dags logs plugins
```

### Initialize and start Airflow

Run the initialization command and wait for it to finish successfully:

```bash
docker compose --profile airflow up airflow-db airflow-init
```

Start the Airflow webserver and scheduler:

```bash
docker compose --profile airflow up -d airflow-webserver airflow-scheduler
docker compose --profile airflow ps
```

Open [http://localhost:8080](http://localhost:8080) and sign in with `AIRFLOW_ADMIN_USERNAME` and `AIRFLOW_ADMIN_PASSWORD` from `.env`.

### Verify the smoke-test DAG

1. Find `paystream_m4_smoke_test` in the DAG list and enable it if paused.
2. Open the DAG and select **Trigger DAG**.
3. In Grid view, confirm `check_airflow_runtime` and `orchestration_ready` both succeed.
4. Open each task's logs to inspect its output.

This verifies the Airflow installation only; it does not start Kafka or Spark jobs.

## Milestone 4.2: Airflow and PostgreSQL integration (completed)

Milestone 4.2 integrates Airflow with the PayStream PostgreSQL service and adds data quality validation for `payment_events`.

- `paystream_m4_2_db_connection` checks the database connection and reports the database, user, and server time.
- `paystream_data_quality` checks the database connection, then runs 13 SQL-based validations against `payment_events` and reports the results. The validation task fails if any checks find invalid records.

### Configure the database connection

Add these values to the existing `.env` file. Use the same database name, user, and password as the PayStream PostgreSQL settings above. The host and port below are for container-to-container communication, not the host machine.

```env
PAYSTREAM_DB_HOST=postgres
PAYSTREAM_DB_PORT=5432
PAYSTREAM_DB_NAME=paystream
PAYSTREAM_DB_USER=paystream
PAYSTREAM_DB_PASSWORD=change_this_to_match_POSTGRES_PASSWORD
```

Ensure the Airflow settings from Milestone 4.1 are also present. Compose mounts the application source at `/opt/airflow/src` and sets `PYTHONPATH=/opt/airflow`, allowing DAGs to import the `src` package. Start PayStream PostgreSQL and the Airflow services:

```bash
docker compose up -d postgres
docker compose --profile airflow up -d airflow-webserver airflow-scheduler
```

In the Airflow UI at [http://localhost:8080](http://localhost:8080):

1. Trigger `paystream_m4_2_db_connection` and confirm `check_paystream_postgres_connection` succeeds. The task should report the database, user, and server time.
2. Trigger `paystream_data_quality` and confirm `check_database_connection` succeeds. Review `execute_data_quality_checks` for the validation report; it fails when checks find invalid records.

View service logs with:

```bash
docker compose --profile airflow logs -f airflow-webserver airflow-scheduler
```

### Airflow commands and notes

```bash
docker compose --profile airflow logs -f airflow-webserver airflow-scheduler
docker compose --profile airflow logs airflow-init
docker compose --profile airflow stop airflow-webserver airflow-scheduler airflow-db
```

Stopping services preserves Airflow metadata. Do not use `docker compose down -v` unless you intend to delete the Airflow metadata volume. This is a local development setup, not a production Airflow deployment.

## Technology stack

- Python 3.11+
- Kafka (via Docker and KRaft)
- Confluent Kafka Python client
- Pydantic for event validation
- Spark Structured Streaming
- PostgreSQL
- Apache Airflow with LocalExecutor
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
