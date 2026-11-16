# PayStream

PayStream is a local payment-event streaming project built around Apache Kafka, Python, Pydantic, and Spark Structured Streaming. It generates synthetic payment workflows, validates event schemas, and demonstrates how to route valid, invalid, and late-arriving records through Kafka topics.

The project is designed for local experimentation and end-to-end demos rather than production deployment.

## What this project does

- Generates realistic payment lifecycles for a transaction
- Publishes lifecycle events to Kafka
- Validates payloads against a strict schema using Pydantic
- Simulates duplicates, malformed payloads, and late event timestamps
- Exposes a Kafka consumer for inspection and debugging
- Includes a Spark-based streaming processor for validation, watermarking, and DLQ-style routing

## Project structure

- [src/generator.py](src/generator.py) — builds synthetic payment lifecycles
- [src/models.py](src/models.py) — Pydantic event model and validation rules
- [src/producer.py](src/producer.py) — writes events to Kafka
- [src/consumer.py](src/consumer.py) — reads and inspects Kafka messages
- [src/stream_processor.py](src/stream_processor.py) — Spark job for streaming validation and routing
- [src/config.py](src/config.py) — Kafka configuration defaults
- [schemas/payment_event.schema.json](schemas/payment_event.schema.json) — JSON schema for payment events
- [tests](tests) — validation and lifecycle tests

## Prerequisites

Before you begin, make sure you have:

- macOS or Linux environment
- Docker Desktop running
- Python 3.11+
- Java installed for Spark (required by the streaming processor)

Check the environment:

```bash
docker --version
docker compose version
python3 --version
java -version
```

## Local setup

### 1. Start Kafka

```bash
docker compose up -d
docker compose ps
chmod +x scripts/create_topics.sh
./scripts/create_topics.sh
```

This starts the local Kafka cluster and creates the expected topics. The default bootstrap server is:

- Local machine: `localhost:29092`
- Inside Docker: `kafka:9092`

### 2. Create the Python environment

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
pip install -r requirements.txt
```

## Generating payment events

Each transaction emits a short lifecycle with ordered events such as:

- `PAYMENT_INITIATED`
- `PAYMENT_STATUS_UPDATED`
- `PAYMENT_COMPLETED` or `PAYMENT_FAILED`

Run the producer:

```bash
python -m src.producer --count 100
python -m src.producer --count 1000 --rate 20
```

Add noisy traffic to simulate real-world issues:

```bash
python -m src.producer --count 1000 --rate 20 \
  --duplicate-rate 0.02 --malformed-rate 0.01 \
  --late-rate 0.03 --late-delay-minutes 10
```

### Meaning of the flags

- `--count`: total lifecycle transactions to generate
- `--rate`: events pushed per second (or a producer pacing rate)
- `--duplicate-rate`: probability of sending a duplicate payload
- `--malformed-rate`: probability of generating invalid JSON or invalid fields
- `--late-rate`: probability of emitting late data with a stale `event_time`

## Consuming and inspecting events

Open a second terminal and run:

```bash
source .venv/bin/activate
python -m src.consumer
```

Read only a fixed number of messages:

```bash
python -m src.consumer --max-messages 30
```

Read from a fresh consumer group from the beginning of the topic:

```bash
python -m src.consumer --group-id paystream-inspector-2
```

The consumer validates each event using the `PaymentEvent` model and prints any schema failures.

## Event contract

The event schema is defined in [schemas/payment_event.schema.json](schemas/payment_event.schema.json) and mirrored by [src/models.py](src/models.py).

Key rules:

- `event_id` uniquely identifies a message
- `transaction_id` groups all events for one payment lifecycle
- `amount_minor` is stored as an integer in paise to avoid floating-point money issues
- `event_time` and `ingestion_time` must be timezone-aware timestamps
- `currency` is constrained to `INR`
- `event_type`, `payment_rail`, `status`, and `schema_version` are strictly validated

## Kafka topics

This project expects the following topics:

- `payment-events` — canonical payment lifecycle stream
- `payment-events-retry` — reserved for retryable failures
- `payment-events-dlq` — reserved for invalid or unrecoverable records
- `payment-events-audit` — reserved for audit or operational events

Inspect the topics:

```bash
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 --list

docker compose exec kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 --describe --topic payment-events
```

## Running tests

```bash
pytest -q
```

The suite checks the generator lifecycle, unique IDs, malformed payload handling, and schema constraints.

## Spark streaming processor

The project also includes a Spark Structured Streaming processor in [src/stream_processor.py](src/stream_processor.py). It reads from Kafka, validates records, routes invalid records to a DLQ topic, and tracks late-arriving events using event-time watermarks.

Typical usage:

```bash
PYSPARK_PYTHON=python3 spark-submit src/stream_processor.py \
  --bootstrap-servers localhost:29092 \
  --source-topic payment-events \
  --processed-topic payment-events-processed \
  --dlq-topic payment-events-dlq \
  --late-topic payment-events-late
```

## Stopping services

```bash
docker compose down
```

To fully reset Kafka state and delete local data:

```bash
docker compose down -v
```

## Notes and limitations

- This is a local development and demo project, not production infrastructure.
- Kafka is run with a local single-node configuration.
- Validation is implemented in Python and Spark for demonstration purposes.
- Retry and DLQ flows are prepared for extension but may still require additional operational handling.

## Suggested next steps

- add a real DLQ consumer and retry loop
- integrate with a data warehouse or analytics sink
- add schema registry support
- move event validation into a shared contract layer across producer and consumer
- add monitoring and alerting around Kafka lag and invalid-message rates
