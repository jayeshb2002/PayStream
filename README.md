# PayStream — Milestone 1: Kafka Ingestion Foundation

Local payment-event ingestion using Apache Kafka (KRaft), Python, Pydantic and
the Confluent Kafka client.

## Prerequisites
- macOS
- Docker Desktop running
- Python 3.11 or 3.12

Check:
```bash
docker --version
docker compose version
python3 --version
```

## 1. Start Kafka and create topics
```bash
docker compose up -d
docker compose ps
chmod +x scripts/create_topics.sh
./scripts/create_topics.sh
```

Kafka bootstrap address from your Mac: `localhost:29092`.
Kafka address inside Docker: `kafka:9092`.

## 2. Create Python environment
```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
pip install -r requirements.txt
```

## 3. Generate transactions
Each transaction emits three lifecycle events: initiated, processing, and
completed or failed.

```bash
python -m src.producer --count 100
python -m src.producer --count 1000 --rate 20
```

Inject duplicates, malformed messages and late event timestamps:
```bash
python -m src.producer --count 1000 --rate 20 \
  --duplicate-rate 0.02 --malformed-rate 0.01 \
  --late-rate 0.03 --late-delay-minutes 10
```

Rates are probabilities per valid lifecycle event. Duplicates and malformed
messages are additional Kafka messages. Late messages have an old `event_time`
and current `ingestion_time`.

## 4. Consume and inspect events
In another terminal:
```bash
source .venv/bin/activate
python -m src.consumer
```

Read a fixed number:
```bash
python -m src.consumer --max-messages 30
```

Use a new consumer group to read from the beginning:
```bash
python -m src.consumer --group-id paystream-inspector-2
```

The inspector validates messages and manually commits offsets after inspecting
each message. Invalid messages are reported; DLQ routing comes in Milestone 2.

## 5. Run tests
```bash
pytest -q
```

## 6. Inspect Kafka
```bash
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 --list

docker compose exec kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 --describe --topic payment-events
```

## 7. Stop
```bash
docker compose down
```
To also delete Kafka's local data volume:
```bash
docker compose down -v
```

## Event contract
See `schemas/payment_event.schema.json`. `amount_minor` is an integer amount
in paise for INR, avoiding floating-point money arithmetic. `event_id` uniquely
identifies an event; `transaction_id` is shared across one payment lifecycle.
Kafka message key is `transaction_id`, preserving order within a partition.

## Topics
- `payment-events`: incoming payment lifecycle events
- `payment-events-retry`: reserved for retryable failures
- `payment-events-dlq`: reserved for invalid/permanently failed messages
- `payment-events-audit`: reserved for processing audit events

## Milestone 1 limitations
- One local broker; replication factor one.
- Schema is versioned and validated in Python; Schema Registry integration comes later.
- The consumer is an inspector, not a production sink.
- No end-to-end exactly-once guarantee across Kafka and external databases.
- Late-event handling is simulated here; Spark watermarking comes in Milestone 2.
- Retry and DLQ topics are created but not yet wired into the consumer.

## Next milestone
Build Spark Structured Streaming over `payment-events`, with validation,
event-ID deduplication, event-time watermarks and DLQ routing.
