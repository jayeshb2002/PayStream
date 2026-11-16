# PayStream — Milestone 2: Spark Structured Streaming

This is an overlay for the working Milestone 1 repository. Copy `src/stream_processor.py` into the existing `src/` directory. Keep your existing Kafka producer, consumer, Pydantic model, and Docker Compose file.

## What this milestone adds

- A Spark Structured Streaming job that reads `payment-events` from Kafka.
- JSON parsing against the Milestone 1 payment-event contract.
- Required-field / enum / amount / timestamp validation.
- Invalid-record routing to the existing `payment-events-dlq` topic.
- Event-ID deduplication with a 10-minute event-time watermark.
- A late-event output topic, `payment-events-late`, for events whose `ingestion_time` is more than the configured watermark after `event_time`.
- Valid events written to `payment-events-processed`.
- Separate checkpoint directories for the valid and invalid streaming queries.

## Prerequisites (macOS)

1. Milestone 1 Kafka is running: `docker compose up -d`.
2. Java 17 is installed. Check with `java -version`.
3. Python 3.10 or 3.11 is recommended for this Spark 3.5 setup.
4. Run these commands from the root of your existing PayStream repository.

Install Java 17 if needed (Homebrew):

```bash
brew install openjdk@17
```

Install PySpark into the same virtual environment used in Milestone 1:

```bash
source .venv/bin/activate
pip install pyspark==3.5.5
```

Spark's Kafka connector is fetched by `spark-submit` using Maven coordinates. The first run needs internet access.

## Add the processor

Copy `src/stream_processor.py` from this milestone overlay into your existing repository's `src/` folder. It does not replace any Milestone 1 files.

## Create output topics

Create the two new output topics. Run from the repository root:

```bash
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 --create --if-not-exists \
  --topic payment-events-processed --partitions 3 --replication-factor 1

docker compose exec kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 --create --if-not-exists \
  --topic payment-events-late --partitions 3 --replication-factor 1
```

The existing `payment-events-dlq` topic is reused.

## Start the processor

Keep Kafka running. Open a new terminal, go to the repository root, activate `.venv`, then run:

```bash
spark-submit \
  --master 'local[*]' \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.5 \
  src/stream_processor.py
```

The default Kafka address is `localhost:29092`, which is the host listener from the Milestone 1 Docker Compose setup.

Useful options:

```bash
spark-submit \
  --master 'local[*]' \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.5 \
  src/stream_processor.py \
  --starting-offsets earliest \
  --watermark '10 minutes' \
  --max-offsets-per-trigger 5000 \
  --trigger-seconds 5
```

To use a different broker address or topic names, pass `--bootstrap-servers`, `--source-topic`, `--processed-topic`, `--dlq-topic`, or `--late-topic`.

## Generate test traffic

In another terminal, activate `.venv` and use the Milestone 1 producer:

```bash
python -m src.producer --count 20 --rate 0
```

Generate a mix of normal, duplicate, malformed, and old-event-time records:

```bash
python -m src.producer \
  --count 100 \
  --rate 0 \
  --duplicate-rate 0.10 \
  --malformed-rate 0.05 \
  --late-rate 0.20 \
  --late-delay-minutes 20
```

Note: the Milestone 1 producer's `--late-rate` controls old `event_time` values. The processor classifies an event as late when `ingestion_time - event_time` exceeds the configured watermark. This is separate from Spark's watermark-based deduplication state.

## Verify output

Use separate terminals to consume each output topic. If your Milestone 1 consumer supports a `--topic` argument, use it; otherwise use Kafka's console consumer:

```bash
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic payment-events-processed \
  --from-beginning
```

```bash
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic payment-events-dlq \
  --from-beginning
```

```bash
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic payment-events-late \
  --from-beginning
```

## Checkpoints and restarting

The processor creates these local folders:

```text
data/checkpoints/milestone-2/valid/
data/checkpoints/milestone-2/invalid/
```

Stop the Spark job with Ctrl+C and restart it with the same checkpoint paths to resume its streaming progress. Do not delete these folders if you want to resume from the saved offsets and state. To intentionally replay from scratch, stop Spark first and remove only this milestone's checkpoint directory:

```bash
rm -rf data/checkpoints/milestone-2
```

Then restart Spark. Replaying can create duplicate output records because the Kafka sink and checkpoint are not a single cross-system transaction.

## Important engineering notes

- `dropDuplicatesWithinWatermark` is available in Spark 3.5+. It bounds deduplication state using event time and the configured watermark.
- The `payment-events-late` classification uses the difference between the event's own `ingestion_time` and `event_time`. It is not a direct feed of Spark's internally dropped late rows; Spark does not expose those rows from this deduplication operator.
- Events older than the active watermark may be dropped by the stateful deduplication operator. They will not necessarily reach the late topic. A future enhancement can add a separate raw-event audit path and explicit late-event reconciliation.
- The two streaming queries read the source topic independently and have separate checkpoints. This is deliberate for this learning milestone, but it duplicates source reads.
- Kafka writes inside `foreachBatch` are at-least-once in failure scenarios. A crash after writing output but before checkpoint commit can cause replay. We do not claim end-to-end exactly-once processing.
- The current output is Kafka, not a database. PostgreSQL / ClickHouse sinks are later milestones.
- This is a local development setup, not a production deployment.

## Troubleshooting

**`java` not found / unsupported Java version**: install Java 17 and ensure `JAVA_HOME` points to it.

**Kafka connection refused**: confirm `docker compose ps` shows Kafka healthy and use `localhost:29092` from your Mac (not `kafka:9092`, which is for containers on the Docker network).

**Maven package download fails**: check internet access and retry the same `spark-submit` command. Spark needs `spark-sql-kafka-0-10_2.12:3.5.5` and its dependencies.

**Output topics do not exist**: run the topic creation commands above.

**No output appears**: keep the Spark process running, publish new records, and check its logs. With `--starting-offsets earliest`, a new checkpoint starts from the earliest available source offsets.

## Run the small unit test

From the existing repository root, copy `tests/test_watermark.py` into your existing `tests/` directory, then run:

```bash
pytest -v tests/test_watermark.py
```

This test checks the duration-to-SQL-interval conversion used by the late-event classifier. It does not replace an end-to-end Spark + Kafka integration test.
