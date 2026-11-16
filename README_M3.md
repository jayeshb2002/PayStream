# PayStream — Milestone 3: PostgreSQL Storage Layer

This is an **overlay** for the existing PayStream repository. Keep all Milestone 1 and Milestone 2 files. Add the new files from this folder; do not replace `src/stream_processor.py`.

## What this milestone adds

- PostgreSQL 16 running in Docker as a separate service.
- `payment_events` as the durable, queryable event table.
- `payment_events_staging` as a temporary landing table for Spark micro-batches.
- A Spark Structured Streaming sink that reads `payment-events-processed` from Kafka.
- Idempotent database insertion using `event_id` as the primary key and `ON CONFLICT DO NOTHING`.
- Atomic merge-and-cleanup of each staging batch in one PostgreSQL transaction.
- Useful indexes and two starter analytics views.

## Architecture

```text
Python producer
      |
      v
Kafka: payment-events
      |
      v
Milestone 2 Spark processor
      |
      v
Kafka: payment-events-processed
      |
      v
Milestone 3 Spark PostgreSQL sink
      |
      v
PostgreSQL: payment_events
```

The Milestone 2 processor and Milestone 3 sink are separate Spark applications. Run both at the same time when testing the full pipeline.

## Files to add

Copy these into the existing repository:

```text
docker-compose.m3.yml
.env.m3.example
requirements-m3.txt
sql/001_init.sql
src/postgres_sink.py
tests/test_postgres_sink.py
```

Append the contents of `.gitignore.append` to your existing `.gitignore`.

## 1. Configure PostgreSQL

From the root of the existing PayStream repository:

```bash
cp .env.m3.example .env.m3
```

The supplied password is for local development only. Change it in `.env.m3` if desired. Do not commit `.env.m3`.

Start PostgreSQL alongside the existing Kafka service:

```bash
docker compose --env-file .env.m3 \
  -f docker-compose.yml \
  -f docker-compose.m3.yml \
  up -d postgres
```

Check that it is healthy:

```bash
docker compose --env-file .env.m3 \
  -f docker-compose.yml \
  -f docker-compose.m3.yml \
  ps
```

The SQL initialization script creates the target table, staging table, indexes, and analytics views when the PostgreSQL volume is first initialized.

If the database volume already existed before you added this milestone, run the idempotent SQL script manually:

```bash
docker compose --env-file .env.m3 \
  -f docker-compose.yml \
  -f docker-compose.m3.yml \
  exec -T postgres psql -U paystream -d paystream < sql/001_init.sql
```

If you changed `POSTGRES_USER` or `POSTGRES_DB` in `.env.m3`, use those values instead of `paystream` in the command.

## 2. Install Python dependency

Use the same virtual environment as Milestones 1 and 2:

```bash
source .venv/bin/activate
pip install psycopg2-binary==2.9.10
```

PySpark 3.5.5 should already be installed from Milestone 2. If not:

```bash
pip install pyspark==3.5.5
```

## 3. Load database settings into your terminal

The Spark process runs on your Mac, so it connects to PostgreSQL through `localhost:5432`.

```bash
set -a
source .env.m3
set +a
```

Confirm the database is reachable:

```bash
docker compose --env-file .env.m3 \
  -f docker-compose.yml \
  -f docker-compose.m3.yml \
  exec postgres pg_isready -U "$POSTGRES_USER" -d "$POSTGRES_DB"
```

## 4. Start the Milestone 2 processor

Open Terminal 1. Activate the virtual environment and start Kafka-to-Spark processing:

```bash
source .venv/bin/activate

spark-submit \
  --master 'local[*]' \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.5 \
  src/stream_processor.py
```

Keep this process running. It reads `payment-events` and publishes clean events to `payment-events-processed`.

## 5. Start the Milestone 3 PostgreSQL sink

Open Terminal 2. Activate the same virtual environment, load `.env.m3`, and run:

```bash
cd ~/Documents/Projects/PayStream
source .venv/bin/activate
set -a && source .env.m3 && set +a

spark-submit \
  --master 'local[*]' \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.5,org.postgresql:postgresql:42.7.5 \
  src/postgres_sink.py
```

The first run downloads the PostgreSQL JDBC driver. Java 17 is recommended for this Spark 3.5.5 setup.

## 6. Generate payment traffic

Open Terminal 3 and run the Milestone 1 producer:

```bash
source .venv/bin/activate
python -m src.producer --count 50 --rate 0
```

For a more realistic test, generate duplicates and malformed events too:

```bash
python -m src.producer \
  --count 100 \
  --rate 0 \
  --duplicate-rate 0.10 \
  --malformed-rate 0.05 \
  --late-rate 0.10 \
  --late-delay-minutes 20
```

Only valid, on-time events that Milestone 2 publishes to `payment-events-processed` are consumed by this PostgreSQL sink. Invalid and late events remain in their separate Kafka topics.

## 7. Verify rows in PostgreSQL

```bash
docker compose --env-file .env.m3 \
  -f docker-compose.yml \
  -f docker-compose.m3.yml \
  exec postgres psql -U "$POSTGRES_USER" -d "$POSTGRES_DB"
```

Inside `psql`, run:

```sql
SELECT COUNT(*) FROM payment_events;

SELECT event_id, transaction_id, payment_rail, amount_minor,
       status, event_time
FROM payment_events
ORDER BY stored_at DESC
LIMIT 10;
```

Check the analytics views:

```sql
SELECT * FROM daily_payment_analytics
ORDER BY payment_date_utc DESC, payment_rail, status;

SELECT * FROM bank_payment_analytics
ORDER BY sender_bank, payment_rail, status;
```

Exit `psql` with `\\q`.

## 8. Run tests

```bash
pytest -v tests/test_postgres_sink.py
```

These are unit tests for connection configuration and idempotent merge SQL. They do not replace a live Kafka + Spark + PostgreSQL integration test.

## How duplicate protection works

1. Spark writes each micro-batch to `payment_events_staging` with its Spark batch ID.
2. Python opens a PostgreSQL transaction.
3. PostgreSQL inserts one row per `event_id` into `payment_events`.
4. `ON CONFLICT (event_id) DO NOTHING` ignores an event already stored.
5. The same transaction deletes that batch's staging rows and commits.

If the process fails before the merge transaction, Spark can replay the batch and staging may receive repeated rows. `DISTINCT ON (event_id)` collapses those rows during the merge. If Spark replays an event from an earlier batch, the primary key prevents a second target row.

This gives us idempotent target-table inserts for this sink. It is not a claim of globally exactly-once processing across Kafka, Spark, and PostgreSQL.

## Important notes

- This is a local development architecture, not a production deployment.
- Keep the same Milestone 3 checkpoint directory when restarting normally.
- Do not delete `data/checkpoints/milestone-3/postgres-sink` unless you intentionally want to replay the Kafka topic. The database primary key protects existing event rows during replay.
- The staging table is cleaned after a successful merge. If a merge fails, inspect the Spark error and staging rows before manually deleting anything.
- The database stores event-level lifecycle records. A single transaction can have multiple events such as initiated, status updated, and completed.
- `amount_minor` stores paise for INR. Divide by 100 only for display/reporting; never use floating point for stored money.
- PostgreSQL `TIMESTAMPTZ` stores instants; Spark is configured to use UTC.

## Stop services

Stop Spark with Ctrl+C in both Spark terminals. Stop PostgreSQL and Kafka using the Compose files you normally use. To stop only PostgreSQL:

```bash
docker compose --env-file .env.m3 \
  -f docker-compose.yml \
  -f docker-compose.m3.yml \
  stop postgres
```

Do not run `docker compose down -v` unless you intentionally want to delete the PostgreSQL data volume.
