# Setup and operations

This guide covers the local development setup. Compose provides Kafka,
PostgreSQL, the Streamlit dashboard, and an optional Airflow profile.

## Prerequisites

- Docker with the Docker Compose plugin
- Python 3.11
- Java 17 and Spark 3.5.5 for the local Spark applications
- Git

The Airflow profile builds a custom Airflow 2.10.5 / Python 3.11 image using
`Dockerfile.airflow`. The dashboard uses `Dockerfile.dashboard`.

## Python environment

From the repository root:

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
pip install -r requirements.txt -r requirements-frontend.txt
```

The frontend requirements add Streamlit, pandas, and Plotly. Airflow ML
dependencies are installed into the custom Airflow image from
`requirements-airflow-ml.txt`; they are kept compatible with the Airflow
constraints file.

## Environment configuration

Create a local, untracked `.env` file. Do not commit credentials. At minimum,
configure the database password values so the application, dashboard, and
Airflow can connect:

```dotenv
POSTGRES_DB=paystream
POSTGRES_USER=paystream
POSTGRES_PASSWORD=replace_with_a_local_password
POSTGRES_PORT=5433

PAYSTREAM_DB_NAME=paystream
PAYSTREAM_DB_USER=paystream
PAYSTREAM_DB_PASSWORD=replace_with_the_same_database_password
# Compose uses these values for Airflow containers.
PAYSTREAM_DB_HOST=postgres
PAYSTREAM_DB_PORT=5432

AIRFLOW_DB_USER=airflow
AIRFLOW_DB_PASSWORD=replace_with_a_local_metadata_password
AIRFLOW_DB_NAME=airflow
AIRFLOW_ADMIN_USERNAME=admin
AIRFLOW_ADMIN_PASSWORD=replace_with_a_local_admin_password
AIRFLOW_WEBSERVER_SECRET_KEY=replace_with_a_long_random_secret
```

Compose networking uses `postgres:5432` and `kafka:9092`. Host-run Python
processes typically use `localhost:5433` and `localhost:29092`. For a local
Python command that reads the Compose `.env`, override the PayStream host and
port for that command:

```bash
PAYSTREAM_DB_HOST=localhost PAYSTREAM_DB_PORT=5433 \
  python -m src.orchestration.ml_dataset
```

The PostgreSQL sink also reads `POSTGRES_HOST`, `POSTGRES_PORT`,
`POSTGRES_DB`, `POSTGRES_USER`, and `POSTGRES_PASSWORD`. Set
`POSTGRES_HOST=localhost` for a sink running on the host.

## Start core services

Start Kafka, PayStream PostgreSQL, and the dashboard:

```bash
docker compose --env-file .env up -d
docker compose --env-file .env ps
```

The default Compose configuration includes the dashboard. It is available at
[http://localhost:8501](http://localhost:8501).

### Create Kafka topics

Create the topics covered by the helper script:

```bash
bash scripts/create_topics.sh
```

Spark also writes to `payment-events-processed` and `payment-events-late`;
create these explicitly because Kafka auto-creation is disabled:

```bash
for topic in payment-events-processed payment-events-late; do
  docker exec paystream-kafka /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server localhost:9092 \
    --create --if-not-exists --topic "$topic" \
    --partitions 3 --replication-factor 1
done
```

### Apply PostgreSQL schema

On the first start with a new PostgreSQL data volume, Compose runs
`sql/001_init.sql` automatically. It does not automatically apply the later
SQL files. Apply migrations 002 through 011 in order:

```bash
set -e
for migration in \
  sql/002_analytics.sql \
  sql/003_pipeline_metrics.sql \
  sql/004_data_quality_monitoring.sql \
  sql/005_pipeline_failures.sql \
  sql/006_risk_analytics.sql \
  sql/007_transaction_anomalies.sql \
  sql/008_transaction_risk_storage.sql \
  sql/009_ml_training_dataset.sql \
  sql/010_ml_prediction_storage.sql \
  sql/011_ml_risk_integration.sql
do
  echo "Applying ${migration}"
  docker compose --env-file .env exec -T postgres \
    sh -c 'psql -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d "$POSTGRES_DB"' \
    < "$migration"
done
```

`-T` disables TTY allocation when feeding SQL through stdin. The scripts use
idempotent object creation where possible, but reapplying `CREATE TABLE IF NOT
EXISTS` will not add columns to a table created by an older schema version.
Inspect existing tables before diagnosing missing-column errors.

### Generate and stream events

Generate synthetic payment events:

```bash
source .venv/bin/activate
python -m src.producer --count 100
```

Optional traffic variation:

```bash
python -m src.producer \
  --count 1000 \
  --rate 20 \
  --duplicate-rate 0.02 \
  --malformed-rate 0.01 \
  --late-rate 0.03 \
  --late-delay-minutes 10
```

Run the Spark validator/router from another terminal:

```bash
PYSPARK_PYTHON=python3 spark-submit \
  --master 'local[*]' \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.5 \
  src/stream_processor.py
```

Inspect Kafka messages with `python -m src.consumer`. To persist processed
events, run the PostgreSQL sink in another terminal after exporting the
`POSTGRES_*` settings from `.env`:

```bash
set -a
source .env
set +a

spark-submit \
  --master 'local[*]' \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.5,org.postgresql:postgresql:42.7.5 \
  src/postgres_sink.py
```

## Start Airflow

Airflow is optional and isolated behind the `airflow` Compose profile. Build
the custom image, initialize the metadata database and user, then start the
webserver and scheduler:

```bash
docker compose --env-file .env --profile airflow build \
  airflow-init airflow-webserver airflow-scheduler

docker compose --env-file .env --profile airflow up \
  airflow-db airflow-init

docker compose --env-file .env --profile airflow up -d \
  airflow-webserver airflow-scheduler
```

Open [http://localhost:8080](http://localhost:8080) and sign in with the
Airflow admin values from `.env`. DAGs and source are mounted into the
containers; model artifacts are shared through `./models`.

Useful commands:

```bash
docker compose --profile airflow ps
docker compose --profile airflow logs -f airflow-webserver airflow-scheduler
docker compose --profile airflow logs airflow-init
```

## Dashboard updates

The dashboard image copies `src/` into the image rather than mounting it as a
live source volume. Rebuild after changing dashboard code:

```bash
docker compose --env-file .env up -d --build dashboard
```

## Tests

Run the full unit suite from the activated virtual environment:

```bash
pytest -q
```

Tests use mocks for most database behavior; a passing unit suite does not
replace applying SQL migrations and checking a live local pipeline.

## Troubleshooting

| Symptom | Check |
|---|---|
| `the input device is not a TTY` while applying SQL | Use `docker compose exec -T` when redirecting a SQL file to `psql`. |
| Database connection refused from a host process | Use `localhost:5433`; use `postgres:5432` only from another Compose container. |
| Missing database column after applying a script | Existing tables are not changed by `CREATE TABLE IF NOT EXISTS`; inspect the table and apply an explicit `ALTER TABLE`. |
| Spark cannot write processed or late events | Confirm `payment-events-processed` and `payment-events-late` exist; the helper script does not create them. |
| Airflow cannot connect to PayStream PostgreSQL | Check the Airflow container's `PAYSTREAM_DB_HOST=postgres`, port `5432`, and matching credentials. |
| Dashboard changes are not visible in Docker | Rebuild the `dashboard` service image. |

Use `docker compose down` to stop services while retaining named volumes. Avoid
`docker compose down -v` unless you intend to delete Kafka and PostgreSQL data.
