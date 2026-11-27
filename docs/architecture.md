# Architecture

PayStream is a local payment-event pipeline that combines Kafka, Spark
Structured Streaming, PostgreSQL, Apache Airflow, and a Streamlit dashboard.
It includes event validation, operational monitoring, statistical risk
analysis, and an unsupervised ML anomaly-detection workflow.

## Data flow

```text
Synthetic event generator
          |
          v
Kafka: payment-events
          |
          v
Spark Structured Streaming
   |          |          |
   v          v          v
processed    DLQ       late events
   |
   v
PostgreSQL staging -> transactional merge -> payment_events
                                              |
                  +---------------------------+---------------------+
                  |                           |                     |
                  v                           v                     v
          Analytics snapshots       Statistical risk       ML dataset,
             and monitors           and risk history       predictions,
                                                               integration
                  \                           |                     /
                   \                          v                    /
                    +---------------- Streamlit dashboard --------+

Airflow schedules analytics, monitoring, risk, and ML workflows.
```

## Runtime components

| Component | Responsibility | Main implementation |
|---|---|---|
| Event generator and producer | Generate synthetic payment lifecycle events and publish them to Kafka | `src/generator.py`, `src/producer.py` |
| Event contract | Validate event fields, enum values, amounts, and timezone-aware timestamps | `src/models.py`, `schemas/payment_event.schema.json` |
| Kafka | Buffer and route payment events between pipeline stages | `docker-compose.yml` |
| Stream processor | Parse, validate, deduplicate, watermark, and route events | `src/stream_processor.py` |
| PostgreSQL sink | Stage processed events and merge them idempotently into PostgreSQL | `src/postgres_sink.py` |
| Orchestration | Schedule analytics, monitoring, risk, and ML tasks | `dags/`, `src/orchestration/` |
| Dashboard | Present pipeline, quality, failure, statistical-risk, and ML information | `src/dashboard/` |

The default Compose services are Kafka, PayStream PostgreSQL, and the
dashboard. Airflow and its separate metadata PostgreSQL database are enabled
with the `airflow` Compose profile.

## Event contract

Each payment event has a unique `event_id` and a `transaction_id` shared by
events in one payment lifecycle. Amounts are integer minor units (`amount_minor`;
for INR, paise). The event contains a payment rail, status, event type,
sender/receiver bank, event and ingestion timestamps, producer ID, sequence
number, and schema version.

`PaymentEvent` rejects unknown fields, invalid enum values, non-positive
amounts, and timezone-naive timestamps. Kafka message keys use the transaction
ID so events for a lifecycle share a partition key.

## Kafka topics

| Topic | Purpose |
|---|---|
| `payment-events` | Producer input and Spark source |
| `payment-events-processed` | Valid, deduplicated events consumed by the PostgreSQL sink |
| `payment-events-dlq` | Invalid or unrecoverable events with validation context |
| `payment-events-late` | Events arriving after the Spark watermark |
| `payment-events-retry` | Reserved for retry workflows |
| `payment-events-audit` | Reserved for audit workflows |

Kafka topic auto-creation is disabled in Compose. The topic setup script creates
the source, retry, DLQ, and audit topics; the processed and late topics must
also be created before running Spark. See [operations.md](operations.md).

## PostgreSQL data layers

The numbered SQL files are applied to the PayStream database. `001_init.sql`
is mounted as a PostgreSQL initialization script and runs automatically only
when the data directory is first initialized. Later migrations are applied
manually in numerical order.

| Migration | Main objects |
|---|---|
| `001_init.sql` | `payment_events`, staging table, daily and bank analytics views |
| `002_analytics.sql` | Daily and bank-wise analytics snapshot tables |
| `003_pipeline_metrics.sql` | `pipeline_metrics` |
| `004_data_quality_monitoring.sql` | Data-quality run and check-result history |
| `005_pipeline_failures.sql` | Pipeline failure and incident history |
| `006_risk_analytics.sql` | Transaction features and initial statistical-risk structures |
| `007_transaction_anomalies.sql` | Statistical anomaly results |
| `008_transaction_risk_storage.sql` | Persisted risk runs/results and `latest_transaction_risk` |
| `009_ml_training_dataset.sql` | `transaction_ml_dataset` |
| `010_ml_prediction_storage.sql` | ML prediction runs/results and `latest_ml_predictions` |
| `011_ml_risk_integration.sql` | Integrated risk runs/results and `latest_integrated_risk_results` |

The numbered scripts are schema definitions, not a migration-versioning
framework. `CREATE TABLE IF NOT EXISTS` does not reconcile an already existing
table with a changed definition. When upgrading an existing database, inspect
its columns and apply any required `ALTER TABLE` statements.

## Airflow workflows

Airflow uses its own PostgreSQL metadata database. PayStream application data
remains in the `postgres` service. Current scheduled DAGs include:

| DAG | Schedule | Purpose |
|---|---|---|
| `paystream_m4_3_analytics` | Daily at 01:15 UTC | Refresh daily and bank analytics snapshots |
| `paystream_m4_4_health_monitor` | Every 5 minutes | Check Kafka, PayStream PostgreSQL, and Airflow metadata health |
| `paystream_m4_4_metrics` | Hourly | Report recent pipeline metrics |
| `paystream_m4_4_quality_monitor` | Every 15 minutes | Persist quality checks and enforce the configured threshold |
| `paystream_m4_4_failure_monitor` | Every 5 minutes | Discover and record Airflow task failures |
| `paystream_m5_risk_analysis` | Hourly | Run the earlier feature and risk-analysis workflow |
| `paystream_m5_5_automated_risk_analysis` | Every 30 minutes | Build features, detect statistical anomalies, and score risk |
| `paystream_m6_7_ml_pipeline` | Daily at 02:00 UTC | Prepare, train, evaluate, predict, and integrate ML risk |

The database connection checks, data-quality check, and smoke-test DAGs are
manually triggered. See [operations.md](operations.md) for startup instructions
and [ml-pipeline.md](ml-pipeline.md) for ML dependencies and artifacts.

## Reliability and limitations

- Spark output is at-least-once across retries; `event_id` and PostgreSQL
  conflict handling provide downstream duplicate protection.
- `amount_minor` is integral to avoid floating-point currency rounding.
- The ML model is unsupervised. Statistical anomaly results are comparison
  signals, not ground-truth fraud labels or a measure of fraud accuracy.
- This Compose topology is for local development and demonstrations, not
  production deployment.
