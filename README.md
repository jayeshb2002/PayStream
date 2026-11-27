# PayStream

PayStream is a local payment-event streaming project. It demonstrates Kafka
ingestion, Spark validation and routing, PostgreSQL persistence and analytics,
Airflow orchestration, transaction risk scoring, and an unsupervised ML anomaly
detection workflow with a Streamlit dashboard.

## Architecture at a glance

```text
Python event producer
        |
        v
Kafka: payment-events
        |
        v
Spark Structured Streaming
   |          |          |
processed    DLQ       late events
   |
   v
PostgreSQL payment_events
   |              |
   v              v
Analytics       Risk and ML workflows
                    |
                    v
             Streamlit dashboard

Airflow schedules analytics, monitoring, risk, and ML workflows.
```

Core technologies: Python 3.11, Kafka 3.9, Spark 3.5, PostgreSQL 16, Airflow
2.10, and Streamlit.

## Quick start

Prerequisites and the full local setup are in
[Setup and operations](docs/operations.md).

1. Create a local `.env` file using the variable names documented in the
   operations guide. Do not commit credentials.
2. Create and activate a Python environment, then install dependencies:

   ```bash
   python3 -m venv .venv
   source .venv/bin/activate
   pip install -r requirements.txt -r requirements-frontend.txt
   ```

3. Start the default Compose services:

   ```bash
   docker compose --env-file .env up -d
   ```

   This starts Kafka, PayStream PostgreSQL, and the dashboard. The dashboard
   is served at [http://localhost:8501](http://localhost:8501).

4. Create Kafka topics and apply database migrations 002–011. The complete
   commands and ordering are in [Setup and operations](docs/operations.md).
5. Generate a small sample of payment events:

   ```bash
   python -m src.producer --count 100
   ```

Airflow is optional and starts under its Compose profile; see the operations
guide.

## Documentation

- [Architecture](docs/architecture.md) — system components, event flow,
  topics, database layers, and DAG schedules.
- [Setup and operations](docs/operations.md) — environment, Compose services,
  SQL migrations, Kafka topics, local processing, Airflow, and troubleshooting.
- [ML pipeline](docs/ml-pipeline.md) — dataset, model, evaluation, prediction,
  integration, artifacts, and focused tests.
- [Milestone ledger](MILESTONES.md) — implementation checklist and progress
  notes.

## Tests

Run the unit suite from the activated environment:

```bash
pytest -q
```

Most database tests use mocks. For live validation, apply the SQL migrations
and follow the service-specific checks in the operations guide.

## Project layout

| Path | Contents |
|---|---|
| `src/producer.py`, `src/generator.py` | Synthetic payment event generation and publishing |
| `src/stream_processor.py` | Spark validation, deduplication, watermarking, and routing |
| `src/postgres_sink.py` | PostgreSQL staging and idempotent merge |
| `src/orchestration/` | Analytics, monitoring, risk, and ML workflows |
| `dags/` | Airflow DAG definitions |
| `src/dashboard/` | Streamlit dashboards and PostgreSQL query helpers |
| `sql/` | Ordered database schema scripts |
| `schemas/` | Payment event JSON Schema |
| `tests/` | Unit and workflow tests |

This repository is intended for local development and demonstration, not
production deployment.
