# Machine-learning pipeline

PayStream's Milestone 6 workflow uses an Isolation Forest to flag anomalous
transactions. It is an unsupervised anomaly detector, not a supervised fraud
classifier.

## Workflow

The scheduled DAG `paystream_m6_7_ml_pipeline` runs daily at 02:00 UTC. Its
tasks run sequentially:

1. `prepare_ml_dataset` reads transaction features and statistical anomaly
   context into `transaction_ml_dataset`.
2. `train_isolation_forest` prepares model features and saves the trained
   pipeline and metadata.
3. `evaluate_ml_model` evaluates anomaly scores and selects the configured
   percentile threshold (95th percentile in the DAG).
4. `generate_and_persist_predictions` predicts one result per transaction and
   stores the run and transaction-level predictions.
5. `integrate_ml_risk` joins the latest ML prediction run with statistical
   risk results and stores integrated scores.

The DAG retries tasks twice, does not catch up missed scheduled runs, and
allows one active DAG run. Statistical risk data must already exist for
integration; the Milestone 5 risk workflow produces that upstream data.

## Features and model behavior

The training feature set is defined in `src/orchestration/ml_model.py`:

| Type | Features |
|---|---|
| Numeric | `amount_minor`, `event_count`, `distinct_event_type_count`, `failed_event_count`, `lifecycle_duration_seconds` |
| Categorical | `payment_rail`, `currency`, `latest_status` |

Statistical anomaly fields are retained in the dataset for comparison but are
excluded from model inputs. The model requires at least 20 training rows and
uses an Isolation Forest with 200 estimators and a fixed random seed.

Isolation Forest's `decision_function` is lower for more anomalous records.
Prediction code negates the score so **higher anomaly scores indicate greater
anomaly**. A transaction is classified as `ANOMALY` when its score meets or
exceeds the stored threshold.

Evaluation considers 90th, 95th, 97.5th, and 99th percentile candidates. It
reports alert rates and comparison with statistical anomaly results. The
current workflow has no labelled fraud ground truth; statistical overlap is
not model accuracy, fraud probability, or proof of fraud.

## Data and artifacts

The database objects are created by the numbered migrations:

- `transaction_ml_dataset` — model features plus statistical comparison
  columns (`sql/009_ml_training_dataset.sql`)
- `ml_prediction_runs` — run metadata, threshold, counts, anomaly-rate
  percentage, duration, and status
- `transaction_ml_predictions` — per-transaction score and classification
- `latest_ml_predictions` — latest completed prediction for each transaction
  (`sql/010_ml_prediction_storage.sql`)
- `integrated_risk_analysis_runs` and
  `transaction_integrated_risk_results` — integrated run and transaction
  history
- `latest_integrated_risk_results` — latest integrated result per transaction
  (`sql/011_ml_risk_integration.sql`)

By default, local artifacts are written beneath `models/`:

| Artifact | Purpose |
|---|---|
| `isolation_forest.joblib` | Trained preprocessing and Isolation Forest pipeline |
| `isolation_forest_metadata.json` | Training configuration, feature lists, and summary |
| `ml_evaluation_report.json` | Evaluation statistics and candidate thresholds |
| `ml_threshold.json` | Selected threshold and score direction |

The `models/` directory is ignored by Git and mounted into Airflow containers.
Create it before starting the workflow if it does not exist:

```bash
mkdir -p models
```

## Run manually

Apply migrations through `011_ml_risk_integration.sql` first. For a local
Python process, use the host database address (`localhost:5433`) if the
`.env` file contains the Compose address (`postgres:5432`).

The statistical feature and risk results must exist before integration. A
manual end-to-end sequence is:

```bash
PAYSTREAM_DB_HOST=localhost PAYSTREAM_DB_PORT=5433 \
  python -m src.orchestration.transaction_features

PAYSTREAM_DB_HOST=localhost PAYSTREAM_DB_PORT=5433 \
  python -m src.orchestration.anomaly_detection

PAYSTREAM_DB_HOST=localhost PAYSTREAM_DB_PORT=5433 \
  python -m src.orchestration.risk_scoring

PAYSTREAM_DB_HOST=localhost PAYSTREAM_DB_PORT=5433 \
  python -m src.orchestration.ml_dataset

PAYSTREAM_DB_HOST=localhost PAYSTREAM_DB_PORT=5433 \
  python -m src.orchestration.ml_model

PAYSTREAM_DB_HOST=localhost PAYSTREAM_DB_PORT=5433 \
  python -m src.orchestration.ml_evaluation --target-percentile 95

PAYSTREAM_DB_HOST=localhost PAYSTREAM_DB_PORT=5433 \
  python -m src.orchestration.ml_prediction_storage

PAYSTREAM_DB_HOST=localhost PAYSTREAM_DB_PORT=5433 \
  python -m src.orchestration.ml_risk_integration \
    --ml-adjustment-points 20
```

These modules also require the database name, user, and password environment
variables documented in [operations.md](operations.md). The inline host/port
overrides do not replace those credentials.

## Dashboard

The existing Streamlit dashboard includes ML run KPIs, anomaly distributions,
score visualization, high-scoring transactions, integrated risk comparisons,
run history, and transaction inspection. It reads prediction data from
`latest_ml_predictions` and integrated data from
`latest_integrated_risk_results`.

If the dashboard reports a missing-column error, compare the query's selected
columns with the installed SQL migration. Existing database tables may require
an explicit `ALTER TABLE`; rebuild the dashboard image after changing its
Python source.

## Focused tests

```bash
pytest -q tests/test_ml_dataset.py \
  tests/test_ml_model.py \
  tests/test_ml_evaluation.py \
  tests/test_ml_prediction.py \
  tests/test_ml_prediction_storage.py \
  tests/test_ml_risk_integration.py \
  tests/test_ml_dashboard.py
```
