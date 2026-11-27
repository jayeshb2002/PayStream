
-- Tracks each execution of the ML prediction pipeline.

CREATE TABLE IF NOT EXISTS ml_prediction_runs (
    run_id UUID PRIMARY KEY,

    model_name VARCHAR(150) NOT NULL,

    threshold DOUBLE PRECISION NOT NULL,
    percentile DOUBLE PRECISION NOT NULL,

    total_transactions INTEGER NOT NULL DEFAULT 0,
    anomaly_count INTEGER NOT NULL DEFAULT 0,
    normal_count INTEGER NOT NULL DEFAULT 0,

    anomaly_rate_percent DOUBLE PRECISION NOT NULL DEFAULT 0,

    duration_ms BIGINT,

    status VARCHAR(20) NOT NULL DEFAULT 'COMPLETED'
        CHECK (status IN ('COMPLETED', 'FAILED')),

    started_at TIMESTAMPTZ NOT NULL,
    completed_at TIMESTAMPTZ NOT NULL,

    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    CHECK (threshold >= '-Infinity'::DOUBLE PRECISION),
    CHECK (percentile > 0 AND percentile < 100),
    CHECK (total_transactions >= 0),
    CHECK (anomaly_count >= 0),
    CHECK (normal_count >= 0),
    CHECK (anomaly_count + normal_count = total_transactions)
);


-- Stores predictions generated for individual transactions.
-- Multiple runs can contain predictions for the same transaction.

CREATE TABLE IF NOT EXISTS transaction_ml_predictions (
    run_id UUID NOT NULL
        REFERENCES ml_prediction_runs(run_id),

    transaction_id VARCHAR(100) NOT NULL
        REFERENCES transaction_ml_dataset(transaction_id),

    ml_anomaly_score DOUBLE PRECISION NOT NULL,

    ml_is_anomaly BOOLEAN NOT NULL,

    ml_prediction VARCHAR(20) NOT NULL
        CHECK (ml_prediction IN ('NORMAL', 'ANOMALY')),

    ml_threshold DOUBLE PRECISION NOT NULL,

    model_name VARCHAR(150) NOT NULL,

    model_percentile DOUBLE PRECISION NOT NULL,

    predicted_at TIMESTAMPTZ NOT NULL,

    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    PRIMARY KEY (run_id, transaction_id),

    CHECK (
        (ml_is_anomaly = TRUE AND ml_prediction = 'ANOMALY')
        OR
        (ml_is_anomaly = FALSE AND ml_prediction = 'NORMAL')
    ),

    CHECK (model_percentile > 0 AND model_percentile < 100)
);


CREATE INDEX IF NOT EXISTS idx_ml_predictions_transaction
ON transaction_ml_predictions(transaction_id);


CREATE INDEX IF NOT EXISTS idx_ml_predictions_run
ON transaction_ml_predictions(run_id);


CREATE INDEX IF NOT EXISTS idx_ml_predictions_anomaly
ON transaction_ml_predictions(ml_is_anomaly);


CREATE INDEX IF NOT EXISTS idx_ml_prediction_runs_completed
ON ml_prediction_runs(completed_at DESC);


-- Returns the latest prediction for every transaction.

CREATE OR REPLACE VIEW latest_ml_predictions AS
SELECT
    run_id,
    transaction_id,
    ml_anomaly_score,
    ml_is_anomaly,
    ml_prediction,
    ml_threshold,
    model_name,
    model_percentile,
    predicted_at
FROM (
    SELECT
        p.*,
        ROW_NUMBER() OVER (
            PARTITION BY p.transaction_id
            ORDER BY
                r.completed_at DESC,
                p.predicted_at DESC,
                p.run_id DESC
        ) AS row_num
    FROM transaction_ml_predictions p
    INNER JOIN ml_prediction_runs r
        ON p.run_id = r.run_id
    WHERE r.status = 'COMPLETED'
) ranked
WHERE row_num = 1;
