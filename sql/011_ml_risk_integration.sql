
CREATE TABLE IF NOT EXISTS integrated_risk_analysis_runs (
    run_id UUID PRIMARY KEY,

    ml_run_id UUID NOT NULL
        REFERENCES ml_prediction_runs(run_id),

    total_transactions INTEGER NOT NULL DEFAULT 0,
    ml_anomaly_count INTEGER NOT NULL DEFAULT 0,
    adjusted_transaction_count INTEGER NOT NULL DEFAULT 0,

    ml_adjustment_points INTEGER NOT NULL DEFAULT 20,

    started_at TIMESTAMPTZ NOT NULL,
    completed_at TIMESTAMPTZ NOT NULL,

    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    duration_ms BIGINT,

    CHECK (total_transactions >= 0),
    CHECK (ml_anomaly_count >= 0),
    CHECK (adjusted_transaction_count >= 0),
    CHECK (ml_adjustment_points >= 0)
);

ALTER TABLE integrated_risk_analysis_runs
    ADD COLUMN IF NOT EXISTS duration_ms BIGINT;


CREATE TABLE IF NOT EXISTS transaction_integrated_risk_results (
    run_id UUID NOT NULL
        REFERENCES integrated_risk_analysis_runs(run_id),

    transaction_id VARCHAR(100) NOT NULL,

    ml_run_id UUID NOT NULL,

    statistical_risk_score DOUBLE PRECISION NOT NULL,
    statistical_risk_category VARCHAR(20) NOT NULL,

    ml_anomaly_score DOUBLE PRECISION NOT NULL,
    ml_is_anomaly BOOLEAN NOT NULL,
    ml_prediction VARCHAR(20) NOT NULL,

    ml_adjustment_points INTEGER NOT NULL DEFAULT 0,

    integrated_risk_score DOUBLE PRECISION NOT NULL,
    integrated_risk_category VARCHAR(20) NOT NULL,

    integration_reason TEXT NOT NULL,

    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    PRIMARY KEY (run_id, transaction_id),

    FOREIGN KEY (ml_run_id, transaction_id)
        REFERENCES transaction_ml_predictions(run_id, transaction_id),

    CHECK (statistical_risk_score BETWEEN 0 AND 100),
    CHECK (integrated_risk_score BETWEEN 0 AND 100),
    CHECK (ml_adjustment_points >= 0),

    CHECK (
        (ml_is_anomaly = TRUE AND ml_prediction = 'ANOMALY')
        OR
        (ml_is_anomaly = FALSE AND ml_prediction = 'NORMAL')
    )
);


CREATE INDEX IF NOT EXISTS idx_integrated_risk_transaction
ON transaction_integrated_risk_results(transaction_id);


CREATE INDEX IF NOT EXISTS idx_integrated_risk_category
ON transaction_integrated_risk_results(integrated_risk_category);


CREATE INDEX IF NOT EXISTS idx_integrated_risk_run
ON transaction_integrated_risk_results(run_id);


CREATE OR REPLACE VIEW latest_integrated_risk_results AS
SELECT
    run_id,
    ml_run_id,
    transaction_id,
    statistical_risk_score,
    statistical_risk_category,
    ml_anomaly_score,
    ml_is_anomaly,
    ml_prediction,
    ml_adjustment_points,
    integrated_risk_score,
    integrated_risk_category,
    integration_reason,
    created_at
FROM (
    SELECT
        result.*,
        ROW_NUMBER() OVER (
            PARTITION BY result.transaction_id
            ORDER BY run.completed_at DESC, result.created_at DESC
        ) AS row_num
    FROM transaction_integrated_risk_results result
    INNER JOIN integrated_risk_analysis_runs run
        ON result.run_id = run.run_id
) ranked
WHERE row_num = 1;
