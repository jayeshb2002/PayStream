
-- =========================================================
-- PayStream Milestone 5: Risk Analytics
-- =========================================================

-- 1. Transaction-level feature table

CREATE TABLE IF NOT EXISTS transaction_features (
    transaction_id VARCHAR(100) PRIMARY KEY,

    payment_rail VARCHAR(20) NOT NULL,
    amount_minor BIGINT NOT NULL,
    currency CHAR(3) NOT NULL,

    sender_bank VARCHAR(100),
    receiver_bank VARCHAR(100),

    first_event_time TIMESTAMPTZ NOT NULL,
    last_event_time TIMESTAMPTZ NOT NULL,

    latest_status VARCHAR(30) NOT NULL,

    event_count INTEGER NOT NULL DEFAULT 0,
    distinct_event_types INTEGER NOT NULL DEFAULT 0,
    failed_event_count INTEGER NOT NULL DEFAULT 0,

    lifecycle_duration_seconds BIGINT NOT NULL DEFAULT 0,

    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_transaction_features_rail
ON transaction_features(payment_rail);

CREATE INDEX IF NOT EXISTS idx_transaction_features_amount
ON transaction_features(amount_minor DESC);

CREATE INDEX IF NOT EXISTS idx_transaction_features_status
ON transaction_features(latest_status);


-- 2. Risk analysis execution history

CREATE TABLE IF NOT EXISTS risk_analysis_runs (
    run_id UUID PRIMARY KEY,

    total_transactions BIGINT NOT NULL DEFAULT 0,
    analysed_transactions BIGINT NOT NULL DEFAULT 0,

    low_risk_count BIGINT NOT NULL DEFAULT 0,
    medium_risk_count BIGINT NOT NULL DEFAULT 0,
    high_risk_count BIGINT NOT NULL DEFAULT 0,
    critical_risk_count BIGINT NOT NULL DEFAULT 0,

    anomaly_count BIGINT NOT NULL DEFAULT 0,

    status VARCHAR(20) NOT NULL DEFAULT 'RUNNING',

    execution_duration_ms BIGINT,

    started_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    completed_at TIMESTAMPTZ,

    CONSTRAINT chk_risk_run_status
        CHECK (status IN ('RUNNING', 'COMPLETED', 'FAILED'))
);

CREATE INDEX IF NOT EXISTS idx_risk_runs_started_at
ON risk_analysis_runs(started_at DESC);


-- 3. Transaction risk scores

CREATE TABLE IF NOT EXISTS transaction_risk_scores (
    run_id UUID NOT NULL REFERENCES risk_analysis_runs(run_id),

    transaction_id VARCHAR(100) NOT NULL,

    risk_score NUMERIC(5, 2) NOT NULL,
    risk_category VARCHAR(20) NOT NULL,

    is_anomaly BOOLEAN NOT NULL DEFAULT FALSE,

    robust_z_score NUMERIC(12, 4),

    risk_flags TEXT[] NOT NULL DEFAULT '{}',

    explanation TEXT,

    amount_minor BIGINT NOT NULL,
    payment_rail VARCHAR(20) NOT NULL,

    latest_status VARCHAR(30) NOT NULL,

    analysed_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,

    PRIMARY KEY (run_id, transaction_id),

    CONSTRAINT chk_risk_score
        CHECK (risk_score BETWEEN 0 AND 100),

    CONSTRAINT chk_risk_category
        CHECK (
            risk_category IN (
                'LOW',
                'MEDIUM',
                'HIGH',
                'CRITICAL'
            )
        )
);

CREATE INDEX IF NOT EXISTS idx_risk_scores_transaction
ON transaction_risk_scores(transaction_id);

CREATE INDEX IF NOT EXISTS idx_risk_scores_category
ON transaction_risk_scores(risk_category);

CREATE INDEX IF NOT EXISTS idx_risk_scores_anomaly
ON transaction_risk_scores(is_anomaly);

CREATE INDEX IF NOT EXISTS idx_risk_scores_run
ON transaction_risk_scores(run_id);
