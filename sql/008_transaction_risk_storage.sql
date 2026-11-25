
-- ============================================================
-- PayStream Milestone 5.4
-- Transaction Risk Results Storage
-- ============================================================

CREATE TABLE IF NOT EXISTS risk_analysis_runs (
    run_id UUID PRIMARY KEY,

    total_transactions BIGINT NOT NULL DEFAULT 0
        CHECK (total_transactions >= 0),

    flagged_transactions BIGINT NOT NULL DEFAULT 0
        CHECK (flagged_transactions >= 0),

    low_risk_count BIGINT NOT NULL DEFAULT 0
        CHECK (low_risk_count >= 0),

    medium_risk_count BIGINT NOT NULL DEFAULT 0
        CHECK (medium_risk_count >= 0),

    high_risk_count BIGINT NOT NULL DEFAULT 0
        CHECK (high_risk_count >= 0),

    critical_risk_count BIGINT NOT NULL DEFAULT 0
        CHECK (critical_risk_count >= 0),

    execution_duration_ms BIGINT NOT NULL DEFAULT 0
        CHECK (execution_duration_ms >= 0),

    status VARCHAR(20) NOT NULL DEFAULT 'COMPLETED'
        CHECK (status IN ('COMPLETED')),

    started_at TIMESTAMPTZ NOT NULL,
    completed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    CONSTRAINT chk_risk_run_flagged_count
        CHECK (flagged_transactions <= total_transactions),

    CONSTRAINT chk_risk_run_category_total
        CHECK (
            low_risk_count
            + medium_risk_count
            + high_risk_count
            + critical_risk_count
            = total_transactions
        )
);

ALTER TABLE risk_analysis_runs
    ADD COLUMN IF NOT EXISTS flagged_transactions BIGINT NOT NULL
    DEFAULT 0
    CHECK (
        flagged_transactions >= 0
        AND flagged_transactions <= total_transactions
    );


CREATE TABLE IF NOT EXISTS transaction_risk_results (
    run_id UUID NOT NULL,

    transaction_id VARCHAR(100) NOT NULL,

    risk_score INTEGER NOT NULL
        CHECK (risk_score BETWEEN 0 AND 100),

    risk_category VARCHAR(20) NOT NULL
        CHECK (
            risk_category IN (
                'LOW',
                'MEDIUM',
                'HIGH',
                'CRITICAL'
            )
        ),

    risk_flags JSONB NOT NULL DEFAULT '[]'::JSONB
        CHECK (jsonb_typeof(risk_flags) = 'array'),

    risk_flag_count INTEGER NOT NULL DEFAULT 0
        CHECK (risk_flag_count >= 0),

    is_flagged BOOLEAN NOT NULL DEFAULT FALSE,

    analyzed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    PRIMARY KEY (run_id, transaction_id),

    CONSTRAINT fk_risk_result_run
        FOREIGN KEY (run_id)
        REFERENCES risk_analysis_runs(run_id)
        ON DELETE CASCADE,

    CONSTRAINT fk_risk_result_transaction
        FOREIGN KEY (transaction_id)
        REFERENCES transaction_features(transaction_id)
        ON DELETE CASCADE
);


-- Indexes for risk analysis queries.

CREATE INDEX IF NOT EXISTS idx_risk_results_transaction
    ON transaction_risk_results (transaction_id);

CREATE INDEX IF NOT EXISTS idx_risk_results_category
    ON transaction_risk_results (risk_category);

CREATE INDEX IF NOT EXISTS idx_risk_results_score
    ON transaction_risk_results (risk_score DESC);

CREATE INDEX IF NOT EXISTS idx_risk_results_run
    ON transaction_risk_results (run_id);

CREATE INDEX IF NOT EXISTS idx_risk_runs_completed
    ON risk_analysis_runs (completed_at DESC);


-- Latest available risk result for every transaction.

CREATE OR REPLACE VIEW latest_transaction_risk AS
SELECT DISTINCT ON (r.transaction_id)
    r.transaction_id,
    r.risk_score,
    r.risk_category,
    r.risk_flags,
    r.risk_flag_count,
    r.is_flagged,
    r.run_id,
    r.analyzed_at

FROM transaction_risk_results r

INNER JOIN risk_analysis_runs a
    ON r.run_id = a.run_id

ORDER BY
    r.transaction_id,
    a.completed_at DESC,
    r.analyzed_at DESC;
