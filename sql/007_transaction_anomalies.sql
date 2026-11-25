
CREATE TABLE IF NOT EXISTS transaction_anomaly_results (
    transaction_id VARCHAR(100) PRIMARY KEY,

    payment_rail VARCHAR(20) NOT NULL,
    currency CHAR(3) NOT NULL,
    amount_minor BIGINT NOT NULL,

    baseline_transaction_count INTEGER NOT NULL,

    baseline_median_minor NUMERIC(20, 2),
    baseline_mad_minor NUMERIC(20, 2),
    baseline_p95_minor NUMERIC(20, 2),

    robust_z_score NUMERIC(18, 4),

    is_robust_z_anomaly BOOLEAN NOT NULL DEFAULT FALSE,
    is_percentile_anomaly BOOLEAN NOT NULL DEFAULT FALSE,
    is_anomaly BOOLEAN NOT NULL DEFAULT FALSE,

    baseline_status VARCHAR(30) NOT NULL,

    anomaly_reasons TEXT[] NOT NULL DEFAULT '{}',

    analyzed_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT fk_anomaly_transaction
        FOREIGN KEY (transaction_id)
        REFERENCES transaction_features(transaction_id)
        ON DELETE CASCADE,

    CONSTRAINT chk_baseline_count
        CHECK (baseline_transaction_count >= 1),

    CONSTRAINT chk_baseline_status
        CHECK (baseline_status IN ('READY', 'INSUFFICIENT_DATA'))
);

CREATE INDEX IF NOT EXISTS idx_anomaly_results_flag
    ON transaction_anomaly_results (is_anomaly);

CREATE INDEX IF NOT EXISTS idx_anomaly_results_rail_currency
    ON transaction_anomaly_results (payment_rail, currency);

CREATE INDEX IF NOT EXISTS idx_anomaly_results_analyzed_at
    ON transaction_anomaly_results (analyzed_at DESC);

CREATE INDEX IF NOT EXISTS idx_anomaly_results_z_score
    ON transaction_anomaly_results (robust_z_score DESC);
