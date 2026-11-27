
CREATE TABLE IF NOT EXISTS transaction_ml_dataset (
    transaction_id VARCHAR(100) PRIMARY KEY,

    -- Categorical features
    payment_rail VARCHAR(20) NOT NULL,
    currency CHAR(3) NOT NULL,
    latest_status VARCHAR(30),

    -- Numeric ML features
    amount_minor BIGINT NOT NULL,
    event_count INTEGER NOT NULL,
    distinct_event_type_count INTEGER NOT NULL,
    failed_event_count INTEGER NOT NULL,
    lifecycle_duration_seconds DOUBLE PRECISION,

    -- Statistical anomaly results for comparison only.
    -- Do not pass these columns into Isolation Forest.
    statistical_is_anomaly BOOLEAN,
    statistical_robust_z_score DOUBLE PRECISION,
    statistical_baseline_status VARCHAR(30),
    statistical_anomaly_reasons TEXT[],

    source_feature_updated_at TIMESTAMPTZ,
    dataset_updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    CONSTRAINT fk_ml_dataset_transaction
        FOREIGN KEY (transaction_id)
        REFERENCES transaction_features(transaction_id)
        ON DELETE CASCADE,

    CONSTRAINT chk_ml_amount
        CHECK (amount_minor >= 0),

    CONSTRAINT chk_ml_event_count
        CHECK (event_count >= 0),

    CONSTRAINT chk_ml_distinct_event_count
        CHECK (distinct_event_type_count >= 0),

    CONSTRAINT chk_ml_failed_event_count
        CHECK (failed_event_count >= 0)
);

CREATE INDEX IF NOT EXISTS idx_ml_dataset_payment_rail
    ON transaction_ml_dataset(payment_rail);

CREATE INDEX IF NOT EXISTS idx_ml_dataset_currency
    ON transaction_ml_dataset(currency);

CREATE INDEX IF NOT EXISTS idx_ml_dataset_updated_at
    ON transaction_ml_dataset(dataset_updated_at DESC);

CREATE INDEX IF NOT EXISTS idx_ml_dataset_statistical_anomaly
    ON transaction_ml_dataset(statistical_is_anomaly);
