
CREATE TABLE IF NOT EXISTS daily_payment_analytics_snapshot (
    analytics_date DATE NOT NULL,
    payment_rail VARCHAR(20) NOT NULL,

    total_transactions BIGINT NOT NULL DEFAULT 0,
    successful_transactions BIGINT NOT NULL DEFAULT 0,
    failed_transactions BIGINT NOT NULL DEFAULT 0,
    pending_transactions BIGINT NOT NULL DEFAULT 0,

    total_amount_minor BIGINT NOT NULL DEFAULT 0,
    successful_amount_minor BIGINT NOT NULL DEFAULT 0,

    success_rate NUMERIC(5, 2) NOT NULL DEFAULT 0,

    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,

    PRIMARY KEY (analytics_date, payment_rail)
);


CREATE TABLE IF NOT EXISTS bank_payment_analytics_snapshot (
    analytics_date DATE NOT NULL,
    sender_bank VARCHAR(32) NOT NULL,
    payment_rail VARCHAR(20) NOT NULL,

    total_transactions BIGINT NOT NULL DEFAULT 0,
    successful_transactions BIGINT NOT NULL DEFAULT 0,
    failed_transactions BIGINT NOT NULL DEFAULT 0,

    total_amount_minor BIGINT NOT NULL DEFAULT 0,
    successful_amount_minor BIGINT NOT NULL DEFAULT 0,

    success_rate NUMERIC(5, 2) NOT NULL DEFAULT 0,

    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,

    PRIMARY KEY (
        analytics_date,
        sender_bank,
        payment_rail
    )
);


CREATE INDEX IF NOT EXISTS idx_daily_snapshot_date
ON daily_payment_analytics_snapshot (analytics_date);


CREATE INDEX IF NOT EXISTS idx_bank_snapshot_date
ON bank_payment_analytics_snapshot (analytics_date);


CREATE INDEX IF NOT EXISTS idx_bank_snapshot_sender
ON bank_payment_analytics_snapshot (sender_bank);
