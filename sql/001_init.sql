CREATE TABLE IF NOT EXISTS payment_events (
    event_id        VARCHAR(100) PRIMARY KEY,
    transaction_id  VARCHAR(100) NOT NULL,
    event_type      VARCHAR(50) NOT NULL,
    payment_rail    VARCHAR(20) NOT NULL,
    amount_minor    BIGINT NOT NULL CHECK (amount_minor >= 0),
    currency        CHAR(3) NOT NULL,
    sender_bank     VARCHAR(100),
    receiver_bank   VARCHAR(100),
    status          VARCHAR(30) NOT NULL,
    event_time      TIMESTAMPTZ NOT NULL,
    ingestion_time  TIMESTAMPTZ NOT NULL,
    producer_id     VARCHAR(100) NOT NULL,
    sequence_number INTEGER NOT NULL CHECK (sequence_number >= 0),
    schema_version  INTEGER NOT NULL CHECK (schema_version >= 1),
    source_batch_id BIGINT NOT NULL,
    stored_at       TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Spark first writes to this staging table. The merge into payment_events and
-- deletion from staging happen in one PostgreSQL transaction.
CREATE TABLE IF NOT EXISTS payment_events_staging (
    event_id        VARCHAR(100) NOT NULL,
    transaction_id  VARCHAR(100) NOT NULL,
    event_type      VARCHAR(50) NOT NULL,
    payment_rail    VARCHAR(20) NOT NULL,
    amount_minor    BIGINT NOT NULL,
    currency        CHAR(3) NOT NULL,
    sender_bank     VARCHAR(100),
    receiver_bank   VARCHAR(100),
    status          VARCHAR(30) NOT NULL,
    event_time      TIMESTAMPTZ NOT NULL,
    ingestion_time  TIMESTAMPTZ NOT NULL,
    producer_id     VARCHAR(100) NOT NULL,
    sequence_number INTEGER NOT NULL,
    schema_version  INTEGER NOT NULL,
    spark_batch_id  BIGINT NOT NULL,
    sink_received_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_payment_events_event_time
    ON payment_events (event_time);
CREATE INDEX IF NOT EXISTS idx_payment_events_transaction_id
    ON payment_events (transaction_id);
CREATE INDEX IF NOT EXISTS idx_payment_events_rail_status
    ON payment_events (payment_rail, status);
CREATE INDEX IF NOT EXISTS idx_payment_events_sender_bank
    ON payment_events (sender_bank);
CREATE INDEX IF NOT EXISTS idx_payment_events_receiver_bank
    ON payment_events (receiver_bank);
CREATE INDEX IF NOT EXISTS idx_payment_events_staging_batch
    ON payment_events_staging (spark_batch_id);

CREATE OR REPLACE VIEW daily_payment_analytics AS
SELECT
    DATE_TRUNC('day', event_time AT TIME ZONE 'UTC')::date AS payment_date_utc,
    payment_rail,
    status,
    COUNT(*) AS event_count,
    SUM(amount_minor) AS total_amount_minor,
    ROUND(SUM(amount_minor)::numeric / 100, 2) AS total_amount
FROM payment_events
GROUP BY 1, 2, 3;

CREATE OR REPLACE VIEW bank_payment_analytics AS
SELECT
    sender_bank,
    payment_rail,
    status,
    COUNT(*) AS event_count,
    SUM(amount_minor) AS total_amount_minor,
    ROUND(SUM(amount_minor)::numeric / 100, 2) AS total_amount
FROM payment_events
GROUP BY sender_bank, payment_rail, status;
