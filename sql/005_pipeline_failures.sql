
CREATE TABLE IF NOT EXISTS pipeline_failures (
    failure_id UUID PRIMARY KEY,
    failure_key VARCHAR(64) NOT NULL UNIQUE,

    component VARCHAR(100) NOT NULL,
    failure_type VARCHAR(150) NOT NULL,
    severity VARCHAR(20) NOT NULL,

    error_message TEXT NOT NULL,
    stack_trace TEXT,

    context JSONB NOT NULL DEFAULT '{}'::jsonb,

    status VARCHAR(20) NOT NULL DEFAULT 'OPEN',
    occurrence_count BIGINT NOT NULL DEFAULT 1,

    first_seen TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    last_seen TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    resolved_at TIMESTAMPTZ,

    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT chk_pipeline_failure_severity
        CHECK (severity IN ('LOW', 'MEDIUM', 'HIGH', 'CRITICAL')),

    CONSTRAINT chk_pipeline_failure_status
        CHECK (status IN ('OPEN', 'RESOLVED'))
);

CREATE INDEX IF NOT EXISTS idx_pipeline_failures_component
    ON pipeline_failures (component);

CREATE INDEX IF NOT EXISTS idx_pipeline_failures_status
    ON pipeline_failures (status, last_seen DESC);

CREATE INDEX IF NOT EXISTS idx_pipeline_failures_severity
    ON pipeline_failures (severity, last_seen DESC);

CREATE INDEX IF NOT EXISTS idx_pipeline_failures_last_seen
    ON pipeline_failures (last_seen DESC);
