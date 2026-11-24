
CREATE TABLE IF NOT EXISTS pipeline_metrics (
    id BIGSERIAL PRIMARY KEY,

    component VARCHAR(100) NOT NULL,
    batch_id BIGINT NOT NULL,

    metric_name VARCHAR(100) NOT NULL,
    metric_value BIGINT NOT NULL,

    processing_duration_ms BIGINT,

    recorded_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT uq_pipeline_metric
        UNIQUE (component, batch_id, metric_name)
);

CREATE INDEX IF NOT EXISTS idx_pipeline_metrics_recorded_at
    ON pipeline_metrics (recorded_at DESC);

CREATE INDEX IF NOT EXISTS idx_pipeline_metrics_component
    ON pipeline_metrics (component, metric_name);
