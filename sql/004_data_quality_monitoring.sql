
CREATE TABLE IF NOT EXISTS data_quality_runs (
    run_id UUID PRIMARY KEY,
    total_records BIGINT NOT NULL,
    total_checks INTEGER NOT NULL,
    passed_checks INTEGER NOT NULL,
    failed_checks INTEGER NOT NULL,
    quality_score NUMERIC(5, 2) NOT NULL,
    threshold_score NUMERIC(5, 2) NOT NULL,
    status VARCHAR(20) NOT NULL,
    execution_duration_ms BIGINT NOT NULL,
    started_at TIMESTAMPTZ NOT NULL,
    completed_at TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT chk_quality_run_status
        CHECK (status IN ('PASSED', 'WARNING', 'FAILED')),

    CONSTRAINT chk_quality_score
        CHECK (quality_score BETWEEN 0 AND 100)
);

CREATE TABLE IF NOT EXISTS data_quality_check_results (
    id BIGSERIAL PRIMARY KEY,
    run_id UUID NOT NULL
        REFERENCES data_quality_runs(run_id) ON DELETE CASCADE,
    check_name VARCHAR(150) NOT NULL,
    failed_records BIGINT NOT NULL,
    status VARCHAR(20) NOT NULL,
    recorded_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT chk_quality_check_status
        CHECK (status IN ('PASSED', 'FAILED')),

    CONSTRAINT uq_quality_run_check
        UNIQUE (run_id, check_name)
);

CREATE INDEX IF NOT EXISTS idx_quality_runs_created_at
    ON data_quality_runs (created_at DESC);

CREATE INDEX IF NOT EXISTS idx_quality_runs_status
    ON data_quality_runs (status, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_quality_results_run_id
    ON data_quality_check_results (run_id);

CREATE INDEX IF NOT EXISTS idx_quality_results_check_name
    ON data_quality_check_results (check_name, recorded_at DESC);
