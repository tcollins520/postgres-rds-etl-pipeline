CREATE SCHEMA IF NOT EXISTS audit;

CREATE TABLE IF NOT EXISTS audit.etl_batch_log (
    execution_id      BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    batch_id          VARCHAR NOT NULL,
    workflow_name     VARCHAR NOT NULL,
    source_system     VARCHAR,
    start_time        TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    end_time          TIMESTAMPTZ,
    records_read      BIGINT NOT NULL DEFAULT 0,
    records_inserted  BIGINT NOT NULL DEFAULT 0,
    records_updated   BIGINT NOT NULL DEFAULT 0,
    records_rejected  BIGINT NOT NULL DEFAULT 0,
    batch_status      VARCHAR NOT NULL DEFAULT 'RUNNING',
    error_message     TEXT,

    CONSTRAINT chk_etl_batch_status
        CHECK (batch_status IN ('RUNNING', 'SUCCESS', 'FAILED'))
);

CREATE INDEX IF NOT EXISTS idx_etl_batch_log_batch_id
ON audit.etl_batch_log (batch_id);
