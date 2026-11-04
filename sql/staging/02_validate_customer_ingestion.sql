-- ============================================================
-- Project: PostgreSQL RDS ETL Pipeline
-- Purpose: Validate CSV customer ingestion into staging
-- Batch: BATCH_20261103_001
-- ============================================================

SELECT
    customer_id,
    first_name,
    last_name,
    email,
    source_updated_at,
    batch_id
FROM staging.stg_customers
WHERE batch_id = 'BATCH_20261103_001'
ORDER BY customer_id;
