-- ============================================================
-- Project: PostgreSQL RDS ETL Pipeline
-- Object: staging.stg_customers
-- Purpose: Store customer records extracted from source systems
-- ============================================================

-- Create staging customer table

CREATE TABLE staging.stg_customers (
    customer_id       VARCHAR(50) NOT NULL,
    first_name        VARCHAR(100),
    last_name         VARCHAR(100),
    email             VARCHAR(255),
    phone             VARCHAR(30),
    address           VARCHAR(255),
    city              VARCHAR(100),
    state             VARCHAR(100),
    postal_code       VARCHAR(20),
    country           VARCHAR(100),
    customer_segment  VARCHAR(50),
    source_system     VARCHAR(100) NOT NULL,
    batch_id          VARCHAR(100) NOT NULL,
    source_updated_at TIMESTAMPTZ,
    loaded_at         TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- Preserve the original source timestamp string.
-- Applied after the initial CSV ingestion encountered
-- source timestamp format compatibility requirements.

ALTER TABLE staging.stg_customers
ALTER COLUMN source_updated_at TYPE VARCHAR(30)
USING source_updated_at::VARCHAR;
