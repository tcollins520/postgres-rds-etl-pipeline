\set ON_ERROR_STOP on

BEGIN;

-- Select one complete record per customer from the requested batch.
-- Prefer the most complete record, then the most recently loaded.

CREATE TEMP TABLE scd_customer_source
ON COMMIT DROP
AS
SELECT DISTINCT ON (customer_id)
    customer_id,
    first_name,
    last_name,
    email,
    phone,
    address,
    city,
    state,
    postal_code,
    country,
    customer_segment
FROM staging.stg_customers
WHERE source_system = 'RETAIL_CSV'
  AND batch_id = :'batch_id'
ORDER BY
    customer_id,
    (
        (first_name IS NOT NULL)::int +
        (last_name IS NOT NULL)::int +
        (email IS NOT NULL)::int +
        (phone IS NOT NULL)::int +
        (address IS NOT NULL)::int +
        (city IS NOT NULL)::int +
        (state IS NOT NULL)::int +
        (postal_code IS NOT NULL)::int +
        (country IS NOT NULL)::int +
        (customer_segment IS NOT NULL)::int
    ) DESC,
    loaded_at DESC;

-- Expire current dimension records when tracked attributes change.

UPDATE production.dim_customer AS d
SET
    effective_end_date = CURRENT_TIMESTAMP,
    is_current = FALSE
FROM scd_customer_source AS s
WHERE d.customer_id = s.customer_id
  AND d.is_current = TRUE
  AND d.source_system = 'CRM'
  AND ROW(
      d.first_name, d.last_name, d.email, d.phone,
      d.address, d.city, d.state, d.postal_code,
      d.country, d.customer_segment
  ) IS DISTINCT FROM ROW(
      s.first_name, s.last_name, s.email, s.phone,
      s.address, s.city, s.state, s.postal_code,
      s.country, s.customer_segment
  );

-- Insert new customers and new versions of changed customers.

INSERT INTO production.dim_customer (
    customer_id,
    first_name,
    last_name,
    email,
    phone,
    address,
    city,
    state,
    postal_code,
    country,
    customer_segment,
    effective_start_date,
    effective_end_date,
    is_current,
    source_system,
    created_at
)
SELECT
    s.customer_id,
    s.first_name,
    s.last_name,
    s.email,
    s.phone,
    s.address,
    s.city,
    s.state,
    s.postal_code,
    s.country,
    s.customer_segment,
    CURRENT_TIMESTAMP,
    '9999-12-31 00:00:00+00',
    TRUE,
    'CRM',
    CURRENT_TIMESTAMP
FROM scd_customer_source AS s
WHERE NOT EXISTS (
    SELECT 1
    FROM production.dim_customer AS d
    WHERE d.customer_id = s.customer_id
      AND d.is_current = TRUE
)
ON CONFLICT (customer_id) WHERE is_current
DO NOTHING;

COMMIT;