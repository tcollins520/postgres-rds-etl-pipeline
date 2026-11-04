CREATE SCHEMA IF NOT EXISTS production;

CREATE TABLE IF NOT EXISTS production.customers (
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
    source_updated_at VARCHAR(30),
    last_batch_id     VARCHAR(100) NOT NULL,
    created_at        TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at        TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT customers_pk
        PRIMARY KEY (source_system, customer_id)
);
