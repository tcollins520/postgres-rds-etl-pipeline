# ETL and SQL Components

This document describes the ETL orchestration, ingestion-flow definitions, and SQL scripts in the PostgreSQL RDS ETL pipeline repository.

## 1. Directory overview

```text
etl/
├── airflow/
│   └── dags/
│       ├── inspect_etl_audit.py
│       ├── nifi_connection_test.py
│       ├── retail_customer_etl.py
│       └── staging_batch_history.py
├── nifi/
│   └── flows/
│       ├── csv_customers_ingestion_audit_flow.json
│       ├── csv_customers_ingestion.json
│       └── oracle_lyris_customers_ingestion.json
└── scripts/

sql/
├── audit/
│   └── 01_etl_batch_log.sql
├── production/
│   ├── 01_customers.sql
│   └── 02_scd_type2_customers.sql
└── staging/
    ├── 01_stg_customers.sql
    └── 02_validate_customer_ingestion.sql
```

The `etl/` directory contains orchestration and ingestion assets. The `sql/` directory contains database objects and SQL used by the staging, production, and audit layers.

## 2. ETL assets

### Apache Airflow DAGs

Location: `etl/airflow/dags/`

| File | Purpose |
|---|---|
| `retail_customer_etl.py` | Main customer ETL workflow. It validates an existing staging batch, loads the customer dimension using the SCD Type 2 transformation, and writes an ETL audit result. |
| `staging_batch_history.py` | DAG for inspecting staging-batch history. |
| `inspect_etl_audit.py` | DAG for inspecting ETL audit records. |
| `nifi_connection_test.py` | DAG for testing the configured NiFi connection. |

The main customer workflow has three tasks:

1. **Validate staging** — checks the selected batch in `staging.stg_customers` before loading.
2. **Load customer dimension** — applies the customer SCD Type 2 transformation to `production.dim_customer`.
3. **Audit ETL run** — records the outcome and processing counts in `audit.etl_batch_log`, including the Airflow run identifier.

The audit task is designed to record a failure outcome when an upstream task fails or is skipped, so failed runs can be reviewed rather than silently omitted.

### Apache NiFi flow definitions

Location: `etl/nifi/flows/`

| File | Purpose |
|---|---|
| `csv_customers_ingestion.json` | NiFi flow definition for CSV customer ingestion. |
| `csv_customers_ingestion_audit_flow.json` | NiFi flow definition for CSV customer ingestion with audit-related processing. |
| `oracle_lyris_customers_ingestion.json` | NiFi flow definition for the Oracle Lyris customer ingestion path. |

These JSON files are flow definitions, not Airflow DAGs. Review the flow and its processor configuration before importing or starting it in a NiFi environment.

### Scripts

Location: `etl/scripts/`

This directory is reserved for supporting ETL scripts. Document individual scripts here as they are added.

## 3. SQL assets

### Audit layer

Location: `sql/audit/`

| File | Purpose |
|---|---|
| `01_etl_batch_log.sql` | Defines the ETL batch audit table used to record batch execution outcomes and processing details. |

The audit table is used by the Airflow workflow to retain execution history, including status, records read, inserted and updated counts, error details, and the Airflow run identifier. The table's identity column generates the execution ID; inserts should omit that identity column when it is defined as `GENERATED ALWAYS`.

The project also uses a unique partial index on non-null `airflow_run_id` values to support idempotent audit writes for an Airflow run.

### Staging layer

Location: `sql/staging/`

| File | Purpose |
|---|---|
| `01_stg_customers.sql` | Creates the customer staging table used as the landing area for ingested customer records. |
| `02_validate_customer_ingestion.sql` | Contains SQL for validating customer ingestion data before the production load. |

The Airflow workflow validates a batch in `staging.stg_customers` before attempting the production transformation.

### Production layer

Location: `sql/production/`

| File | Purpose |
|---|---|
| `01_customers.sql` | Creates the production customer table structure. |
| `02_scd_type2_customers.sql` | Implements the customer Type 2 slowly changing dimension load. |

The SCD Type 2 transformation preserves historical customer versions. When a customer's tracked attributes change, the prior version is expired and a new current version is inserted. New customers are inserted as their first version; unchanged customers do not require a new version.

## 4. End-to-end processing flow

The current implemented Airflow workflow starts from a batch that is already present in the staging table:

```text
Source / NiFi ingestion
        |
        v
staging.stg_customers
        |
        v
Airflow: validate_staging
        |
        v
Airflow: load_customer_dimension
        |
        v
production.dim_customer
        |
        v
Airflow: audit_etl_run
        |
        v
audit.etl_batch_log
```

Airflow currently runs the customer workflow as a manually triggered DAG. It processes an existing staging batch; the DAG does not currently start the NiFi ingestion flow itself.

## 5. Running and operating the workflow

Before triggering a run:

1. Confirm that the expected batch has been ingested into `staging.stg_customers`.
2. Confirm that the batch identifier configured for the DAG matches the batch to process.
3. Confirm that the Airflow PostgreSQL connection points to the intended RDS database.
4. Confirm that the Airflow NiFi connection is available if running the NiFi connection test DAG.
5. Trigger `retail_customer_etl` from the Airflow UI or using the project's approved Airflow operating procedure.

After the run:

1. Review the task states in the Airflow UI.
2. Review the corresponding record in `audit.etl_batch_log`.
3. Confirm the expected customer versions in `production.dim_customer`.
4. Investigate any failed validation, load, or audit details before rerunning a batch.

Do not start a NiFi flow or reprocess an input file without first checking the flow's source-file handling and the batch's current staging and production state. This helps avoid duplicate or unintended ingestion.

## 6. Current implementation scope and follow-up work

Implemented and exercised:

- Customer CSV data is available in the staging table for the current example batch.
- Airflow validates the staging batch and runs the customer SCD Type 2 load.
- Airflow writes success and failure outcomes to the ETL audit table.
- The success and failure audit paths have been tested.

Current limitations / planned enhancements:

- The Airflow customer workflow is manually triggered and does not yet launch NiFi ingestion.
- The workflow expects the source batch to exist in staging before it runs.
- Rejected-record handling and full source-to-target reconciliation remain follow-up work.
- The Oracle-to-PostgreSQL NiFi extraction path has not yet been implemented end to end.
- Add descriptions and usage instructions for individual files placed in `etl/scripts/`.

## 7. Security and change-management notes

- Do not commit database passwords, private keys, certificates containing private key material, or other credentials.
- Keep database access least-privileged. The ETL account should receive only the schema and table permissions required by the workflow.
- Use TLS for connections to Amazon RDS and other remote services.
- Review SQL changes before execution, especially DDL, data-modification statements, and scripts that expire or insert dimension versions.
- Test changes with a controlled batch before using them against production data.
