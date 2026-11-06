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
│       ├── csv_customers_ingestion_orchestration.json
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

The `etl/` directory contains Airflow DAGs and NiFi flow definitions. The `sql/` directory contains database objects and SQL used by the staging, production, and audit layers.

## 2. ETL assets

### Apache Airflow DAGs

Location: `etl/airflow/dags/`

| File | Purpose |
|---|---|
| `retail_customer_etl.py` | Main retail customer workflow. Generates a batch ID, archives the source file, triggers and monitors NiFi ingestion, validates the staging batch, loads the customer dimension using SCD Type 2, and audits the run. |
| `staging_batch_history.py` | DAG for inspecting staging-batch history. |
| `inspect_etl_audit.py` | DAG for inspecting ETL audit records. |
| `nifi_connection_test.py` | DAG for testing the configured NiFi connection. |

The main retail customer DAG has seven tasks:

1. **Generate batch ID (`generate_batch_id`)** — creates a unique batch identifier for the run.
2. **Archive source file (`archive_source_file`)** — archives the input CSV before NiFi consumes it.
3. **Trigger NiFi ingestion (`trigger_nifi_ingestion`)** — starts the retail NiFi ingestion flow through the configured NiFi API connection.
4. **Monitor NiFi ingestion (`monitor_nifi_ingestion`)** — monitors the flow and checks for the expected batch in PostgreSQL staging.
5. **Validate staging (`validate_staging`)** — validates the selected batch in `staging.stg_customers`.
6. **Load customer dimension (`load_customer_dimension`)** — applies the SCD Type 2 transformation to `production.dim_customer`.
7. **Audit ETL run (`audit_etl_run`)** — records the run outcome and processing counts in `audit.etl_batch_log`.

The DAG uses the Airflow connections `nifi_api` and `postgres_operations`. Connection credentials are managed in Airflow and must not be placed in source files or committed to Git.

The DAG is currently manually triggered (`schedule=None`).

### Apache NiFi flow definitions

Location: `etl/nifi/flows/`

| File | Purpose |
|---|---|
| `csv_customers_ingestion.json` | Earlier NiFi flow definition for CSV customer ingestion. |
| `csv_customers_ingestion_audit_flow.json` | CSV ingestion flow definition with audit-related processing. |
| `csv_customers_ingestion_orchestration.json` | Current exported NiFi CSV ingestion flow definition associated with the retail Airflow-orchestrated workflow. |
| `oracle_lyris_customers_ingestion.json` | NiFi flow definition for the Oracle LYRIS customer ingestion path. |

These JSON files are NiFi flow definitions, not Airflow DAGs. Airflow orchestration logic is maintained separately in `etl/airflow/dags/retail_customer_etl.py`. Review processor configuration, controller-service references, parameter contexts, and environment-specific settings before importing or starting a flow in another NiFi environment.

The Oracle LYRIS NiFi flow has been implemented and manually run. Its orchestration by Airflow is not yet confirmed.

### Scripts

Location: `etl/scripts/`

This directory is reserved for supporting ETL scripts. Document individual scripts here as they are added.

## 3. SQL assets

### Audit layer

Location: `sql/audit/`

| File | Purpose |
|---|---|
| `01_etl_batch_log.sql` | Defines the ETL batch audit table used to record batch execution outcomes and processing details. |

The audit table is used by the Airflow workflow to retain execution history, including status, records read, inserted and updated counts, error details, timestamps, and the Airflow run identifier. The table's identity column generates the execution ID; inserts should omit that identity column when it is defined as `GENERATED ALWAYS`.

The project also uses a unique partial index on non-null `airflow_run_id` values to support idempotent audit writes for an Airflow run.

NiFi and Airflow can write separate audit entries for the same batch. Use the batch ID and execution context to distinguish ingestion-level records from Airflow execution records. Do not add counters from both entries together as though they represent separate source rows.

### Staging layer

Location: `sql/staging/`

| File | Purpose |
|---|---|
| `01_stg_customers.sql` | Creates the customer staging table used as the landing area for ingested customer records. |
| `02_validate_customer_ingestion.sql` | Contains SQL for validating customer ingestion data before the production load. |

The Airflow workflow validates the batch in `staging.stg_customers` before attempting the production transformation.

### Production layer

Location: `sql/production/`

| File | Purpose |
|---|---|
| `01_customers.sql` | Creates the production customer table structure. |
| `02_scd_type2_customers.sql` | Implements the customer Type 2 slowly changing dimension load. |

The SCD Type 2 transformation preserves historical customer versions. When a customer's tracked attributes change, the prior version is expired and a new current version is inserted. New customers are inserted as their first version; unchanged customers do not require a new version.

## 4. End-to-end processing flow

The current retail CSV workflow is orchestrated by Airflow:

```text
CSV source file
      |
      v
Airflow: generate_batch_id
      |
      v
Airflow: archive_source_file
      |
      v
Airflow: trigger_nifi_ingestion
      |
      v
Apache NiFi CSV ingestion
      |
      v
staging.stg_customers
      |
      v
Airflow: monitor_nifi_ingestion
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

The DAG is manually triggered (`schedule=None`). It coordinates the retail NiFi ingestion, staging validation, production dimension load, and Airflow audit.

The Oracle LYRIS extraction is a separate NiFi flow. Its Airflow orchestration is not yet confirmed.

## 5. Running and operating the workflow

Before triggering a run:

1. Confirm the expected CSV input file is present in the configured NiFi incoming location.
2. Confirm the Airflow connections `nifi_api` and `postgres_operations` are configured and available.
3. Confirm the NiFi flow and its required controller services are configured for the target environment.
4. Trigger `retail_customer_etl` from the Airflow UI or using the project's approved Airflow operating procedure.

After the run:

1. Review all seven task states in the Airflow UI.
2. Review the corresponding Airflow execution record in `audit.etl_batch_log`.
3. Review the batch in `staging.stg_customers`.
4. Confirm the expected current and historical customer versions in `production.dim_customer`.
5. Investigate failed ingestion, validation, load, or audit details before rerunning a batch.

The DAG archives the source file before triggering NiFi ingestion. Do not manually move, restore, or reprocess an input file without first checking the batch's staging and production state. This helps avoid duplicate or unintended ingestion.

## 6. Verified execution and current scope

The retail CSV Airflow workflow was successfully executed:

- The DAG imported successfully without DAG import errors.
- All seven tasks completed successfully in the verified manual run.
- The verified batch contained four records in `staging.stg_customers`.
- The Airflow audit entry recorded `SUCCESS` and four records read.
- Earlier failed runs remain in audit history.

The inserted and updated counters describe the SCD Type 2 dimension operation; they are not separate source-row totals and should not be summed to infer the number of input records.

Implemented and exercised:

- CSV customer ingestion through NiFi.
- NiFi success and failure audit routing.
- Airflow source-file archiving and NiFi API triggering/monitoring for the retail CSV workflow.
- Airflow staging validation and customer SCD Type 2 loading.
- Airflow success/failure audit handling.
- Oracle LYRIS extraction and production loading through a separate NiFi flow.

Current limitations and planned enhancements:

- The retail DAG is manually triggered; scheduled execution remains future work.
- Airflow orchestration of the Oracle LYRIS flow has not yet been confirmed.
- MongoDB extraction remains planned.
- Rejected-record classification and comprehensive source-to-target reconciliation remain follow-up work.
- NiFi and Airflow may create separate audit entries for a batch; a consolidated canonical batch summary is a potential improvement.
- Add descriptions and usage instructions for individual files placed in `etl/scripts/`.

## 7. Security and change-management notes

- Do not commit database passwords, private keys, certificates containing private key material, or other credentials.
- Keep database access least-privileged. The ETL account should receive only the schema and table permissions required by the workflow.
- Use TLS for connections to Amazon RDS and other remote services.
- Review NiFi flow exports for sensitive or environment-specific configuration before committing or sharing them.
- Review SQL changes before execution, especially DDL, data-modification statements, and scripts that expire or insert dimension versions.
- Test changes with a controlled batch before using them against production data.
