# PostgreSQL RDS DBA & ETL Pipeline

## Project Overview

This project demonstrates the design, deployment, administration, security, and operation of an Amazon RDS for PostgreSQL environment integrated with an end-to-end ETL pipeline.

The solution combines AWS infrastructure, PostgreSQL database administration, Apache NiFi-based data ingestion, SQL transformations, and Apache Airflow workflow orchestration.

Data is being integrated from three source types:

- CSV files
- Oracle Database
- MongoDB

CSV ingestion and the initial Oracle LYRIS CUSTOMERS integration have been implemented using Apache NiFi. Airflow orchestration is implemented for the retail CSV workflow, including staging validation, SCD Type 2 dimension loading, and success/failure audit logging. MongoDB integration is planned.

The pipeline loads source data into PostgreSQL staging tables, transforms and validates the data, and loads it into production tables.

The project demonstrates practical DBA and data engineering skills, including secure database access, heterogeneous data integration, incremental processing, performance tuning, backup and recovery, and operational automation.

## Technology Stack

| Category | Technologies |
|---|---|
| Cloud Platform | Amazon Web Services (AWS) |
| Target Database | Amazon RDS for PostgreSQL 18.6 |
| Source Databases | Oracle Database 19c, MongoDB |
| Source Files | CSV |
| Infrastructure as Code | Terraform |
| Data Integration | Apache NiFi 2.12.0 |
| Workflow Orchestration | Apache Airflow 3.3.2 |
| Database Language | SQL, PL/pgSQL |
| Operating System | Red Hat Enterprise Linux 8.10 |
| Connectivity | JDBC, MongoDB connectivity, TLS/SSL |
| Version Control | Git, GitHub |

## Architecture

The project follows a source-to-staging-to-production architecture.

**Implemented CSV ingestion workflow:**

```text
             CSV Source File
                    |
                    v
               Apache NiFi
                    |
          +---------+---------+
          |                   |
          v                   v
     CSV Parsing         Batch Metadata
          |                   |
          +---------+---------+
                    |
                    v
          PostgreSQL RDS 18.6
                    |
                    v
             STAGING SCHEMA
                    |
                    v
          SQL Transformation
          Validation and Upsert
                    |
                    v
            PRODUCTION SCHEMA
                    |
                    v
             Customer Data
```

**Implemented Oracle LYRIS CUSTOMERS workflow:**

```text
       Oracle Database 19c
        LYRIS_APP.CUSTOMERS
                  |
                  v
          Apache NiFi 2.12.0
                  |
          ExecuteSQL (JDBC)
                  |
          UpdateAttribute
          Batch ID / Source
                  |
          UpdateRecord
          Record Metadata
                  |
          PutDatabaseRecord
                  |
                  v
       PostgreSQL RDS 18.6
                  |
                  v
      staging.stg_lyris_customers
                  |
          ReplaceText
          Build Production UPSERT
                  |
                  v
              PutSQL
                  |
                  v
      production.lyris_customers
```

**Implemented orchestration:**

Apache Airflow 3.3.2 runs the `retail_customer_etl` DAG. It validates a batch already loaded in PostgreSQL staging, applies an SCD Type 2 load to `production.dim_customer`, and writes execution results to `audit.etl_batch_log`. The DAG is currently manually triggered (`schedule=None`). The existing NiFi retail processors remain a separate ingestion flow; Airflow does not yet start or stop those processors.

**Planned architecture extensions:**

- MongoDB document and collection extraction.
- Automated scheduling and a controlled NiFi-to-Airflow handoff.
- Additional production dimension and fact tables.

## AWS Infrastructure

The database environment is deployed in AWS using Terraform.

Infrastructure includes:

- Amazon VPC and subnet architecture
- Public and private subnets
- Internet Gateway and NAT Gateway
- Public and private route tables
- Security groups and controlled network access
- Amazon RDS for PostgreSQL 18.6
- Multi-AZ database deployment
- EC2 bastion host for administrative access
- Dedicated EC2 instance for Apache NiFi
- Encrypted gp3 storage

The RDS database is deployed in private networking and accessed through controlled connections.

## PostgreSQL Database Administration

The DBA portion of this project covers:

- Database provisioning and configuration
- Database roles and privileges
- Role-based access control (RBAC)
- Schema ownership and permissions
- Least-privilege access
- Secure database connectivity using TLS
- Database objects and schema management
- SQL administration and troubleshooting
- Query performance analysis
- Indexing and execution plans
- Backup and recovery
- Point-in-time recovery (PITR)
- High availability and disaster recovery concepts

## Database Design

The database uses separate schemas to organize ingestion, production data, and ETL auditing.

### Staging Layer

The staging schema contains source-specific tables used to receive extracted data before production loading.

Implemented tables include:

- `staging.stg_customers` — CSV customer ingestion
- `staging.stg_lyris_customers` — Oracle LYRIS customer ingestion
- Source system identification
- Batch ID tracking
- Load timestamps

Oracle source data is maintained separately from the existing CSV customer pipeline.

### Production Data Model

Implemented production tables include:

- `production.dim_customer` — retail customer dimension with SCD Type 2 history
- `production.lyris_customers` — Oracle LYRIS customer records

The customer dimension maintains current and historical versions using effective dates and an `is_current` flag. The Oracle production table maintains source-system and batch metadata.

### Audit Schema

The audit schema contains:

- `audit.etl_batch_log`

This table supports ETL execution tracking and operational monitoring. The retail Airflow DAG writes batch status, record counts, timestamps, error details, and the Airflow run identifier. PostgreSQL generates the `execution_id` identity value.

## ETL Pipeline

The ETL workflow is organized into three primary stages.

### 1. Extract

Apache NiFi is used to extract data from source systems and load it into PostgreSQL staging tables.

**Implemented CSV extraction:**

- CSV file ingestion using NiFi
- `GetFile` processor for file acquisition
- CSVReader Controller Service for record parsing
- `PutDatabaseRecord` processor for PostgreSQL insertion
- PostgreSQL JDBC connectivity
- Batch identification and source metadata
- Staging data validation

**Implemented Oracle extraction:**

- Oracle Database 19c JDBC connectivity
- Dedicated read-only Oracle connection for LYRIS
- `NIFI_LYRIS_RO` source account
- `ExecuteSQL` extraction from `LYRIS_APP.CUSTOMERS`
- `UpdateAttribute` for source and batch metadata
- `UpdateRecord` for record enrichment
- `PutDatabaseRecord` for PostgreSQL staging insertion
- `ReplaceText` to construct the production UPSERT statement
- `PutSQL` to execute the production load
- Error routing through `LogAttribute`

The initial Oracle pipeline successfully extracted and loaded three customer records into `staging.stg_lyris_customers` and `production.lyris_customers`.

**Planned extraction capabilities:**

- MongoDB collection extraction
- Incremental extraction from database sources
- Additional source-specific data type and schema handling

### 2. Transform

The transformation phase processes staging data and prepares it for production.

**Implemented:**

- SQL-based production UPSERT for Oracle LYRIS CUSTOMERS
- Conversion of staged `created_at` values into PostgreSQL timestamps
- Conflict handling using `ON CONFLICT`
- Updating existing Oracle customer records when the customer ID already exists
- Retail customer SCD Type 2 processing from `staging.stg_customers` into `production.dim_customer`
- Duplicate source-row selection using customer completeness and load timestamp
- Change detection using `IS DISTINCT FROM`
- Batch ID and source-system filtering

**Planned capabilities:**

- Broader data cleansing and standardization
- Rejected-record handling and detailed data-quality rules
- Product and store SCD Type 1 processing
- Date dimension management
- Fact table preparation

### 3. Load

The production layer receives data from the staging layer.

**Implemented:**

- CSV customer production table
- Oracle LYRIS customer production table
- SQL-based inserts and updates
- Conflict handling using `ON CONFLICT`
- Batch ID tracking
- Created and updated timestamps

**Planned capabilities:**

- Dimension table loading
- Fact table loading
- Incremental processing
- Change detection
- Rejected-record handling
- Idempotent ETL execution
- Data reconciliation

## ETL Monitoring and Automation

The audit schema and Airflow provide execution tracking for the retail CSV workflow.

**Implemented:**

- `audit.etl_batch_log` records successful and failed executions
- Airflow DAG `retail_customer_etl` with three dependent tasks: `validate_staging`, `load_customer_dimension`, and `audit_etl_run`
- Staging batch validation through the `postgres_operations` Airflow connection
- SCD Type 2 processing in a database transaction
- Audit task configured with `TriggerRule.ALL_DONE`, so audit logging runs when upstream tasks fail
- Airflow run ID stored in `airflow_run_id`, with a partial unique index supporting retry-safe audit upserts
- Audit fields for records read, inserted, updated, rejected, status, error message, and timestamps
- Successful pipeline execution verified
- Failure path tested using a nonexistent batch; the audit row recorded `FAILED`
- Separate DAGs for staging batch history, audit inspection, and NiFi API connectivity testing

**Current scope and limitations:**

- The retail DAG processes a batch already present in staging. It does not yet trigger the NiFi retail processors.
- The DAG is manually triggered; scheduled execution and a controlled NiFi handoff remain future enhancements.
- Inserted and updated counts reflect the SCD Type 2 dimension load. Rejected-record classification and source-to-target reconciliation are not yet implemented.

## Performance and Reliability

The DBA engineering portion will cover:

- Query execution plan analysis
- Index design and optimization
- PostgreSQL performance monitoring
- Statistics and maintenance
- Vacuum and analyze operations
- Connection and resource monitoring
- Backup validation
- Point-in-time recovery exercises
- Multi-AZ availability and failover concepts

## Repository Structure

```text
postgres-rds-etl-pipeline/
│
├── README.md
├── .gitignore
├── customers_sample.csv
│
├── docs/
│
├── etl/
│   ├── airflow/
│   │   └── dags/
│   │       ├── retail_customer_etl.py
│   │       ├── staging_batch_history.py
│   │       ├── inspect_etl_audit.py
│   │       └── nifi_connection_test.py
│   │
│   └── nifi/
│       └── flows/
│           ├── csv_customers_ingestion_audit_flow.json
│           ├── csv_customers_ingestion.json
│           └── oracle_lyris_customers_ingestion.json
│
├── scripts/
│
├── sql/
│   ├── audit/
│   │   └── 01_etl_batch_log.sql
│   │
│   ├── production/
│   │   └── 01_customers.sql
│   │
│   └── staging/
│       ├── 01_stg_customers.sql
│       └── 02_validate_customer_ingestion.sql
│
└── terraform/
```

## Project Implementation Status

| Phase | Description | Status |
|---|---|---|
| 1 | AWS infrastructure and RDS deployment | Complete |
| 2 | PostgreSQL administration and security | Complete |
| 3 | Database and ETL schema design | Complete |
| 4 | CSV extraction using Apache NiFi | Complete |
| 5 | NiFi success and failure audit handling | Complete |
| 6 | Customer staging-to-production upsert | Complete |
| 7 | Oracle LYRIS CUSTOMERS extraction and production load | Complete |
| 8 | Airflow orchestration and audit logging for retail CSV batch | Complete (manual trigger; NiFi triggering not integrated yet) |
| 9 | PostgreSQL performance tuning | Planned |
| 10 | Backup, recovery, and PITR | Planned |
| 11 | High availability and operational security | Planned |
| 12 | Data transformation and cleansing enhancements | Planned |
| 13 | Production dimension and fact loading | Planned |
| 14 | Incremental ETL and advanced upserts | Planned |
| 15 | MongoDB extraction | Planned |
| 16 | Final documentation and portfolio review | In progress |

## Security Considerations

Security is incorporated throughout the architecture.

- Private database deployment
- Restricted security group access
- TLS-encrypted database connections
- Role-based access control
- Least-privilege permissions
- Encrypted AWS storage
- Separate database schemas and ownership
- Secure source database credentials
- No passwords, private keys, or Terraform state files committed to GitHub

Connection details and environment-specific configuration should be supplied separately when importing or deploying the NiFi flow.

## Documentation

Project implementation details, configuration steps, SQL scripts, and operational procedures are maintained in the `docs/` directory.

Documentation includes or will include:

- AWS infrastructure deployment
- PostgreSQL DBA administration
- Apache NiFi configuration
- CSV extraction and ingestion
- NiFi success and failure auditing
- SQL transformation and production loading
- Oracle extraction and NiFi configuration
- MongoDB extraction
- Airflow orchestration and DAG operations
- DBA monitoring SQL and RDS CloudWatch monitoring
- Backup and recovery procedures

## Project Objective

The goal of this project is to demonstrate practical PostgreSQL DBA and data engineering capabilities in a cloud-based environment.

It brings together AWS infrastructure, PostgreSQL administration, Oracle and MongoDB integration, Apache NiFi, Apache Airflow, SQL development, dimensional modeling, ETL automation, performance optimization, and recovery planning in one end-to-end engineering project.