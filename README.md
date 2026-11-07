# PostgreSQL RDS DBA & ETL Pipeline

## Project Overview

This project demonstrates the design, deployment, administration, security, and operation of an Amazon RDS for PostgreSQL environment integrated with a multi-source ETL pipeline.

The solution combines AWS infrastructure, PostgreSQL database administration, Apache NiFi data ingestion, SQL transformations, and Apache Airflow workflow orchestration.

The project integrates data from three source types:

- CSV files
- Oracle Database
- MongoDB

CSV ingestion and the initial Oracle LYRIS CUSTOMERS integration have been implemented using Apache NiFi. Airflow orchestrates both pipelines: the retail CSV workflow archives the source file, triggers and monitors NiFi, validates staging, loads the SCD Type 2 customer dimension, and audits execution; the Oracle LYRIS workflow starts and monitors its dedicated NiFi processors, verifies the staging-to-production batch, and audits the Airflow run. The retail DAG is scheduled daily at 2:00 AM Eastern, and the Oracle LYRIS DAG is scheduled daily at 2:15 AM Eastern. The Oracle DAG is deployed and registered without import errors; its first scheduled execution remains pending verification. MongoDB integration remains planned.

The pipeline loads source data into PostgreSQL staging tables, transforms and validates the data, and loads it into production tables.

The project demonstrates practical DBA and data engineering skills, including secure database access, heterogeneous data integration, incremental processing concepts, performance tuning, backup and recovery planning, and operational automation.

## Technology Stack

| Category | Technologies |
|---|---|
| Cloud Platform | Amazon Web Services (AWS) |
| Target Database | Amazon RDS for PostgreSQL 18.6 |
| Source Databases | Oracle Database 19c |
| Source Files | CSV |
| Infrastructure as Code | Terraform |
| Data Integration | Apache NiFi 2.12.0 |
| Workflow Orchestration | Apache Airflow 3.3.2 |
| Database Language | SQL, PL/pgSQL |
| Operating System | Red Hat Enterprise Linux 8.10 |
| Connectivity | JDBC, TLS/SSL |
| Version Control | Git, GitHub |

## Architecture

The project follows a source-to-staging-to-production architecture.

### Retail CSV ingestion and orchestration

```text
CSV Source File
      |
      v
Airflow: retail_customer_etl
      |
      +--> Archive source file
      |
      +--> Trigger NiFi ingestion
      |         |
      |         v
      |     Apache NiFi
      |         |
      |         +--> CSV parsing
      |         +--> Batch/source metadata
      |         +--> PostgreSQL insertion
      |         |
      |         v
      |   staging.stg_customers
      |
      +--> Monitor NiFi ingestion
      |
      +--> Validate staging batch
      |
      +--> Load SCD Type 2 dimension
      |         |
      |         v
      |   production.dim_customer
      |
      +--> Audit ETL execution
                |
                v
        audit.etl_batch_log
```

The `retail_customer_etl` DAG coordinates the retail CSV process and uses the configured NiFi and PostgreSQL connections. It is scheduled daily at 2:00 AM Eastern (`America/New_York`) and also supports manual triggering.

### Oracle LYRIS CUSTOMERS ingestion

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
 staging.stg_lyris_customers
         |
     ReplaceText
     Build Production UPSERT
         |
       PutSQL
         |
         v
 production.lyris_customers
```

The Oracle LYRIS flow has been implemented and manually run through NiFi. The `oracle_lyris_customer_etl` Airflow DAG is deployed and registered without import errors. It starts the LYRIS downstream processors before the Oracle `ExecuteSQL` processor, monitors LYRIS flow queues, verifies the latest batch in staging and production, and records the Airflow audit entry. It is scheduled daily at 2:15 AM Eastern (`America/New_York`). Its first scheduled execution has not yet been verified.

### Planned architecture extensions

- First scheduled-run verification for the Oracle LYRIS Airflow DAG.
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
- Dedicated EC2 instance for Apache NiFi and Airflow
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

### Staging layer

Implemented staging tables include:

- `staging.stg_customers` — retail CSV customer ingestion
- `staging.stg_lyris_customers` — Oracle LYRIS customer ingestion

The staging data includes source-system identification, batch tracking, and load timestamps.

Oracle source data is maintained separately from the retail CSV customer pipeline.

### Production data model

Implemented production tables include:

- `production.dim_customer` — retail customer dimension with SCD Type 2 history
- `production.lyris_customers` — Oracle LYRIS customer records

The retail customer dimension maintains current and historical versions using effective dates and an `is_current` flag. The Oracle production table maintains source-system and batch metadata.

### Audit schema

The audit schema contains:

- `audit.etl_batch_log`

This table supports ETL execution tracking and operational monitoring. Airflow records batch status, record counts, timestamps, error details, and the Airflow run identifier. PostgreSQL generates the `execution_id` identity value.

The current design can contain separate NiFi-side and Airflow-side audit entries for the same batch. Interpret the entries by batch ID and execution source; do not add their counters together as though they represent separate source records.

## ETL Pipeline

The ETL workflow is organized into extract, transform, and load stages.

### 1. Extract

Apache NiFi extracts data from source systems and loads it into PostgreSQL staging tables.

**Implemented CSV extraction:**

- CSV file ingestion using NiFi
- `GetFile` processor for file acquisition
- CSVReader Controller Service for record parsing
- `PutDatabaseRecord` processor for PostgreSQL insertion
- PostgreSQL JDBC connectivity
- Batch identification and source metadata
- NiFi success and failure audit routing

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

- Retail customer dimension loading
- Oracle LYRIS customer production loading
- SQL-based inserts and updates
- Conflict handling using `ON CONFLICT` for the Oracle production load
- Batch ID tracking
- Created and updated timestamps
- SCD Type 2 history management for retail customers

**Planned capabilities:**

- Additional dimension and fact table loading
- Broader incremental processing
- Rejected-record handling
- Expanded source-to-target reconciliation

## ETL Monitoring and Automation

Airflow coordinates the retail CSV ingestion and production load. The `retail_customer_etl` DAG contains seven tasks and is scheduled daily at 2:00 AM Eastern:

1. `generate_batch_id` — creates the batch identifier for the run.
2. `archive_source_file` — archives the input file before NiFi consumes it.
3. `trigger_nifi_ingestion` — starts the retail NiFi ingestion flow through the NiFi API.
4. `monitor_nifi_ingestion` — monitors ingestion and checks that the expected batch has reached staging.
5. `validate_staging` — validates the batch in `staging.stg_customers`.
6. `load_customer_dimension` — applies the SCD Type 2 transformation to `production.dim_customer`.
7. `audit_etl_run` — records the Airflow execution outcome and processing counts in `audit.etl_batch_log`.

**Verified execution:**

- The DAG imported successfully without DAG import errors.
- A manual Airflow run completed with all seven tasks successful.
- The verified batch loaded four CSV customer records into `staging.stg_customers`.
- The Airflow audit entry recorded `SUCCESS` and four records read.
- Prior failed runs remain available in audit history.

The reported inserted and updated counters describe the dimension-load operation; they should not be summed to infer a source-row total. The source/staging count for the verified batch was four records.

### Oracle LYRIS Airflow orchestration

The `oracle_lyris_customer_etl` DAG is deployed to the Airflow DAG directory, appears in `airflow dags list`, and has no DAG import errors. The only warning observed during the import-error check was the optional Graphviz rendering warning; it does not prevent DAG registration or execution.

The Oracle DAG contains three tasks:

1. `trigger_lyris_nifi_ingestion` — starts the LYRIS downstream processors and then starts the Oracle extraction processor.
2. `monitor_lyris_ingestion` — monitors the LYRIS NiFi queues and verifies the latest batch in `staging.stg_lyris_customers` and `production.lyris_customers`.
3. `audit_lyris_run` — writes the Airflow execution result to `audit.etl_batch_log`.

The Oracle LYRIS DAG is scheduled daily at 2:15 AM Eastern. Its first scheduled execution has not yet been verified.

**Current scope and limitations:**

- The retail DAG is scheduled daily at 2:00 AM Eastern; a manual run completed successfully with all seven tasks successful.
- The Oracle LYRIS DAG is deployed, registered, and scheduled for 2:15 AM Eastern; first scheduled-run verification is pending.
- MongoDB extraction remains planned.
- NiFi and Airflow may each write an audit entry for the same batch. Use batch ID and execution context to interpret the records.
- Rejected-record classification and comprehensive source-to-target reconciliation remain future enhancements.

## Performance and Reliability

The DBA engineering portion includes work and planned exercises in:

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
├── README.md
├── README_ETL_SQL.md
├── 1_PostgreSQL_RDS_ETL_Engineering.docx
├── customers_sample.csv
├── docs/
├── etl/
│   ├── airflow/
│   │   └── dags/
│   │       ├── retail_customer_etl.py
│   │       ├── staging_batch_history.py
│   │       ├── inspect_etl_audit.py
│   │       └── nifi_connection_test.py
│   └── nifi/
│       └── flows/
│           ├── csv_customers_ingestion_audit_flow.json
│           ├── csv_customers_ingestion.json
│           ├── csv_customers_ingestion_orchestration.json
│           └── oracle_lyris_customers_ingestion.json
├── scripts/
├── sql/
└── terraform/
```

## Project Implementation Status

| Phase | Description | Status |
|---|---|---|
| 1 | AWS infrastructure and RDS deployment | Complete |
| 2 | PostgreSQL administration and security foundation | Complete |
| 3 | Database and ETL schema design | Complete |
| 4 | CSV extraction using Apache NiFi | Complete |
| 5 | NiFi success and failure audit handling | Implemented |
| 6 | Retail customer staging-to-production SCD Type 2 load | Complete |
| 7 | Oracle LYRIS CUSTOMERS NiFi extraction and production load | Complete |
| 8 | Airflow orchestration of retail CSV ingestion and load | Implemented, scheduled, and manually verified |
| 9 | Airflow orchestration of Oracle LYRIS ingestion | Deployed, registered, and scheduled; first scheduled run pending verification |
| 10 | PostgreSQL performance tuning | Planned |
| 11 | Backup, recovery, and PITR exercises | Planned |
| 12 | High availability and operational security enhancements | In progress / further exercises planned |
| 13 | Data transformation and cleansing enhancements | Planned |
| 14 | Additional production dimension and fact loading | Planned |
| 15 | Expanded incremental ETL and reconciliation | Planned |
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

Connection details and environment-specific configuration should be supplied separately when importing or deploying NiFi flow definitions. Review exported flow definitions for sensitive configuration before committing or sharing them.

## Documentation

Project implementation details, configuration steps, SQL scripts, and operational procedures are maintained in the `docs/` directory and the repository README files.

Documentation covers:

- AWS infrastructure deployment
- PostgreSQL DBA administration
- Apache NiFi configuration
- CSV extraction and ingestion
- NiFi success and failure auditing
- SQL transformation and production loading
- Oracle extraction and NiFi configuration
- Airflow orchestration and DAG operations
- DBA monitoring SQL and RDS CloudWatch monitoring
- Backup and recovery procedures


## Project Objective

The goal of this project is to demonstrate practical PostgreSQL DBA and data engineering capabilities in a cloud-based environment.

It brings together AWS infrastructure, PostgreSQL administration, Oracle integration, planned MongoDB integration, Apache NiFi, Apache Airflow, SQL development, dimensional modeling, ETL automation, performance optimization, and recovery planning in one engineering project.
