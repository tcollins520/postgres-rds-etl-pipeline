# PostgreSQL RDS DBA & ETL Pipeline

## Project Overview

This project demonstrates the design, deployment, administration, security, and operation of an Amazon RDS for PostgreSQL environment integrated with an end-to-end ETL pipeline.

The solution combines AWS infrastructure, PostgreSQL database administration, Apache NiFi-based data ingestion, SQL transformations, and planned Apache Airflow workflow orchestration.

Data is being integrated from three source types:

- CSV files
- Oracle Database
- MongoDB

CSV ingestion has been implemented using Apache NiFi. Oracle and MongoDB integrations are planned.

The pipeline loads source data into PostgreSQL staging tables, transforms and validates the data, and loads it into production tables.

The project demonstrates practical DBA and data engineering skills, including secure database access, heterogeneous data integration, incremental processing, performance tuning, backup and recovery, and operational automation.

## Technology Stack

| Category | Technologies |
|---|---|
| Cloud Platform | Amazon Web Services (AWS) |
| Target Database | Amazon RDS for PostgreSQL 18.6 |
| Source Databases | Oracle Database, MongoDB |
| Source Files | CSV |
| Infrastructure as Code | Terraform |
| Data Integration | Apache NiFi 2.12 |
| Workflow Orchestration | Apache Airflow (planned) |
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

**Planned architecture extensions:**

- Oracle Database extraction through JDBC.
- MongoDB document and collection extraction.
- Apache Airflow orchestration and scheduling.
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

| Schema | Purpose |
|---|---|
| `staging` | Holds raw source data during ingestion |
| `production` | Contains transformed business data |
| `audit` | Stores ETL execution and batch information |

### Staging Layer

The staging layer receives source data before transformation and production loading.

**Implemented:**

- `staging.stg_customers`
- CSV customer ingestion
- Source system identification
- Batch identification
- Raw source timestamp preservation
- Staging data validation

The staging table preserves the original source timestamp as text to avoid assuming a source timezone or changing the original timestamp representation.

### Production Data Model

The initial production implementation includes:

- `production.customers`

The customer table uses a composite primary key consisting of `source_system` and `customer_id`.

The staging-to-production load uses PostgreSQL `INSERT ... ON CONFLICT DO UPDATE` to support inserts and updates.

**Planned dimensional model:**

Dimension tables:

- `production.dim_customer` — Slowly Changing Dimension Type 2 (SCD2)
- `production.dim_product` — Slowly Changing Dimension Type 1 (SCD1)
- `production.dim_store` — Slowly Changing Dimension Type 1 (SCD1)
- `production.dim_date`

Fact table:

- `production.fact_sales` — Sales transactions at the individual order-line grain

### ETL Audit

The `audit.etl_batch_log` table records ETL execution information and supports operational monitoring.

Implemented audit fields include:

- Execution ID
- Batch ID
- Workflow name
- Source system
- Start and end timestamps
- Records read, inserted, updated, and rejected
- Batch status
- Error message

The table supports `RUNNING`, `SUCCESS`, and `FAILED` statuses.

The NiFi workflow has been tested with both successful and failed staging-load scenarios.

**Current limitation:** The NiFi audit paths record execution status and error information, but detailed record-count metrics are not yet dynamically calculated. The current audit flow uses default or placeholder count values.

## ETL Pipeline

The ETL workflow is organized into three primary stages.

### 1. Extract

Apache NiFi is used to extract data from source systems and load it into PostgreSQL staging tables.

**Implemented: CSV customer ingestion**

The NiFi flow contains six processors:

1. `GetFile` — Retrieves the incoming CSV file.
2. `UpdateAttribute` — Adds source system, workflow, and batch metadata.
3. `PutDatabaseRecord` — Parses CSV records using the CSVReader Controller Service and inserts them into PostgreSQL staging.
4. `ReplaceText` — Prepares the successful execution audit SQL.
5. `ReplaceText_Failure_Audit` — Prepares the failed execution audit SQL.
6. `PutSQL` — Executes the audit SQL against PostgreSQL.

The successful staging-load path writes a `SUCCESS` audit record.

The failed staging-load path writes a `FAILED` audit record with an error description.

The workflow has been tested for both paths, and the corresponding audit records were verified in PostgreSQL.

**Additional implemented functionality:**

- PostgreSQL JDBC connectivity
- CSVReader Controller Service
- Batch identification and source metadata
- Staging data insertion
- Successful and failed execution auditing

**Planned:**

- Oracle database extraction using JDBC
- MongoDB collection extraction
- Incremental extraction from database sources
- Source-specific data type and schema handling
- Secure connectivity to source databases

### 2. Transform

SQL transformations are used to process staging data and prepare it for production.

**Implemented:**

- Staging-to-production customer data transformation
- Customer record insertion
- Duplicate handling through conflict detection
- PostgreSQL upsert processing

The customer upsert uses:

```sql
INSERT INTO production.customers (
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
    source_system,
    source_updated_at,
    last_batch_id
)
SELECT
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
    source_system,
    source_updated_at,
    batch_id
FROM staging.stg_customers
WHERE batch_id = 'BATCH_20261103_001'
ON CONFLICT (source_system, customer_id)
DO UPDATE SET
    first_name = EXCLUDED.first_name,
    last_name = EXCLUDED.last_name,
    email = EXCLUDED.email,
    phone = EXCLUDED.phone,
    address = EXCLUDED.address,
    city = EXCLUDED.city,
    state = EXCLUDED.state,
    postal_code = EXCLUDED.postal_code,
    country = EXCLUDED.country,
    customer_segment = EXCLUDED.customer_segment,
    source_updated_at = EXCLUDED.source_updated_at,
    last_batch_id = EXCLUDED.last_batch_id,
    updated_at = CURRENT_TIMESTAMP;
```

The upsert was tested by inserting customer records and subsequently updating an existing customer's email address.

**Planned capabilities:**

- Data cleansing and standardization
- Data type conversion
- Null and invalid-record handling
- Business-rule validation
- Duplicate detection
- Customer SCD Type 2 processing
- Product and store SCD Type 1 processing
- Date dimension management
- Fact table preparation

### 3. Load

The production layer receives transformed data from the staging layer.

**Implemented:**

- Customer production table
- Composite primary key
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

The audit schema provides execution tracking for the NiFi workflow.

**Implemented:**

- ETL batch audit table
- Successful batch tracking
- Failed batch tracking
- Execution timestamps
- Workflow and source identification
- Failure message recording

**Planned:**

Apache Airflow will be used to schedule and coordinate ETL workflows.

Planned capabilities include:

- ETL batch logging enhancements
- Source-to-target record reconciliation
- Accurate inserted, updated, and rejected record counts
- Rejected-record logging
- Error handling and recovery
- Automated pipeline execution
- Workflow dependencies and scheduling

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
│
├── customers_sample.csv
│
├── terraform/
│
├── sql/
│   ├── staging/
│   ├── production/
│   │   └── 01_customers.sql
│   └── audit/
│       └── 01_etl_batch_log.sql
│
├── etl/
│   ├── nifi/
│   │   └── flows/
│   │       └── csv_customers_ingestion_audit_flow.json
│   └── airflow/
│
├── scripts/
│   ├── administration/
│   ├── backup_recovery/
│   └── monitoring/
│
├── docs/
│
└── .gitignore
```

## Project Implementation Status

| Phase | Description | Status |
|---|---|---|
| 1 | AWS infrastructure and RDS deployment | Complete |
| 2 | PostgreSQL administration and security | In progress |
| 3 | Database and ETL schema design | In progress |
| 4 | CSV extraction using Apache NiFi | Complete |
| 5 | NiFi success and failure audit handling | Complete |
| 6 | Customer staging-to-production upsert | Complete |
| 7 | Oracle extraction | Planned |
| 8 | MongoDB extraction | Planned |
| 9 | Data transformation and cleansing enhancements | Planned |
| 10 | Production dimension and fact loading | Planned |
| 11 | Incremental ETL and advanced upserts | Planned |
| 12 | Accurate ETL metrics and reconciliation | Planned |
| 13 | Airflow orchestration and scheduling | Planned |
| 14 | PostgreSQL performance tuning | Planned |
| 15 | Backup, recovery, and PITR | Planned |
| 16 | High availability and operational security | Planned |
| 17 | Final documentation and portfolio review | In progress |

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

Documentation will include:

- AWS infrastructure deployment
- PostgreSQL DBA administration
- Apache NiFi configuration
- CSV extraction and ingestion
- NiFi success and failure auditing
- SQL transformation and production loading
- Oracle extraction
- MongoDB extraction
- Airflow orchestration
- Backup and recovery procedures

## Project Objective

The goal of this project is to demonstrate practical PostgreSQL DBA and data engineering capabilities in a cloud-based environment.

It brings together AWS infrastructure, PostgreSQL administration, Oracle and MongoDB integration, Apache NiFi, Apache Airflow, SQL development, dimensional modeling, ETL automation, performance optimization, and recovery planning in one end-to-end engineering project.