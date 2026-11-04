# PostgreSQL RDS DBA & ETL Pipeline

## Project Overview

This project demonstrates the design, deployment, administration, security, and operation of an Amazon RDS for PostgreSQL environment integrated with an end-to-end ETL pipeline.

The solution combines AWS infrastructure, PostgreSQL database administration, Apache NiFi-based data ingestion, SQL transformations, and Apache Airflow workflow orchestration.

Data will be extracted from three source types:

- CSV files
- Oracle Database
- MongoDB

The extracted data will be loaded into PostgreSQL staging tables, transformed, validated, and loaded into production tables.

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
| Workflow Orchestration | Apache Airflow |
| Database Language | SQL, PL/pgSQL |
| Operating System | Red Hat Enterprise Linux 8.10 |
| Connectivity | JDBC, MongoDB connectivity, TLS/SSL |
| Version Control | Git, GitHub |

## Architecture

The project follows a source-to-staging-to-production architecture.

```text
                 SOURCE SYSTEMS
        ┌────────────┬────────────┐
        │            │            │
      CSV Files    Oracle      MongoDB
        │            │            │
        └────────────┼────────────┘
                     │
                     ▼
              Apache NiFi
             Data Extraction
                     │
                     ▼
          Amazon RDS PostgreSQL
                 18.6
                     │
                     ▼
             STAGING SCHEMA
                     │
                     ▼
          SQL Transformations
          Validation & Cleansing
                     │
                     ▼
            PRODUCTION SCHEMA
                     │
              ┌──────┴──────┐
              │             │
          Dimensions     Fact Tables
              │             │
              └──────┬──────┘
                     │
                     ▼
             Reporting & Analytics

           Apache Airflow
       Workflow Orchestration
```

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

The staging layer receives source data from CSV, Oracle, and MongoDB.

Planned staging tables will be organized around the source entities and their corresponding data structures.

### Production Data Model

The production layer will follow a dimensional modeling approach.

**Dimension tables:**

- `production.dim_customer` — Slowly Changing Dimension Type 2 (SCD2)
- `production.dim_product` — Slowly Changing Dimension Type 1 (SCD1)
- `production.dim_store` — Slowly Changing Dimension Type 1 (SCD1)
- `production.dim_date`

**Fact table:**

- `production.fact_sales` — Sales transactions at the individual order-line grain

### ETL Audit

`audit.etl_batch_log` will record ETL execution information and support operational monitoring and reconciliation.

## ETL Pipeline

The ETL workflow is organized into three primary stages.

### 1. Extract

Apache NiFi is used to extract data from multiple source systems and load it into PostgreSQL staging tables.

**Implemented:**

- CSV file ingestion using NiFi
- `GetFile` processor for file acquisition
- CSVReader Controller Service for record parsing
- `PutDatabaseRecord` processor for PostgreSQL insertion
- PostgreSQL JDBC connectivity
- Batch identification and source metadata
- Staging data validation

**Planned:**

- Oracle database extraction using JDBC
- MongoDB collection extraction
- Incremental extraction from database sources
- Source-specific data type and schema handling
- Secure connectivity to source databases

### 2. Transform

The transformation phase will process staging data and prepare it for production.

Planned capabilities include:

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

The load phase will populate production dimension and fact tables.

Planned capabilities include:

- Dimension table loading
- Fact table loading
- SQL-based upserts
- Incremental processing
- Change detection
- Rejected-record handling
- Idempotent ETL execution
- Data reconciliation

## ETL Monitoring and Automation

Apache Airflow will be used to schedule and coordinate ETL workflows.

Planned capabilities:

- ETL batch logging
- Source-to-target record reconciliation
- Successful and failed batch tracking
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
├── terraform/
│
├── sql/
│   ├── staging/
│   ├── production/
│   └── audit/
│
├── etl/
│   ├── nifi/
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
| 5 | Oracle extraction | Planned |
| 6 | MongoDB extraction | Planned |
| 7 | Data transformation | Planned |
| 8 | Production dimension and fact loading | Planned |
| 9 | Incremental ETL and upserts | Planned |
| 10 | ETL auditing and reconciliation | Planned |
| 11 | Airflow orchestration and scheduling | Planned |
| 12 | PostgreSQL performance tuning | Planned |
| 13 | Backup, recovery, and PITR | Planned |
| 14 | High availability and operational security | Planned |
| 15 | Final documentation and portfolio review | Planned |

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

## Documentation

Project implementation details, configuration steps, SQL scripts, and operational procedures will be maintained in the `docs/` directory.

Documentation will include:

- AWS infrastructure deployment
- PostgreSQL DBA administration
- Apache NiFi configuration
- CSV extraction
- Oracle extraction
- MongoDB extraction
- SQL transformation and loading
- Airflow orchestration
- Backup and recovery procedures

## Project Objective

The goal of this project is to demonstrate practical PostgreSQL DBA and data engineering capabilities in a cloud-based environment.

It brings together AWS infrastructure, PostgreSQL administration, Oracle and MongoDB integration, Apache NiFi, Apache Airflow, SQL development, dimensional modeling, ETL automation, performance optimization, and recovery planning in one end-to-end engineering project.