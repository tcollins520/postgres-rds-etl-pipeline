# Customer CSV Ingestion – Validation Evidence

## Execution Details

- Target database: Amazon RDS PostgreSQL 18.6
- Database: `operations`
- Target table: `staging.stg_customers`
- Source: CSV customer file
- Batch ID: `BATCH_20261103_001`
- Connection security: TLS 1.3

## Validation Query

```sql
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
```

## Execution Results

| Customer ID | First Name | Last Name | Email | Source Updated At |
|---|---|---|---|---|
| C1005 | David | Wilson | david.wilson@example.com | 11/03/2026 09:00:00 |
| C1006 | Lisa | Anderson | lisa.anderson@example.com | 11/03/2026 09:05:00 |
| C1007 | Robert | Thomas | robert.thomas@example.com | 11/03/2026 09:10:00 |
| C1008 | Maria | Jackson | maria.jackson@example.com | 11/03/2026 09:15:00 |

**Result:** 4 rows returned successfully.

## Validation Outcome

- CSV records successfully loaded into PostgreSQL staging.
- Customer IDs were preserved.
- Source timestamp strings were preserved.
- Batch identification was verified.
- PostgreSQL connection was established using TLS 1.3.

<badge color="success">CSV extraction validated</badge>

[postgres@ip-10-30-10-221 ~]$ psql "host=postgres-rds-prod-db01.cqngqgqyix25.us-east-1.rds.amazonaws.com port=5432 dbname=operations user=postgres_admin sslmode=verify-full sslrootcert=/home/postgres/global-bundle.pem" -W
Password: 
psql (18.6)
SSL connection (protocol: TLSv1.3, cipher: TLS_AES_256_GCM_SHA384, compression: off, ALPN: postgresql)
Type "help" for help.

operations=> SELECT
    customer_id,
    first_name,
    last_name,
    email,
    source_updated_at,
    batch_id
FROM staging.stg_customers
WHERE batch_id = 'BATCH_20261103_001'
ORDER BY customer_id;
```
 customer_id | first_name | last_name |           email           |  source_updated_at  |      batch_id      
-------------+------------+-----------+---------------------------+---------------------+--------------------
 C1005       | David      | Wilson    | david.wilson@example.com  | 11/03/2026 09:00:00 | BATCH_20261103_001
 C1006       | Lisa       | Anderson  | lisa.anderson@example.com | 11/03/2026 09:05:00 | BATCH_20261103_001
 C1007       | Robert     | Thomas    | robert.thomas@example.com | 11/03/2026 09:10:00 | BATCH_20261103_001
 C1008       | Maria      | Jackson   | maria.jackson@example.com | 11/03/2026 09:15:00 | BATCH_20261103_001
(4 rows)
```
