from datetime import datetime, timezone

from airflow.sdk import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.trigger_rule import TriggerRule

BATCH_ID = "BATCH_20261103_001"
SOURCE_SYSTEM = "RETAIL_CSV"
WORKFLOW_NAME = "CSV_CUSTOMER_INGESTION"
POSTGRES_CONN_ID = "postgres_operations"


def validate_staging():
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

    row = hook.get_first(
        """
        SELECT COUNT(*)
        FROM staging.stg_customers
        WHERE batch_id = %s
          AND source_system = %s;
        """,
        parameters=(BATCH_ID, SOURCE_SYSTEM),
    )

    record_count = row[0]

    if record_count == 0:
        raise ValueError(f"No staging records found for {BATCH_ID}")

    print(f"Validated batch {BATCH_ID}: {record_count} records")

    return {
        "batch_id": BATCH_ID,
        "records_read": record_count,
        "start_time": datetime.now(timezone.utc).isoformat(),
    }


def load_customer_dimension(**context):
    batch = context["ti"].xcom_pull(task_ids="validate_staging")
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    conn = hook.get_conn()

    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                CREATE TEMP TABLE scd_customer_source
                ON COMMIT DROP
                AS
                SELECT DISTINCT ON (customer_id)
                    customer_id, first_name, last_name, email, phone,
                    address, city, state, postal_code, country,
                    customer_segment
                FROM staging.stg_customers
                WHERE source_system = %s
                  AND batch_id = %s
                ORDER BY customer_id,
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
                """,
                (SOURCE_SYSTEM, batch["batch_id"]),
            )

            cur.execute(
                """
                UPDATE production.dim_customer AS d
                SET effective_end_date = CURRENT_TIMESTAMP,
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
                """
            )
            updated_count = cur.rowcount

            cur.execute(
                """
                INSERT INTO production.dim_customer (
                    customer_id, first_name, last_name, email, phone,
                    address, city, state, postal_code, country,
                    customer_segment, effective_start_date,
                    effective_end_date, is_current, source_system, created_at
                )
                SELECT
                    s.customer_id, s.first_name, s.last_name, s.email,
                    s.phone, s.address, s.city, s.state, s.postal_code,
                    s.country, s.customer_segment, CURRENT_TIMESTAMP,
                    '9999-12-31 00:00:00+00', TRUE, 'RETAIL_CSV', CURRENT_TIMESTAMP
                FROM scd_customer_source s
                WHERE NOT EXISTS (
                    SELECT 1
                    FROM production.dim_customer d
                    WHERE d.customer_id = s.customer_id
                      AND d.is_current = TRUE
                )
                ON CONFLICT (customer_id) WHERE is_current DO NOTHING;
                """
            )
            inserted_count = cur.rowcount

        conn.commit()

        result = {
            "records_updated": updated_count,
            "records_inserted": inserted_count,
        }

        print(f"SCD Type 2 completed: {result}")
        return result

    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def audit_etl_run(**context):
    ti = context["ti"]
    dag_run = context["dag_run"]

    batch = ti.xcom_pull(task_ids="validate_staging")
    results = ti.xcom_pull(task_ids="load_customer_dimension")

    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

    failed_tasks = []

    if batch is None:
        failed_tasks.append("validate_staging=failed_or_skipped")

    if batch is not None and results is None:
        failed_tasks.append("load_customer_dimension=failed_or_skipped")

    batch_status = "FAILED" if failed_tasks else "SUCCESS"
    error_message = "; ".join(failed_tasks) if failed_tasks else None

    records_read = (
        batch["records_read"] if batch else
        hook.get_first(
            """
            SELECT COUNT(*)
            FROM staging.stg_customers
            WHERE batch_id = %s AND source_system = %s;
            """,
            parameters=(BATCH_ID, SOURCE_SYSTEM),
        )[0]
    )

    records_inserted = results["records_inserted"] if results else 0
    records_updated = results["records_updated"] if results else 0

    hook.run(
        """
        INSERT INTO audit.etl_batch_log (
            batch_id,
            workflow_name,
            source_system,
            start_time,
            end_time,
            records_read,
            records_inserted,
            records_updated,
            records_rejected,
            batch_status,
            error_message,
            airflow_run_id
        )
        VALUES (
            %s, %s, %s, %s, CURRENT_TIMESTAMP,
            %s, %s, %s, 0, %s, %s, %s
        )
        ON CONFLICT (airflow_run_id)
        WHERE airflow_run_id IS NOT NULL
        DO UPDATE SET
            end_time = CURRENT_TIMESTAMP,
            records_read = EXCLUDED.records_read,
            records_inserted = EXCLUDED.records_inserted,
            records_updated = EXCLUDED.records_updated,
            batch_status = EXCLUDED.batch_status,
            error_message = EXCLUDED.error_message;
        """,
        parameters=(
            BATCH_ID,
            WORKFLOW_NAME,
            SOURCE_SYSTEM,
            dag_run.start_date or datetime.now(timezone.utc),
            records_read,
            records_inserted,
            records_updated,
            batch_status,
            error_message,
            dag_run.run_id,
        ),
    )

    print(
        f"Audit written: run_id={dag_run.run_id}, "
        f"status={batch_status}, records_read={records_read}, "
        f"inserted={records_inserted}, updated={records_updated}, "
        f"failed_tasks={failed_tasks}"
    )


with DAG(
    dag_id="retail_customer_etl",
    description="Retail customer staging, SCD Type 2 load, and audit",
    start_date=datetime(2026, 11, 5),
    schedule=None,
    catchup=False,
    tags=["retail", "postgresql", "etl"],
) as dag:

    validate = PythonOperator(
        task_id="validate_staging",
        python_callable=validate_staging,
    )

    load_dimension = PythonOperator(
        task_id="load_customer_dimension",
        python_callable=load_customer_dimension,
    )

    audit = PythonOperator(
        task_id="audit_etl_run",
        python_callable=audit_etl_run,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    validate >> load_dimension >> audit
