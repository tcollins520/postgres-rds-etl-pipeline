from datetime import datetime

from airflow.sdk import DAG
from airflow.providers.standard.operators.python import PythonOperator


def inspect_staging_batches():
    from airflow.providers.postgres.hooks.postgres import PostgresHook

    hook = PostgresHook(postgres_conn_id="postgres_operations")

    rows = hook.get_records("""
        SELECT
            batch_id,
            source_system,
            COUNT(*) AS record_count,
            MIN(loaded_at) AS first_loaded,
            MAX(loaded_at) AS last_loaded
        FROM staging.stg_customers
        GROUP BY batch_id, source_system
        ORDER BY last_loaded DESC;
    """)

    print("Staging batch history:")
    for row in rows:
        print(row)


with DAG(
    dag_id="staging_batch_history",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["postgresql", "read-only"],
) as dag:

    inspect_batches = PythonOperator(
        task_id="inspect_staging_batches",
        python_callable=inspect_staging_batches,
    )
