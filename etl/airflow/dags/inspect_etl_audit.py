from datetime import datetime

from airflow.sdk import DAG
from airflow.providers.standard.operators.python import PythonOperator


def inspect_etl_audit():
    from airflow.providers.postgres.hooks.postgres import PostgresHook

    hook = PostgresHook(postgres_conn_id="postgres_operations")

    columns = hook.get_records("""
        SELECT column_name, data_type
        FROM information_schema.columns
        WHERE table_schema = 'audit'
          AND table_name = 'etl_batch_log'
        ORDER BY ordinal_position;
    """)

    print("ETL audit table structure:")
    for column in columns:
        print(column)

    rows = hook.get_records("""
        SELECT *
        FROM audit.etl_batch_log
        LIMIT 10;
    """)

    print("\nETL audit records:")
    for row in rows:
        print(row)


with DAG(
    dag_id="inspect_etl_audit",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["postgresql", "audit", "read-only"],
) as dag:

    inspect_audit = PythonOperator(
        task_id="inspect_etl_audit",
        python_callable=inspect_etl_audit,
    )
