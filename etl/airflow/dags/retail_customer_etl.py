import csv
import json
import shutil
import ssl
import time
import uuid

from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from airflow.sdk import DAG
from airflow.hooks.base import BaseHook
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.trigger_rule import TriggerRule


# =========================================================
# CONFIGURATION
# =========================================================

SOURCE_SYSTEM = "RETAIL_CSV"
WORKFLOW_NAME = "CSV_CUSTOMER_INGESTION"

POSTGRES_CONN_ID = "postgres_operations"
NIFI_CONN_ID = "nifi_api"

NIFI_SOURCE_FILE = Path("/opt/nifi/data/incoming/customers.csv")
ARCHIVE_DIR = Path("/home/ec2-user/airflow/archive")

GETFILE_ID = "a46f2bd2-01a1-1000-d7e2-03e7631a193f"
UPDATE_ATTRIBUTE_ID = "a83fff7e-01a1-1000-565a-f3f1eb2646f9"
UPDATE_RECORD_ID = "b307cbf6-01a1-1000-c008-3287f79df89d"
PUT_DATABASE_RECORD_ID = "a472746c-01a1-1000-cfb8-5f02a384af14"
REPLACE_TEXT_ID = "a846c612-01a1-1000-cbc9-f1a0abf9de6f"
FAILURE_AUDIT_ID = "a85c0e6b-01a1-1000-1497-b512b9aa40e8"
PUT_SQL_ID = "a83e0c47-01a1-1000-7c7b-c2bcaa827563"

RETAIL_PROCESSORS = [
    GETFILE_ID,
    UPDATE_ATTRIBUTE_ID,
    UPDATE_RECORD_ID,
    PUT_DATABASE_RECORD_ID,
    REPLACE_TEXT_ID,
    FAILURE_AUDIT_ID,
    PUT_SQL_ID,
]

DOWNSTREAM_PROCESSORS = [
    PUT_SQL_ID,
    FAILURE_AUDIT_ID,
    REPLACE_TEXT_ID,
    PUT_DATABASE_RECORD_ID,
    UPDATE_RECORD_ID,
    UPDATE_ATTRIBUTE_ID,
]


# =========================================================
# NIFI API
# =========================================================

def nifi_connection():
    conn = BaseHook.get_connection(NIFI_CONN_ID)
    extra = conn.extra_dejson

    verify = extra.get("verify", True)

    if isinstance(verify, str):
        context = ssl.create_default_context(cafile=verify)
    else:
        context = ssl.create_default_context()

    return f"{conn.host.rstrip('/')}:{conn.port}", conn.login, conn.password, context


def nifi_token():
    base_url, username, password, context = nifi_connection()

    body = urlencode({
        "username": username,
        "password": password,
    }).encode()

    request = Request(
        f"{base_url}/nifi-api/access/token",
        data=body,
        headers={
            "Content-Type": "application/x-www-form-urlencoded"
        },
        method="POST",
    )

    with urlopen(request, context=context, timeout=30) as response:
        token = response.read().decode().strip()

    if not token:
        raise RuntimeError("NiFi returned an empty access token")

    return token


def nifi_request(method, endpoint, token, payload=None):
    base_url, _, _, context = nifi_connection()

    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/json",
    }

    body = None

    if payload is not None:
        body = json.dumps(payload).encode()
        headers["Content-Type"] = "application/json"

    request = Request(
        f"{base_url}/nifi-api{endpoint}",
        data=body,
        headers=headers,
        method=method,
    )

    with urlopen(request, context=context, timeout=30) as response:
        result = response.read().decode()

    return json.loads(result) if result else {}


def set_processor_state(token, processor_id, state):
    entity = nifi_request(
        "GET",
        f"/processors/{processor_id}",
        token,
    )

    current_state = entity["component"].get("state")

    if current_state == state:
        return

    payload = {
        "revision": entity["revision"],
        "state": state,
    }

    nifi_request(
        "PUT",
        f"/processors/{processor_id}/run-status",
        token,
        payload,
    )

    print(f"Processor {processor_id}: {state}")


def stop_retail_processors(token):
    for processor_id in RETAIL_PROCESSORS:
        try:
            set_processor_state(token, processor_id, "STOPPED")
        except Exception as exc:
            print(f"Unable to stop {processor_id}: {exc}")


def update_nifi_batch_id(token, batch_id):
    entity = nifi_request(
        "GET",
        f"/processors/{UPDATE_ATTRIBUTE_ID}",
        token,
    )

    payload = {
        "revision": entity["revision"],
        "component": {
            "id": UPDATE_ATTRIBUTE_ID,
            "config": {
                "properties": {
                    "batch_id": batch_id,
                }
            },
        },
    }

    nifi_request(
        "PUT",
        f"/processors/{UPDATE_ATTRIBUTE_ID}",
        token,
        payload,
    )

    print(f"NiFi batch ID updated: {batch_id}")


# =========================================================
# TASK 1: GENERATE BATCH ID
# =========================================================

def generate_batch_id():
    batch_id = (
        "BATCH_"
        + datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
        + "_"
        + uuid.uuid4().hex[:8].upper()
    )

    print(f"Generated batch ID: {batch_id}")
    return batch_id


# =========================================================
# TASK 2: ARCHIVE SOURCE FILE
# =========================================================

def archive_source_file():
    if not NIFI_SOURCE_FILE.is_file():
        raise FileNotFoundError(
            f"Source file not found: {NIFI_SOURCE_FILE}"
        )

    batch_id = None

    # The batch ID is supplied by the calling task through XCom.
    # This function is wrapped by archive_source_task below.
    raise RuntimeError("Use archive_source_task")


def archive_source_task(**context):
    batch_id = context["ti"].xcom_pull(
        task_ids="generate_batch_id"
    )

    if not NIFI_SOURCE_FILE.is_file():
        raise FileNotFoundError(
            f"Source file not found: {NIFI_SOURCE_FILE}"
        )

    ARCHIVE_DIR.mkdir(parents=True, exist_ok=True)

    archive_file = ARCHIVE_DIR / f"customers_{batch_id}.csv"

    shutil.copy2(NIFI_SOURCE_FILE, archive_file)

    with archive_file.open(
        "r",
        encoding="utf-8-sig",
        newline="",
    ) as file:
        expected_records = max(
            sum(1 for _ in csv.reader(file)) - 1,
            0,
        )

    if expected_records == 0:
        raise ValueError("CSV contains no data records")

    print(
        f"Archived {archive_file}; "
        f"expected records: {expected_records}"
    )

    return {
        "archive_file": str(archive_file),
        "expected_records": expected_records,
    }


# =========================================================
# TASK 3: TRIGGER NIFI
# =========================================================

def trigger_nifi_ingestion(**context):
    batch_id = context["ti"].xcom_pull(
        task_ids="generate_batch_id"
    )

    token = nifi_token()

    try:
        if not NIFI_SOURCE_FILE.is_file():
            raise FileNotFoundError(
                "NiFi source CSV is missing"
            )

        update_nifi_batch_id(token, batch_id)

        # Start downstream processors first.
        for processor_id in DOWNSTREAM_PROCESSORS:
            set_processor_state(
                token,
                processor_id,
                "RUNNING",
            )

        # Start GetFile last.
        set_processor_state(
            token,
            GETFILE_ID,
            "RUNNING",
        )

        # GetFile polls every 30 seconds.
        deadline = time.time() + 240

        while time.time() < deadline:
            if not NIFI_SOURCE_FILE.exists():
                print("NiFi retrieved customers.csv")
                break

            time.sleep(5)
        else:
            raise TimeoutError(
                "NiFi did not retrieve the CSV within 240 seconds"
            )

        # Stop polling for additional files.
        set_processor_state(
            token,
            GETFILE_ID,
            "STOPPED",
        )

        print(f"NiFi ingestion started for {batch_id}")

    except Exception:
        stop_retail_processors(token)
        raise


# =========================================================
# TASK 4: MONITOR NIFI
# =========================================================

def monitor_nifi_ingestion(**context):
    ti = context["ti"]
    batch_id = ti.xcom_pull(task_ids="generate_batch_id")
    archive = ti.xcom_pull(task_ids="archive_source_file")
    expected_records = archive["expected_records"]

    if not batch_id or expected_records is None:
        raise RuntimeError("Missing batch ID or expected CSV record count")

    token = nifi_token()

    try:
        response = nifi_request(
            "GET",
            "/process-groups/root/connections",
            token,
        )

        processor_ids = set(RETAIL_PROCESSORS)
        connection_ids = []

        for item in response.get("connections", []):
            component = item.get("component", {})
            source_processor = component.get("source", {})
            destination = component.get("destination", {})

            if (
                source_processor.get("id") in processor_ids
                and destination.get("id") in processor_ids
            ):
                connection_ids.append(component["id"])

        if not connection_ids:
            raise RuntimeError("No Retail CSV connections found")

        deadline = time.time() + 600
        empty_checks = 0

        while time.time() < deadline:
            total_queued = 0

            for connection_id in connection_ids:
                status = nifi_request(
                    "GET",
                    f"/flow/connections/{connection_id}/status",
                    token,
                )
                snapshot = status.get(
                    "connectionStatus", {}
                ).get("aggregateSnapshot", {})

                total_queued += int(snapshot.get("queuedCount", 0))

            print(f"NiFi queued FlowFiles: {total_queued}")

            if total_queued == 0:
                empty_checks += 1
            else:
                empty_checks = 0

            if empty_checks >= 3:
                print("NiFi queues drained; checking PostgreSQL")

                hook = PostgresHook(
                    postgres_conn_id=POSTGRES_CONN_ID
                )

                for attempt in range(12):
                    row = hook.get_first(
                        """
                        SELECT COUNT(*)
                        FROM staging.stg_customers
                        WHERE batch_id = %s
                        """,
                        parameters=(batch_id,),
                    )

                    actual_records = int(row[0])

                    print(
                        f"Batch {batch_id}: "
                        f"{actual_records}/{expected_records} records"
                    )

                    if actual_records == expected_records:
                        print("PostgreSQL ingestion verified")
                        return True

                    if actual_records > expected_records:
                        raise RuntimeError(
                            f"Batch {batch_id} has {actual_records} "
                            f"records; expected {expected_records}"
                        )

                    time.sleep(5)

                raise RuntimeError(
                    f"Batch {batch_id} ingestion incomplete: "
                    f"expected {expected_records}, "
                    f"found {actual_records}"
                )

            time.sleep(5)

        raise TimeoutError(
            "NiFi queues did not drain within 10 minutes"
        )

    finally:
        stop_retail_processors(token)


# =========================================================
# TASK 5: VALIDATE STAGING
# =========================================================

def validate_staging(**context):
    ti = context["ti"]

    batch_id = ti.xcom_pull(
        task_ids="generate_batch_id"
    )

    archive = ti.xcom_pull(
        task_ids="archive_source_file"
    )

    expected_records = archive["expected_records"]

    hook = PostgresHook(
        postgres_conn_id=POSTGRES_CONN_ID
    )

    row = hook.get_first(
        """
        SELECT COUNT(*)
        FROM staging.stg_customers
        WHERE batch_id = %s
          AND source_system = %s;
        """,
        parameters=(batch_id, SOURCE_SYSTEM),
    )

    record_count = row[0]

    if record_count == 0:
        raise ValueError(
            f"No staging records found for {batch_id}"
        )

    if record_count != expected_records:
        raise ValueError(
            f"Expected {expected_records} records; "
            f"found {record_count}"
        )

    print(
        f"Validated batch {batch_id}: {record_count} records"
    )

    return {
        "batch_id": batch_id,
        "records_read": record_count,
    }


# =========================================================
# TASK 6: EXISTING SCD TYPE 2 LOAD
# =========================================================

def load_customer_dimension(**context):
    batch = context["ti"].xcom_pull(
        task_ids="validate_staging"
    )

    hook = PostgresHook(
        postgres_conn_id=POSTGRES_CONN_ID
    )

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


# =========================================================
# TASK 7: EXISTING AUDIT LOGIC WITH DYNAMIC BATCH ID
# =========================================================

def audit_etl_run(**context):
    ti = context["ti"]
    dag_run = context["dag_run"]

    batch_id = ti.xcom_pull(
        task_ids="generate_batch_id"
    )

    batch = ti.xcom_pull(
        task_ids="validate_staging"
    )

    results = ti.xcom_pull(
        task_ids="load_customer_dimension"
    )

    hook = PostgresHook(
        postgres_conn_id=POSTGRES_CONN_ID
    )

    failed_tasks = []

    if batch is None:
        failed_tasks.append(
            "validate_staging=failed_or_skipped"
        )

    if batch is not None and results is None:
        failed_tasks.append(
            "load_customer_dimension=failed_or_skipped"
        )

    batch_status = "FAILED" if failed_tasks else "SUCCESS"

    error_message = (
        "; ".join(failed_tasks) if failed_tasks else None
    )

    records_read = 0

    if batch_id:
        records_read = hook.get_first(
            """
            SELECT COUNT(*)
            FROM staging.stg_customers
            WHERE batch_id = %s
              AND source_system = %s;
            """,
            parameters=(batch_id, SOURCE_SYSTEM),
        )[0]

    else:
        batch_id = f"FAILED_{uuid.uuid4().hex[:12]}"

    records_inserted = (
        results["records_inserted"] if results else 0
    )

    records_updated = (
        results["records_updated"] if results else 0
    )

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
            batch_id,
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
        f"Audit written: {dag_run.run_id}, "
        f"{batch_id}, {batch_status}"
    )


# =========================================================
# DAG DEFINITION
# =========================================================

with DAG(
    dag_id="retail_customer_etl",
    description=(
        "Airflow orchestrated NiFi ingestion, "
        "PostgreSQL SCD Type 2 load, and audit"
    ),
    start_date=datetime(2026, 11, 5),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["retail", "postgresql", "nifi", "etl"],
) as dag:

    generate_batch = PythonOperator(
        task_id="generate_batch_id",
        python_callable=generate_batch_id,
    )

    archive_file = PythonOperator(
        task_id="archive_source_file",
        python_callable=archive_source_task,
    )

    trigger_nifi = PythonOperator(
        task_id="trigger_nifi_ingestion",
        python_callable=trigger_nifi_ingestion,
    )

    monitor_nifi = PythonOperator(
        task_id="monitor_nifi_ingestion",
        python_callable=monitor_nifi_ingestion,
    )

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

    (
        generate_batch
        >> archive_file
        >> trigger_nifi
        >> monitor_nifi
        >> validate
        >> load_dimension
        >> audit
    )
