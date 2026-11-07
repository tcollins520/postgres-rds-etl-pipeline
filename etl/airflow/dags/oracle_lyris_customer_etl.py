import json
import ssl
import time
from datetime import datetime, timezone
from urllib.request import Request, urlopen
from urllib.parse import urlencode

import pendulum

from airflow.sdk import DAG
from airflow.hooks.base import BaseHook
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.trigger_rule import TriggerRule


# =========================================================
# ORACLE LYRIS CONFIGURATION
# =========================================================

NIFI_CONN_ID = "nifi_api"
POSTGRES_CONN_ID = "postgres_operations"

SOURCE_SYSTEM = "LYRIS"
WORKFLOW_NAME = "ORACLE_LYRIS_CUSTOMER_INGESTION"

# Oracle LYRIS NiFi processor IDs only.
EXECUTE_SQL_ID = "66670ca8-c8e4-3450-9d0d-a592c4d7d537"
UPDATE_ATTRIBUTE_ID = "e943838f-bb6c-38da-ba5e-e35d69fb6f35"
UPDATE_RECORD_ID = "ff3fac2d-074c-32da-8244-eff7cdf39017"
PUT_DATABASE_RECORD_ID = "b283396d-45f8-3885-9489-3006c33be49b"
BUILD_PRODUCTION_SQL_ID = "0e1b87dd-4e92-39f3-8902-566031eccb05"
PUT_PRODUCTION_SQL_ID = "b59c846d-fe85-3885-8a31-7ddf86092c1a"
ERROR_LOG_ID = "48bda1b5-cd9d-3037-bd05-6feacdd76117"

LYRIS_PROCESSORS = [
    EXECUTE_SQL_ID,
    UPDATE_ATTRIBUTE_ID,
    UPDATE_RECORD_ID,
    PUT_DATABASE_RECORD_ID,
    BUILD_PRODUCTION_SQL_ID,
    PUT_PRODUCTION_SQL_ID,
    ERROR_LOG_ID,
]

# Start consumers before the Oracle query processor.
DOWNSTREAM_PROCESSORS = [
    ERROR_LOG_ID,
    PUT_PRODUCTION_SQL_ID,
    BUILD_PRODUCTION_SQL_ID,
    PUT_DATABASE_RECORD_ID,
    UPDATE_RECORD_ID,
    UPDATE_ATTRIBUTE_ID,
]


# =========================================================
# NIFI API HELPERS
# =========================================================

def nifi_connection():
    conn = BaseHook.get_connection(NIFI_CONN_ID)
    extra = conn.extra_dejson
    verify = extra.get("verify", True)

    if isinstance(verify, str):
        context = ssl.create_default_context(cafile=verify)
    else:
        context = ssl.create_default_context()

    return (
        f"{conn.host.rstrip('/')}:{conn.port}",
        conn.login,
        conn.password,
        context,
    )


def nifi_token():
    base_url, username, password, context = nifi_connection()
    body = urlencode({"username": username, "password": password}).encode()

    request = Request(
        f"{base_url}/nifi-api/access/token",
        data=body,
        headers={"Content-Type": "application/x-www-form-urlencoded"},
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
    entity = nifi_request("GET", f"/processors/{processor_id}", token)
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
    print(f"NiFi processor {processor_id}: {state}")


def stop_lyris_processors(token):
    for processor_id in LYRIS_PROCESSORS:
        try:
            set_processor_state(token, processor_id, "STOPPED")
        except Exception as exc:
            print(f"Could not stop LYRIS processor {processor_id}: {exc}")


def lyris_connection_ids(token):
    response = nifi_request(
        "GET",
        "/process-groups/root/connections",
        token,
    )

    processor_ids = set(LYRIS_PROCESSORS)
    connection_ids = []

    for item in response.get("connections", []):
        component = item.get("component", {})
        source = component.get("source", {})
        destination = component.get("destination", {})

        if (
            source.get("id") in processor_ids
            and destination.get("id") in processor_ids
        ):
            connection_ids.append(component["id"])

    if not connection_ids:
        raise RuntimeError("No Oracle LYRIS NiFi connections were found")

    return connection_ids


# =========================================================
# TASK 1: START ORACLE LYRIS NIFI FLOW
# =========================================================

def trigger_lyris_ingestion():
    token = nifi_token()

    try:
        # Consumers first; ExecuteSQL starts last to initiate one extraction.
        for processor_id in DOWNSTREAM_PROCESSORS:
            set_processor_state(token, processor_id, "RUNNING")

        set_processor_state(token, EXECUTE_SQL_ID, "RUNNING")
        print("Oracle LYRIS extraction triggered by Airflow")

    except Exception:
        stop_lyris_processors(token)
        raise


# =========================================================
# TASK 2: MONITOR FLOW AND VERIFY POSTGRESQL LOAD
# =========================================================

def monitor_lyris_ingestion():
    token = nifi_token()

    try:
        connection_ids = lyris_connection_ids(token)
        deadline = time.time() + 900
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

            print(f"Oracle LYRIS queued FlowFiles: {total_queued}")

            if total_queued == 0:
                empty_checks += 1
            else:
                empty_checks = 0

            # Require consecutive empty queue checks before database verification.
            if empty_checks >= 3:
                hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

                row = hook.get_first(
                    """
                    SELECT
                        batch_id,
                        COUNT(*) AS staging_rows,
                        MAX(loaded_at) AS latest_load
                    FROM staging.stg_lyris_customers
                    WHERE source_system = %s
                    GROUP BY batch_id
                    ORDER BY MAX(loaded_at) DESC
                    LIMIT 1;
                    """,
                    parameters=(SOURCE_SYSTEM,),
                )

                if not row:
                    raise RuntimeError(
                        "NiFi queues drained, but no LYRIS staging batch was found"
                    )

                batch_id, staging_rows, _ = row
                staging_rows = int(staging_rows)

                if staging_rows < 1:
                    raise RuntimeError(
                        f"LYRIS batch {batch_id} contains no staging records"
                    )

                production_row = hook.get_first(
                    """
                    SELECT COUNT(*)
                    FROM production.lyris_customers
                    WHERE source_system = %s
                      AND last_batch_id = %s;
                    """,
                    parameters=(SOURCE_SYSTEM, batch_id),
                )
                production_rows = int(production_row[0])

                if production_rows != staging_rows:
                    raise RuntimeError(
                        f"LYRIS batch {batch_id}: staging has {staging_rows} "
                        f"rows but production has {production_rows} rows "
                        f"for this batch"
                    )

                print(
                    f"Oracle LYRIS verified: batch={batch_id}, "
                    f"staging={staging_rows}, production={production_rows}"
                )
                return {
                    "batch_id": batch_id,
                    "records_read": staging_rows,
                    "records_loaded": production_rows,
                }

            time.sleep(5)

        raise TimeoutError(
            "Oracle LYRIS NiFi queues did not drain within 15 minutes"
        )

    finally:
        stop_lyris_processors(token)


# =========================================================
# TASK 3: AIRFLOW AUDIT
# =========================================================

def audit_lyris_run(**context):
    ti = context["ti"]
    dag_run = context["dag_run"]
    result = ti.xcom_pull(task_ids="monitor_lyris_ingestion")

    if result:
        batch_id = result["batch_id"]
        records_read = result["records_read"]
        status = "SUCCESS"
        error_message = None
    else:
        batch_id = f"LYRIS_FAILED_{dag_run.run_id[-24:]}"
        records_read = 0
        status = "FAILED"
        failed = [
            task.task_id
            for task in dag_run.get_task_instances()
            if task.state == "failed"
        ]
        error_message = (
            "Failed tasks: " + ", ".join(failed)
            if failed else "Oracle LYRIS ingestion did not complete"
        )

    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
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
            %s, 0, 0, 0, %s, %s, %s
        )
        ON CONFLICT (airflow_run_id)
        WHERE airflow_run_id IS NOT NULL
        DO UPDATE SET
            end_time = CURRENT_TIMESTAMP,
            records_read = EXCLUDED.records_read,
            batch_status = EXCLUDED.batch_status,
            error_message = EXCLUDED.error_message;
        """,
        parameters=(
            batch_id,
            WORKFLOW_NAME,
            SOURCE_SYSTEM,
            dag_run.start_date or datetime.now(timezone.utc),
            records_read,
            status,
            error_message,
            dag_run.run_id,
        ),
    )
    print(f"Oracle LYRIS Airflow audit: {batch_id} {status}")


# =========================================================
# DAG: DAILY AT 2:15 AM EASTERN
# =========================================================

with DAG(
    dag_id="oracle_lyris_customer_etl",
    description=(
        "Airflow orchestration of Oracle LYRIS extraction through NiFi "
        "and PostgreSQL staging/production loading"
    ),
    start_date=pendulum.datetime(2026, 11, 5, tz="America/New_York"),
    schedule="15 2 * * *",
    catchup=False,
    max_active_runs=1,
    tags=["oracle", "lyris", "postgresql", "nifi", "etl"],
) as dag:

    trigger_nifi = PythonOperator(
        task_id="trigger_lyris_nifi_ingestion",
        python_callable=trigger_lyris_ingestion,
    )

    monitor_nifi = PythonOperator(
        task_id="monitor_lyris_ingestion",
        python_callable=monitor_lyris_ingestion,
    )

    audit = PythonOperator(
        task_id="audit_lyris_run",
        python_callable=audit_lyris_run,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    trigger_nifi >> monitor_nifi >> audit
