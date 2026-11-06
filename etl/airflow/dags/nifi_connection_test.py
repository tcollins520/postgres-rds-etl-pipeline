from datetime import datetime
from urllib.parse import urlsplit

import requests
from airflow.sdk import DAG
from airflow.providers.standard.operators.python import PythonOperator


def test_nifi_connection():
    from airflow.sdk.bases.hook import BaseHook

    conn = BaseHook.get_connection("nifi_api")

    base_url = conn.host.rstrip("/")
    if urlsplit(base_url).port is None and conn.port:
        base_url = f"{base_url}:{conn.port}"

    response = requests.post(
        f"{base_url}/nifi-api/access/token",
        data={
            "username": conn.login,
            "password": conn.password,
        },
        verify=conn.extra_dejson["verify"],
        timeout=30,
    )

    if response.status_code != 201:
        raise RuntimeError(
            f"NiFi authentication failed: HTTP {response.status_code}"
        )

    print("NiFi authentication successful.")
    print("TLS certificate verification successful.")


with DAG(
    dag_id="nifi_connection_test",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["integration-test"],
) as dag:

    test_connection = PythonOperator(
        task_id="test_nifi_connection",
        python_callable=test_nifi_connection,
    )
