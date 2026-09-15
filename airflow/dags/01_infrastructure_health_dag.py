from datetime import datetime

import sys
import urllib.request

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator


# ============================================================
# CONFIG
# ============================================================

MINIO_HEALTH_URL = (
    "http://host.docker.internal:9001/minio/health/live"
)

ELASTICSEARCH_URL = (
    "http://host.docker.internal:9200"
)


# ============================================================
# TASK FUNCTIONS
# ============================================================

def check_python():
    """
    Verify that Python tasks can run inside Airflow.
    """

    print("Python task is running successfully.")
    print("Python version:")
    print(sys.version)


def check_service(url, service_name):
    """
    Verify that an HTTP service is reachable from Airflow.
    """

    print(
        "Checking {} at {}".format(
            service_name,
            url
        )
    )

    try:

        response = urllib.request.urlopen(
            url,
            timeout=10
        )

        status_code = response.getcode()

        print(
            "{} HTTP status: {}".format(
                service_name,
                status_code
            )
        )

        if status_code != 200:
            raise Exception(
                "{} returned HTTP {}".format(
                    service_name,
                    status_code
                )
            )

        print(
            "{} connection successful.".format(
                service_name
            )
        )

    except Exception as error:

        print(
            "{} check failed: {}".format(
                service_name,
                error
            )
        )

        raise


def check_minio():

    check_service(
        MINIO_HEALTH_URL,
        "MinIO"
    )


def check_elasticsearch():

    check_service(
        ELASTICSEARCH_URL,
        "Elasticsearch"
    )


# ============================================================
# DAG
# ============================================================

default_args = {
    "owner": "nyc-building-risk",
    "retries": 1
}


with DAG(
    dag_id="nyc_infrastructure_health",
    description=(
        "Checks Airflow, MinIO and Elasticsearch connectivity"
    ),
    default_args=default_args,
    start_date=datetime(2026, 9, 1),
    schedule_interval=None,
    catchup=False,
    tags=[
        "nyc-building-risk",
        "infrastructure"
    ],
) as dag:

    start = DummyOperator(
        task_id="start"
    )

    check_python_task = PythonOperator(
        task_id="check_python",
        python_callable=check_python
    )

    check_minio_task = PythonOperator(
        task_id="check_minio",
        python_callable=check_minio
    )

    check_elasticsearch_task = PythonOperator(
        task_id="check_elasticsearch",
        python_callable=check_elasticsearch
    )

    end = DummyOperator(
        task_id="end"
    )


    start >> check_python_task

    check_python_task >> [
        check_minio_task,
        check_elasticsearch_task
    ]

    [
        check_minio_task,
        check_elasticsearch_task
    ] >> end