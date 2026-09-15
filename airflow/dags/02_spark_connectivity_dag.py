from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator


SSH_OPTIONS = (
    "-i /home/airflow/.ssh/id_rsa "
    "-o IdentitiesOnly=yes "
    "-o BatchMode=yes "
    "-o StrictHostKeyChecking=no "
    "-p 22023"
)

SPARK_HOST = "developer@host.docker.internal"

SPARK_JOB = (
    "/workspace/nyc-building-risk/"
    "spark/jobs/00_airflow_spark_test.py"
)


default_args = {
    "owner": "nyc-building-risk",
    "retries": 1,
}


with DAG(
    dag_id="nyc_spark_connectivity",
    description="Run Spark connectivity test from Airflow",
    default_args=default_args,
    start_date=datetime(2026, 9, 1),
    schedule_interval=None,
    catchup=False,
    tags=[
        "nyc-building-risk",
        "spark",
    ],
) as dag:

    start = DummyOperator(
        task_id="start"
    )

    check_spark = BashOperator(
        task_id="check_spark",
        bash_command=(
            "ssh "
            + SSH_OPTIONS
            + " "
            + SPARK_HOST
            + " 'spark-submit --version'"
        ),
    )

    run_spark_test = BashOperator(
        task_id="run_spark_test",
        bash_command=(
            "ssh "
            + SSH_OPTIONS
            + " "
            + SPARK_HOST
            + " 'cd /workspace/nyc-building-risk && "
            + "spark-submit spark/jobs/00_airflow_spark_test.py'"
        ),
    )

    end = DummyOperator(
        task_id="end"
    )

    start >> check_spark >> run_spark_test >> end