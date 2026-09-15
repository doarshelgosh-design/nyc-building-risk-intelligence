from datetime import datetime

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator


SSH_BASE = (
    "ssh "
    "-i /home/airflow/.ssh/id_rsa "
    "-o IdentitiesOnly=yes "
    "-o BatchMode=yes "
    "-o StrictHostKeyChecking=no "
    "-p 22023 "
    "developer@host.docker.internal "
)


with DAG(
    dag_id="nyc_dob_daily_pipeline",
    start_date=datetime(2026, 9, 15),
    schedule_interval=None,
    catchup=False,
    tags=["nyc", "dob", "bronze", "silver"],
) as dag:

    start = DummyOperator(
        task_id="start"
    )

    ingest_dob = BashOperator(
        task_id="ingest_dob",
        bash_command=(
            SSH_BASE
            + "\""
            + "cd /workspace/nyc-building-risk && "
            + "MINIO_ENDPOINT=host.docker.internal:9001 "
            + "python3 ingestion/dob/daily_ingestion.py"
            + "\""
        ),
    )

    silver_dob = BashOperator(
        task_id="silver_dob",
        bash_command=(
            SSH_BASE
            + "\""
            + "cd /workspace/nyc-building-risk && "
            + "spark-submit "
            + "spark/silver/dob/transform.py"
            + "\""
        ),
    )

    end = DummyOperator(
        task_id="end"
    )

    start >> ingest_dob >> silver_dob >> end