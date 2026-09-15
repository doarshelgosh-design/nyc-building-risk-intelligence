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
    dag_id="nyc_311_daily_pipeline",
    start_date=datetime(2026, 9, 15),
    schedule_interval=None,
    catchup=False,
    tags=["nyc", "311", "bronze", "silver"],
) as dag:

    start = DummyOperator(
        task_id="start"
    )

    ingest_311 = BashOperator(
        task_id="ingest_311",
        bash_command=(
            SSH_BASE
            + "\""
            + "cd /workspace/nyc-building-risk && "
            + "MINIO_ENDPOINT=host.docker.internal:9001 "
            + "python3 ingestion/nyc_311/daily_ingestion.py"
            + "\""
        ),
    )

    silver_311 = BashOperator(
        task_id="silver_311",
        bash_command=(
            SSH_BASE
            + "\""
            + "cd /workspace/nyc-building-risk && "
            + "spark-submit "
            + "spark/silver/nyc_311/transform.py"
            + "\""
        ),
    )

    end = DummyOperator(
        task_id="end"
    )

    start >> ingest_311 >> silver_311 >> end