from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator


default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


with DAG(
    dag_id="nyc_property_risk_pipeline",
    default_args=default_args,
    description="Production Property Risk pipeline",
    start_date=datetime(2026, 9, 1),
    schedule_interval=None,
    catchup=False,
    tags=["nyc", "gold", "risk", "property"],
) as dag:

    start = DummyOperator(
        task_id="start"
    )

    property_risk = BashOperator(
        task_id="property_risk",
        bash_command=(
            "ssh -i /home/airflow/.ssh/id_rsa "
            "-o IdentitiesOnly=yes "
            "-o BatchMode=yes "
            "-o StrictHostKeyChecking=no "
            "-p 22023 developer@host.docker.internal "
            "\"cd /workspace/nyc-building-risk && "
            "MINIO_ENDPOINT=host.docker.internal:9001 "
            "spark-submit spark/gold/property_risk.py\""
        ),
    )

    end = DummyOperator(
        task_id="end"
    )

    start >> property_risk >> end
