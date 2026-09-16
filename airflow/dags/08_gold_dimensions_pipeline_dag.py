from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator


DAG_ID = "nyc_gold_dimensions_pipeline"


default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


REMOTE_COMMAND = """
ssh -i /home/airflow/.ssh/id_rsa \
-o IdentitiesOnly=yes \
-o BatchMode=yes \
-o StrictHostKeyChecking=no \
-p 22023 developer@host.docker.internal \
"cd /workspace/nyc-building-risk && \
spark-submit spark/gold/gold_dimensions.py"
"""


with DAG(
    dag_id=DAG_ID,
    default_args=default_args,
    start_date=datetime(2026, 9, 1),
    schedule_interval=None,
    catchup=False,
    tags=["nyc", "gold", "dimensions"],
) as dag:

    start = DummyOperator(
        task_id="start"
    )

    gold_dimensions = BashOperator(
        task_id="gold_dimensions",
        bash_command=REMOTE_COMMAND,
    )

    end = DummyOperator(
        task_id="end"
    )

    start >> gold_dimensions >> end