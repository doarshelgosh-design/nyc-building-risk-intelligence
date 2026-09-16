from datetime import datetime

from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator


DAG_ID = "nyc_master_pipeline"

PIPELINES = [
    ("run_311", "nyc_311_daily_pipeline"),
    ("run_hpd", "nyc_hpd_daily_pipeline"),
    ("run_dob", "nyc_dob_daily_pipeline"),
    ("run_pluto", "nyc_pluto_snapshot_pipeline"),
    ("run_building_identity", "nyc_building_identity_pipeline"),
    ("run_gold_dimensions", "nyc_gold_dimensions_pipeline"),
    ("run_gold_facts", "nyc_gold_facts_pipeline"),
    ("run_building_risk", "nyc_building_risk_pipeline"),
    ("run_property_risk", "nyc_property_risk_pipeline"),
    ("run_serving_layer", "nyc_serving_layer_pipeline"),
    ("run_elasticsearch_refresh", "nyc_elasticsearch_refresh_pipeline"),
]


with DAG(
    dag_id=DAG_ID,
    description="End-to-end NYC Building Risk production pipeline",
    start_date=datetime(2026, 9, 1),
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
    tags=["nyc", "master", "production"],
) as dag:

    start = DummyOperator(
        task_id="start"
    )

    end = DummyOperator(
        task_id="end"
    )

    previous_task = start

    for task_id, child_dag_id in PIPELINES:
        trigger_task = TriggerDagRunOperator(
            task_id=task_id,
            trigger_dag_id=child_dag_id,
            wait_for_completion=True,
            poke_interval=30,
            reset_dag_run=False,
        )

        previous_task >> trigger_task
        previous_task = trigger_task

    previous_task >> end
