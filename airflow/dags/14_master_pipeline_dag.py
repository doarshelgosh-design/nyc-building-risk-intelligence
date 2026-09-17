from datetime import timedelta

import pendulum

from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator


DAG_ID = "nyc_master_pipeline"

ISRAEL_TZ = pendulum.timezone("Asia/Jerusalem")


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

    # Timezone-aware start date.
    # The schedule below will therefore follow Israel local time,
    # including daylight-saving-time changes.
    start_date=pendulum.datetime(
        2026,
        9,
        1,
        6,
        0,
        tz=ISRAEL_TZ,
    ),

    # Run every day at 06:00 Israel time.
    schedule_interval="0 6 * * *",

    # Do not create historical runs for dates that were missed.
    catchup=False,

    # Never allow two Master runs at the same time.
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

            # Child DAG that should be executed.
            trigger_dag_id=child_dag_id,

            # Use the same logical execution date as the Master DAG.
            # This also makes retries deterministic.
            execution_date="{{ execution_date }}",

            # Wait until the child DAG finishes.
            wait_for_completion=True,

            # Check child DAG state every 30 seconds.
            poke_interval=30,

            # If this same child execution already exists,
            # reset it instead of creating a conflicting DagRun.
            reset_dag_run=True,

            # Retry the Master trigger once if it fails.
            retries=1,

            # Wait 5 minutes before retrying.
            retry_delay=timedelta(minutes=5),
        )

        previous_task >> trigger_task
        previous_task = trigger_task

    previous_task >> end