from __future__ import annotations

import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator


with DAG(
    dag_id="backfill_dag",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    params={"start_date": "2026-01-01", "end_date": "2026-01-07"},
) as dag:
    BashOperator(
        task_id="run_backfill",
        bash_command=(
            "python /opt/project/scripts/backfill.py "
            "--start-date {{ params.start_date }} --end-date {{ params.end_date }}"
        ),
    )

