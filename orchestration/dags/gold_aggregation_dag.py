from __future__ import annotations

import pendulum
from airflow import DAG

from orchestration.plugins.delta_operator import DeltaCommandOperator


with DAG(
    dag_id="gold_aggregation_dag",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule="0 2 * * *",
    catchup=False,
    default_args={"retries": 2},
) as dag:
    DeltaCommandOperator(
        task_id="build_gold_marts",
        callable_path="lakehouse.gold.mart_builder.build_gold_marts",
        kwargs={"run_id": "{{ ts_nodash }}"},
    )

