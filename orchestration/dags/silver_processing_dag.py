from __future__ import annotations

import pendulum
from airflow import DAG

from orchestration.plugins.delta_operator import DeltaCommandOperator


with DAG(
    dag_id="silver_processing_dag",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule="5 * * * *",
    catchup=False,
    default_args={"retries": 2},
) as dag:
    DeltaCommandOperator(
        task_id="process_transactions_silver",
        callable_path="lakehouse.silver.processor.process_transactions_silver",
        kwargs={"run_id": "{{ ts_nodash }}"},
    )

