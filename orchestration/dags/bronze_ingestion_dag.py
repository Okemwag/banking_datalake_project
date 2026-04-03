from __future__ import annotations

import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator

from orchestration.plugins.delta_operator import DeltaCommandOperator
from orchestration.utils.sla_callbacks import sla_miss_callback


with DAG(
    dag_id="bronze_ingestion_dag",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule="*/15 * * * *",
    catchup=False,
    default_args={"retries": 2},
    sla_miss_callback=sla_miss_callback,
) as dag:
    seed_transactions = BashOperator(
        task_id="seed_transactions",
        bash_command=(
            "python /opt/project/scripts/seed_kafka.py "
            "--topic transactions --record-count 250 --run-id {{ ts_nodash }} --run-date {{ ds }}"
        ),
    )

    write_bronze = DeltaCommandOperator(
        task_id="write_transactions_bronze",
        callable_path="lakehouse.bronze.writer.write_transactions_bronze",
        kwargs={"run_date": "{{ ds }}", "run_id": "{{ ts_nodash }}"},
    )

    seed_transactions >> write_bronze

