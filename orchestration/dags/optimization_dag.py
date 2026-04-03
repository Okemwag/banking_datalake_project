from __future__ import annotations

import pendulum
from airflow import DAG

from orchestration.plugins.delta_operator import DeltaCommandOperator


with DAG(
    dag_id="optimization_dag",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule="0 3 * * *",
    catchup=False,
    default_args={"retries": 1},
) as dag:
    compact = DeltaCommandOperator(
        task_id="compact_silver_transactions",
        callable_path="optimization.compactor.compact_table",
        kwargs={"table_name": "silver.transactions_clean"},
    )
    zorder = DeltaCommandOperator(
        task_id="zorder_silver_transactions",
        callable_path="optimization.zordering.zorder_table",
        kwargs={"table_name": "silver.transactions_clean", "columns": ["customer_id", "event_date"]},
    )
    vacuum = DeltaCommandOperator(
        task_id="vacuum_silver_transactions",
        callable_path="optimization.vacuumer.vacuum_table",
        kwargs={"table_name": "silver.transactions_clean", "retention_hours": 168},
    )
    compact >> zorder >> vacuum

