from __future__ import annotations

import pendulum
from airflow import DAG

from orchestration.plugins.dq_operator import DataQualityOperator


with DAG(
    dag_id="dq_validation_dag",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule="30 * * * *",
    catchup=False,
    default_args={"retries": 1},
) as dag:
    bronze = DataQualityOperator(task_id="validate_bronze", layer="bronze", run_id="{{ ts_nodash }}")
    silver = DataQualityOperator(task_id="validate_silver", layer="silver", run_id="{{ ts_nodash }}")
    gold = DataQualityOperator(task_id="validate_gold", layer="gold", run_id="{{ ts_nodash }}")
    bronze >> silver >> gold

