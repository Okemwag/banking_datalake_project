from __future__ import annotations

import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator


with DAG(
    dag_id="dbt_transforms_dag",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    default_args={"retries": 1},
) as dag:
    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command="dbt run --project-dir /opt/project/transforms --profiles-dir /opt/project/transforms",
    )
    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command="dbt test --project-dir /opt/project/transforms --profiles-dir /opt/project/transforms",
    )
    dbt_run >> dbt_test

