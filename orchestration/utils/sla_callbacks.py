from __future__ import annotations

from orchestration.utils.slack_alerts import send_slack_alert


def sla_miss_callback(dag, task_list, blocking_task_list, slas, blocking_tis):
    send_slack_alert(f"SLA miss in DAG {dag.dag_id}: tasks={task_list}")
