from __future__ import annotations

from datetime import UTC, datetime


def build_openlineage_event(job_name: str, run_id: str, inputs: list[str], outputs: list[str]) -> dict[str, object]:
    return {
        "eventTime": datetime.now(UTC).isoformat(),
        "eventType": "COMPLETE",
        "job": {"namespace": "banking-lakehouse", "name": job_name},
        "run": {"runId": run_id},
        "inputs": [{"namespace": "lakehouse", "name": name} for name in inputs],
        "outputs": [{"namespace": "lakehouse", "name": name} for name in outputs],
    }

