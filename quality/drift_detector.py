from __future__ import annotations


def detect_metric_drift(baseline: dict[str, float], current: dict[str, float], tolerance: float = 0.1) -> dict[str, float]:
    drift: dict[str, float] = {}
    for metric_name, baseline_value in baseline.items():
        current_value = current.get(metric_name)
        if current_value is None:
            continue
        if abs(current_value - baseline_value) > tolerance:
            drift[metric_name] = current_value - baseline_value
    return drift

