# ADR 003: Data Quality Severity

Data quality checks are tiered:

- `blocker`: fail the task and halt promotion to the next layer
- `warn`: continue the flow but emit metrics and alerts
- `info`: profile-only signal for drift or anomaly observation

Corrupt payloads and missing natural keys are blockers. Distribution shifts and elevated null rates are warnings unless they cross agreed thresholds.
