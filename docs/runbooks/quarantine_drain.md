# Quarantine Drain Runbook

1. Inspect quarantined records and group by `quarantine_reason`.
2. Fix parser or conforming logic if the failure is systemic.
3. Reprocess the targeted quarantine path with `scripts/reprocess_quarantine.py`.
4. Confirm the replayed records landed in Silver and Gold without duplicate merges.

