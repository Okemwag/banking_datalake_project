# Schema Drift Runbook

1. Check the Bronze schema evolution metrics and the quarantine table.
2. Confirm whether the new or changed fields are backward compatible.
3. Update canonical column mappings in Silver if the new field should be promoted.
4. Re-run the affected Silver and Gold windows through the backfill script.
5. Record the change in the catalog YAML and notify downstream owners.

