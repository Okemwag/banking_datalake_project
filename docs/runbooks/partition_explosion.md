# Partition Explosion Runbook

1. Review the partition monitor output for skewed or overly granular partitions.
2. Verify no job introduced event-date or customer-level partitioning.
3. Run compaction and, if needed, migrate the table to ingestion-date partitioning only.
4. Backfill affected Gold marts after the repartition operation completes.

