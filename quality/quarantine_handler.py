from __future__ import annotations

from pyspark.sql import DataFrame

from lakehouse.bronze.quarantine import quarantine_corrupt_records


def quarantine_failed_quality_rows(df: DataFrame, layer: str, table: str, run_id: str) -> int:
    return quarantine_corrupt_records(df, layer=layer, table=table, run_id=run_id)

