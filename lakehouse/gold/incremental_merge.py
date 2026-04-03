from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession

from lakehouse.runtime import merge_with_retry


def merge_gold_table(
    spark: SparkSession,
    source_df: DataFrame,
    table_name: str,
    table_path: str,
    merge_condition: str,
) -> None:
    merge_with_retry(
        spark,
        source_df,
        table_name,
        table_path,
        merge_condition=merge_condition,
        partition_by=["event_date"],
    )

