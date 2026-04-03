from __future__ import annotations

from collections import defaultdict
from typing import Any

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F


def pick_latest_records(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[str, dict[str, Any]] = defaultdict(dict)
    for record in records:
        current = grouped.get(record["transaction_id"])
        if not current or record["bronze_ingested_at_utc"] >= current["bronze_ingested_at_utc"]:
            grouped[record["transaction_id"]] = record
    return list(grouped.values())


def deduplicate_transactions(df: DataFrame) -> DataFrame:
    window = Window.partitionBy("transaction_id").orderBy(
        F.col("bronze_ingested_at_utc").desc(),
        F.col("source_file").desc(),
    )
    return df.withColumn("row_num", F.row_number().over(window)).filter(F.col("row_num") == 1).drop("row_num")

