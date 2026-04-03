from __future__ import annotations

from lakehouse.runtime import build_spark_session


def compact_table(table_name: str) -> None:
    spark = build_spark_session("compact-table")
    spark.sql(f"OPTIMIZE {table_name}")

