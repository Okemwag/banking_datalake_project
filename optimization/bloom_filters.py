from __future__ import annotations

from lakehouse.runtime import build_spark_session


def enable_bloom_filters(table_name: str, columns: list[str]) -> None:
    spark = build_spark_session("bloom-filter-config")
    for column in columns:
        spark.sql(
            f"ALTER TABLE {table_name} SET TBLPROPERTIES ('delta.bloomFilter.enabled.{column}'='true')"
        )

