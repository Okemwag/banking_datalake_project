from __future__ import annotations

from lakehouse.runtime import build_spark_session


def zorder_table(table_name: str, columns: list[str]) -> None:
    spark = build_spark_session("zorder-table")
    spark.sql(f"OPTIMIZE {table_name} ZORDER BY ({', '.join(columns)})")

