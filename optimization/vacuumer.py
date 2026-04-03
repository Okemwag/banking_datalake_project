from __future__ import annotations

from lakehouse.runtime import build_spark_session


def vacuum_table(table_name: str, retention_hours: int = 168) -> None:
    spark = build_spark_session("vacuum-table")
    spark.sql(f"VACUUM {table_name} RETAIN {retention_hours} HOURS")

