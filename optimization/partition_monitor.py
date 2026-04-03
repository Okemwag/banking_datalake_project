from __future__ import annotations

from pyspark.sql import functions as F

from lakehouse.runtime import build_spark_session


def inspect_partition_cardinality(table_name: str, partition_column: str) -> dict[str, int]:
    spark = build_spark_session("partition-monitor")
    df = spark.table(table_name)
    return {
        "partition_count": df.select(partition_column).distinct().count(),
        "row_count": df.count(),
        "max_rows_per_partition": (
            df.groupBy(partition_column).agg(F.count("*").alias("rows")).agg(F.max("rows")).first()[0]
        ),
    }

