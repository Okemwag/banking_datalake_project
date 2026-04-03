from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def profile_dataframe(df: DataFrame, key_columns: list[str]) -> dict[str, float]:
    metrics = {"row_count": float(df.count())}
    total_rows = max(metrics["row_count"], 1.0)
    for column in key_columns:
        null_rows = df.filter(F.col(column).isNull()).count()
        metrics[f"{column}_null_ratio"] = null_rows / total_rows
    return metrics

