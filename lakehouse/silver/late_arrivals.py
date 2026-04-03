from __future__ import annotations

from datetime import date, timedelta

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def is_late_arrival(event_date: date, reference_date: date, watermark_days: int) -> bool:
    return event_date < (reference_date - timedelta(days=watermark_days))


def mark_late_arrivals(df: DataFrame, watermark_days: int) -> DataFrame:
    return df.withColumn(
        "is_late_arrival",
        F.col("event_date") < F.date_sub(F.current_date(), watermark_days),
    )

