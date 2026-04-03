from __future__ import annotations

from datetime import date

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F


COMMON_TIMEZONE_ALIASES = {
    None: "UTC",
    "": "UTC",
    "z": "UTC",
    "utc": "UTC",
    "gmt": "UTC",
    "est": "America/New_York",
}


def normalize_timezone_name(value: str | None) -> str:
    if value is None:
        return "UTC"
    return COMMON_TIMEZONE_ALIASES.get(value.strip().lower(), value)


def normalized_event_timestamp(column_name: str, timezone_column: str) -> Column:
    return F.coalesce(
        F.to_timestamp(F.col(column_name)),
        F.to_utc_timestamp(F.to_timestamp(F.col(column_name)), F.coalesce(F.col(timezone_column), F.lit("UTC"))),
    )


def add_event_timestamp_utc(df: DataFrame) -> DataFrame:
    return df.withColumn("event_ts_utc", normalized_event_timestamp("event_ts", "event_timezone"))


def event_date_from_timestamp(df: DataFrame) -> DataFrame:
    return df.withColumn("event_date", F.to_date("event_ts_utc"))


def date_from_iso(value: str) -> date:
    return date.fromisoformat(value)

