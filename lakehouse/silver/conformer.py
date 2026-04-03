from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


CANONICAL_COLUMNS = [
    "transaction_id",
    "customer_id",
    "account_id",
    "amount",
    "currency",
    "merchant_name",
    "merchant_category",
    "event_ts",
    "event_timezone",
    "status",
    "payment_channel",
    "risk_score",
    "merchant_region",
]


def ensure_columns(df: DataFrame) -> DataFrame:
    conformed = df
    for column in CANONICAL_COLUMNS:
        if column not in conformed.columns:
            conformed = conformed.withColumn(column, F.lit(None).cast("string"))
    return conformed

