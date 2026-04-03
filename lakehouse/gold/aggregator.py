from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def aggregate_daily_customer_metrics(df: DataFrame) -> DataFrame:
    return df.groupBy("event_date", "customer_id").agg(
        F.count("*").alias("transaction_count"),
        F.sum("amount").alias("gross_amount"),
        F.sum(F.when(F.col("status") == "approved", F.col("amount")).otherwise(F.lit(0))).alias(
            "approved_amount"
        ),
        F.max("risk_score").alias("max_risk_score"),
        F.max(F.col("is_late_arrival").cast("int")).alias("late_arrival_flag"),
    )


def aggregate_account_risk_metrics(df: DataFrame) -> DataFrame:
    return df.groupBy("event_date", "account_id").agg(
        F.count("*").alias("transaction_count"),
        F.sum(F.when(F.col("status") == "chargeback", F.col("amount")).otherwise(F.lit(0))).alias(
            "chargeback_amount"
        ),
        F.avg("risk_score").alias("avg_risk_score"),
        F.sum(F.when(F.col("status") == "declined", F.lit(1)).otherwise(F.lit(0))).alias("declined_count"),
    )

