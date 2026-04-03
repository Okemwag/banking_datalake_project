from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def clean_transactions(df: DataFrame) -> DataFrame:
    return (
        df.withColumn("transaction_id", F.trim(F.col("transaction_id")).cast("string"))
        .withColumn("customer_id", F.trim(F.col("customer_id")).cast("string"))
        .withColumn("account_id", F.trim(F.col("account_id")).cast("string"))
        .withColumn("amount", F.regexp_replace(F.col("amount").cast("string"), ",", "").cast("decimal(18,2)"))
        .withColumn("currency", F.upper(F.coalesce(F.col("currency"), F.lit("USD"))))
        .withColumn("merchant_name", F.trim(F.col("merchant_name")).cast("string"))
        .withColumn(
            "merchant_category",
            F.lower(F.coalesce(F.trim(F.col("merchant_category")), F.lit("unknown"))),
        )
        .withColumn("status", F.lower(F.coalesce(F.col("status"), F.lit("unknown"))))
        .withColumn("payment_channel", F.lower(F.coalesce(F.col("payment_channel"), F.lit("unknown"))))
        .withColumn("risk_score", F.col("risk_score").cast("double"))
        .withColumn("silver_updated_at_utc", F.current_timestamp())
        .withColumn("silver_ingest_date", F.current_date())
    )


def add_quarantine_reason(df: DataFrame) -> DataFrame:
    reason = (
        F.when(F.col("transaction_id").isNull() | (F.col("transaction_id") == ""), F.lit("missing_transaction_id"))
        .when(F.col("customer_id").isNull() | (F.col("customer_id") == ""), F.lit("missing_customer_id"))
        .when(F.col("account_id").isNull() | (F.col("account_id") == ""), F.lit("missing_account_id"))
        .when(F.col("amount").isNull(), F.lit("invalid_amount"))
        .when(F.col("event_ts_utc").isNull(), F.lit("invalid_timestamp"))
    )
    return df.withColumn("quarantine_reason", reason)

