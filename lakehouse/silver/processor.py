from __future__ import annotations

from pyspark.sql import functions as F

from lakehouse.bronze.quarantine import quarantine_corrupt_records
from lakehouse.runtime import build_spark_session, ensure_databases, load_settings, merge_with_retry
from lakehouse.silver.cleaner import add_quarantine_reason, clean_transactions
from lakehouse.silver.conformer import ensure_columns
from lakehouse.silver.deduplicator import deduplicate_transactions
from lakehouse.silver.late_arrivals import mark_late_arrivals
from lakehouse.silver.timezone_normalizer import add_event_timestamp_utc, event_date_from_timestamp
from observability.metrics.pipeline_metrics import record_pipeline_metric


def process_transactions_silver(run_id: str) -> dict[str, int]:
    settings = load_settings()
    spark = build_spark_session("silver-transactions")
    ensure_databases(spark)

    bronze_df = spark.table("bronze.transactions")
    working = bronze_df.filter(F.col("run_id") == run_id)
    if working.rdd.isEmpty():
        return {"rows_written": 0, "rows_quarantined": 0}

    working = ensure_columns(working)
    working = add_event_timestamp_utc(working)
    working = event_date_from_timestamp(working)
    working = clean_transactions(working)
    working = mark_late_arrivals(working, settings.late_arrival_watermark_days)
    working = add_quarantine_reason(working)

    bad_df = working.filter(F.col("quarantine_reason").isNotNull())
    good_df = deduplicate_transactions(working.filter(F.col("quarantine_reason").isNull()))

    if not good_df.rdd.isEmpty():
        merge_with_retry(
            spark,
            good_df,
            "silver.transactions_clean",
            settings.layer_table_uri("silver", "transactions_clean"),
            merge_condition="target.transaction_id = source.transaction_id",
            partition_by=["silver_ingest_date"],
        )

    quarantined = quarantine_corrupt_records(bad_df, layer="silver", table="transactions", run_id=run_id)
    written = good_df.count() if not good_df.rdd.isEmpty() else 0
    record_pipeline_metric(
        spark,
        layer="silver",
        dataset="transactions_clean",
        run_id=run_id,
        metric_name="rows_written",
        metric_value=float(written),
    )
    return {"rows_written": written, "rows_quarantined": quarantined}

