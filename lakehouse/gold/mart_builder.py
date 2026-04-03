from __future__ import annotations

from pyspark.sql import functions as F

from lakehouse.gold.aggregator import aggregate_account_risk_metrics, aggregate_daily_customer_metrics
from lakehouse.gold.incremental_merge import merge_gold_table
from lakehouse.runtime import build_spark_session, ensure_databases, load_settings
from observability.metrics.pipeline_metrics import record_pipeline_metric


def build_gold_marts(run_id: str) -> dict[str, int]:
    settings = load_settings()
    spark = build_spark_session("gold-marts")
    ensure_databases(spark)

    silver_df = spark.table("silver.transactions_clean")
    impacted = silver_df.filter(F.col("run_id") == run_id)
    if impacted.rdd.isEmpty():
        return {"customer_rows": 0, "account_rows": 0}

    affected_dates = [row.event_date for row in impacted.select("event_date").distinct().collect()]
    curated_source = silver_df.filter(F.col("event_date").isin(affected_dates))

    customer_metrics = aggregate_daily_customer_metrics(curated_source)
    account_metrics = aggregate_account_risk_metrics(curated_source)

    merge_gold_table(
        spark,
        customer_metrics,
        "gold.daily_customer_metrics",
        settings.layer_table_uri("gold", "daily_customer_metrics"),
        "target.event_date = source.event_date AND target.customer_id = source.customer_id",
    )
    merge_gold_table(
        spark,
        account_metrics,
        "gold.account_risk_metrics",
        settings.layer_table_uri("gold", "account_risk_metrics"),
        "target.event_date = source.event_date AND target.account_id = source.account_id",
    )

    record_pipeline_metric(
        spark,
        layer="gold",
        dataset="daily_customer_metrics",
        run_id=run_id,
        metric_name="rows_written",
        metric_value=float(customer_metrics.count()),
    )
    return {
        "customer_rows": customer_metrics.count(),
        "account_rows": account_metrics.count(),
    }

