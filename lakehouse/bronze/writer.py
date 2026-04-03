from __future__ import annotations

from pyspark.sql import functions as F

from lakehouse.bronze.quarantine import quarantine_corrupt_records
from lakehouse.bronze.schema_evolution import detect_new_columns, schema_column_names
from lakehouse.runtime import build_spark_session, ensure_databases, load_settings, table_exists, write_delta
from observability.metrics.pipeline_metrics import record_pipeline_metric


def write_transactions_bronze(run_date: str, run_id: str) -> dict[str, int]:
    settings = load_settings()
    spark = build_spark_session("bronze-transactions")
    ensure_databases(spark)

    source_path = f"{settings.raw_dataset_uri('transactions', run_date=run_date, run_id=run_id)}/*.json"
    df = spark.read.option("columnNameOfCorruptRecord", "_corrupt_record").json(source_path)
    if "_corrupt_record" not in df.columns:
        df = df.withColumn("_corrupt_record", F.lit(None).cast("string"))

    enriched = (
        df.withColumn("bronze_ingest_date", F.lit(run_date))
        .withColumn("run_id", F.lit(run_id))
        .withColumn("bronze_ingested_at_utc", F.current_timestamp())
        .withColumn("source_file", F.input_file_name())
    )
    bad_df = enriched.filter(F.col("_corrupt_record").isNotNull())
    good_df = enriched.filter(F.col("_corrupt_record").isNull())

    existing_columns = []
    if table_exists(spark, "bronze.transactions"):
        existing_columns = spark.table("bronze.transactions").columns
    new_columns = detect_new_columns(existing_columns, schema_column_names(good_df.schema))

    if not good_df.rdd.isEmpty():
        write_delta(
            good_df,
            "bronze.transactions",
            settings.layer_table_uri("bronze", "transactions"),
            mode="append",
            partition_by=["bronze_ingest_date"],
        )

    quarantined = quarantine_corrupt_records(bad_df, layer="bronze", table="transactions", run_id=run_id)
    ingested = good_df.count() if not good_df.rdd.isEmpty() else 0
    record_pipeline_metric(
        spark,
        layer="bronze",
        dataset="transactions",
        run_id=run_id,
        metric_name="rows_written",
        metric_value=float(ingested),
        dimensions={"new_columns": ",".join(new_columns)},
    )
    return {"rows_written": ingested, "rows_quarantined": quarantined}

