from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from lakehouse.runtime import build_spark_session, load_settings, write_delta
from observability.metrics.pipeline_metrics import record_pipeline_metric


def quarantine_corrupt_records(df: DataFrame, layer: str, table: str, run_id: str) -> int:
    if df is None or df.rdd.isEmpty():
        return 0

    settings = load_settings()
    quarantined = (
        df.withColumn("quarantine_reason", F.coalesce(F.col("_corrupt_record"), F.lit("invalid_payload")))
        .withColumn("quarantined_at_utc", F.current_timestamp())
        .withColumn("run_id", F.lit(run_id))
    )
    write_delta(
        quarantined,
        f"ops.{table}_quarantine",
        settings.bad_table_uri(layer, table),
        mode="append",
        partition_by=["run_id"],
    )
    record_pipeline_metric(
        build_spark_session("quarantine-metrics"),
        layer=layer,
        dataset=table,
        run_id=run_id,
        metric_name="quarantined_rows",
        metric_value=float(quarantined.count()),
    )
    return quarantined.count()

