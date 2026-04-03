from __future__ import annotations

from pyspark.sql import SparkSession

from lakehouse.runtime import load_settings, write_delta


def record_dq_metric(
    spark: SparkSession,
    layer: str,
    run_id: str,
    passed: bool,
    result_count: int,
) -> None:
    settings = load_settings()
    payload = [
        {
            "layer": layer,
            "run_id": run_id,
            "passed": passed,
            "result_count": result_count,
        }
    ]
    df = spark.createDataFrame(payload)
    write_delta(df, "ops.dq_metrics", settings.layer_table_uri("ops", "dq_metrics"), mode="append")

