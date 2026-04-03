from __future__ import annotations

from typing import Any

from pyspark.sql import SparkSession

from lakehouse.runtime import load_settings, write_delta


def record_pipeline_metric(
    spark: SparkSession,
    layer: str,
    dataset: str,
    run_id: str,
    metric_name: str,
    metric_value: float,
    dimensions: dict[str, str] | None = None,
) -> None:
    settings = load_settings()
    payload = [
        {
            "layer": layer,
            "dataset": dataset,
            "run_id": run_id,
            "metric_name": metric_name,
            "metric_value": metric_value,
            "dimensions": dimensions or {},
        }
    ]
    df = spark.createDataFrame(payload)
    write_delta(df, "ops.pipeline_metrics", settings.layer_table_uri("ops", "pipeline_metrics"), mode="append")

