from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from lakehouse.runtime import build_spark_session
from observability.metrics.dq_metrics import record_dq_metric


SUITE_PATH = Path(__file__).resolve().parent / "expectations"
DEFAULT_TABLES = {
    "bronze": "bronze.transactions",
    "silver": "silver.transactions_clean",
    "gold": "gold.daily_customer_metrics",
}


def _load_suite(layer: str) -> dict[str, Any]:
    return json.loads((SUITE_PATH / f"{layer}_suite.json").read_text())


def _check_expectation(df: DataFrame, expectation: dict[str, Any]) -> tuple[bool, str]:
    expectation_type = expectation["expectation_type"]
    column = expectation["kwargs"].get("column")

    if expectation_type == "not_null":
        success = df.filter(F.col(column).isNull()).count() == 0
    elif expectation_type == "unique":
        success = df.count() == df.select(column).distinct().count()
    elif expectation_type == "accepted_values":
        allowed = expectation["kwargs"]["values"]
        success = df.filter(~F.col(column).isin(allowed)).count() == 0
    elif expectation_type == "non_negative":
        success = df.filter(F.col(column) < 0).count() == 0
    else:
        success = True
    return success, expectation_type


def run_layer_validation(layer: str, table_name: str | None = None, run_id: str | None = None) -> dict[str, Any]:
    spark = build_spark_session(f"dq-{layer}")
    df = spark.table(table_name or DEFAULT_TABLES[layer])
    suite = _load_suite(layer)
    results = []
    passed = True
    for expectation in suite["expectations"]:
        success, name = _check_expectation(df, expectation)
        results.append({"expectation": name, "success": success})
        passed = passed and success
    record_dq_metric(spark, layer=layer, run_id=run_id or "manual", passed=passed, result_count=len(results))
    return {"layer": layer, "passed": passed, "results": results}

