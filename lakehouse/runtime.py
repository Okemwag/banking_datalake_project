from __future__ import annotations

import os
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml
from delta import configure_spark_with_delta_pip
from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession


REPO_ROOT = Path(__file__).resolve().parents[1]


def _deep_merge(base: dict[str, Any], override: dict[str, Any]) -> dict[str, Any]:
    merged = dict(base)
    for key, value in override.items():
        if isinstance(value, dict) and isinstance(merged.get(key), dict):
            merged[key] = _deep_merge(merged[key], value)
        else:
            merged[key] = value
    return merged


@dataclass(slots=True)
class LakehouseSettings:
    project_name: str
    environment: str
    bucket: str
    raw_prefix: str
    bronze_prefix: str
    silver_prefix: str
    gold_prefix: str
    bad_prefix: str
    warehouse_prefix: str
    aws_access_key_id: str
    aws_secret_access_key: str
    s3_endpoint_http: str
    s3_path_style: bool
    spark_master: str
    spark_timezone: str
    hive_metastore_uri: str
    kafka_bootstrap_servers: str
    late_arrival_watermark_days: int
    gold_lookback_days: int
    trino_catalog: str
    trino_schema: str
    trino_host: str
    trino_port: int
    kafka_topics: dict[str, str]

    def s3a_uri(self, path: str) -> str:
        clean_path = path.strip("/")
        return f"s3a://{self.bucket}/{clean_path}" if clean_path else f"s3a://{self.bucket}"

    def raw_dataset_uri(self, dataset: str, run_date: str | None = None, run_id: str | None = None) -> str:
        path = f"{self.raw_prefix}/{dataset}"
        if run_date:
            path = f"{path}/ingest_date={run_date}"
        if run_id:
            path = f"{path}/run_id={run_id}"
        return self.s3a_uri(path)

    def layer_table_uri(self, layer: str, table: str) -> str:
        prefix_map = {
            "bronze": self.bronze_prefix,
            "silver": self.silver_prefix,
            "gold": self.gold_prefix,
            "ops": f"{self.warehouse_prefix}/ops",
        }
        return self.s3a_uri(f"{prefix_map[layer]}/{table}")

    def bad_table_uri(self, layer: str, table: str) -> str:
        return self.s3a_uri(f"{self.bad_prefix}/{layer}/{table}")


def load_settings() -> LakehouseSettings:
    base = yaml.safe_load((REPO_ROOT / "config/base.yaml").read_text())
    env_name = os.getenv("ENVIRONMENT", base["environment"])
    env_file = REPO_ROOT / f"config/{env_name}.yaml"
    env_config = yaml.safe_load(env_file.read_text()) if env_file.exists() else {}
    config = _deep_merge(base, env_config)

    return LakehouseSettings(
        project_name=config["project_name"],
        environment=env_name,
        bucket=os.getenv("S3_BUCKET", config["storage"]["bucket"]),
        raw_prefix=config["storage"]["raw_prefix"],
        bronze_prefix=config["storage"]["bronze_prefix"],
        silver_prefix=config["storage"]["silver_prefix"],
        gold_prefix=config["storage"]["gold_prefix"],
        bad_prefix=config["storage"]["bad_prefix"],
        warehouse_prefix=config["storage"]["warehouse_prefix"],
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID", "minio"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY", "minio123"),
        s3_endpoint_http=os.getenv("S3_ENDPOINT", config["storage"].get("endpoint", "http://minio:9000")),
        s3_path_style=str(os.getenv("S3_PATH_STYLE", config["storage"].get("path_style", True))).lower()
        == "true",
        spark_master=os.getenv("SPARK_MASTER", config["spark"]["master"]),
        spark_timezone=os.getenv("SPARK_TIMEZONE", config["spark"]["timezone"]),
        hive_metastore_uri=os.getenv("HIVE_METASTORE_URI", "thrift://hive-metastore:9083"),
        kafka_bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"),
        late_arrival_watermark_days=int(
            os.getenv(
                "LATE_ARRIVAL_WATERMARK_DAYS",
                config["quality"]["late_arrival_watermark_days"],
            )
        ),
        gold_lookback_days=int(
            os.getenv("GOLD_LOOKBACK_DAYS", config["optimization"]["gold_lookback_days"])
        ),
        trino_catalog=os.getenv("TRINO_CATALOG", "delta_lake"),
        trino_schema=os.getenv("TRINO_SCHEMA", "gold"),
        trino_host=os.getenv("TRINO_HOST", "trino"),
        trino_port=int(os.getenv("TRINO_PORT", "8081")),
        kafka_topics={
            "transactions": os.getenv("KAFKA_TOPIC_TRANSACTIONS", "transactions"),
            "customers": os.getenv("KAFKA_TOPIC_CUSTOMERS", "customers"),
            "events": os.getenv("KAFKA_TOPIC_EVENTS", "events"),
        },
    )


def build_spark_session(app_name: str) -> SparkSession:
    settings = load_settings()
    builder = (
        SparkSession.builder.appName(app_name)
        .master(settings.spark_master)
        .config("spark.sql.session.timeZone", settings.spark_timezone)
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.databricks.delta.schema.autoMerge.enabled", "true")
        .config("spark.databricks.delta.optimizeWrite.enabled", "true")
        .config("spark.databricks.delta.autoCompact.enabled", "true")
        .config("spark.hadoop.fs.s3a.endpoint", settings.s3_endpoint_http)
        .config("spark.hadoop.fs.s3a.access.key", settings.aws_access_key_id)
        .config("spark.hadoop.fs.s3a.secret.key", settings.aws_secret_access_key)
        .config("spark.hadoop.fs.s3a.path.style.access", str(settings.s3_path_style).lower())
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("hive.metastore.uris", settings.hive_metastore_uri)
        .enableHiveSupport()
    )
    return configure_spark_with_delta_pip(builder).getOrCreate()


def ensure_databases(spark: SparkSession) -> None:
    settings = load_settings()
    for database in ("bronze", "silver", "gold", "ops"):
        spark.sql(
            f"""
            CREATE DATABASE IF NOT EXISTS {database}
            LOCATION '{settings.s3a_uri(f"{settings.warehouse_prefix}/{database}.db")}'
            """
        )


def table_exists(spark: SparkSession, table_name: str) -> bool:
    return spark.catalog.tableExists(table_name)


def write_delta(
    df: DataFrame,
    table_name: str,
    path: str,
    mode: str = "append",
    partition_by: list[str] | None = None,
) -> None:
    writer = df.write.format("delta").mode(mode).option("path", path).option("mergeSchema", "true")
    if partition_by:
        writer = writer.partitionBy(*partition_by)
    writer.saveAsTable(table_name)


def merge_with_retry(
    spark: SparkSession,
    source_df: DataFrame,
    target_table: str,
    target_path: str,
    merge_condition: str,
    partition_by: list[str] | None = None,
    retries: int = 3,
    backoff_seconds: int = 2,
) -> None:
    if not table_exists(spark, target_table):
        write_delta(source_df, target_table, target_path, mode="overwrite", partition_by=partition_by)
        return

    last_error: Exception | None = None
    for attempt in range(retries):
        try:
            target = DeltaTable.forName(spark, target_table)
            (
                target.alias("target")
                .merge(source_df.alias("source"), merge_condition)
                .whenMatchedUpdateAll()
                .whenNotMatchedInsertAll()
                .execute()
            )
            return
        except Exception as exc:  # pragma: no cover - Spark conflict behavior is runtime-specific.
            last_error = exc
            time.sleep(backoff_seconds * (attempt + 1))
    if last_error:
        raise last_error

