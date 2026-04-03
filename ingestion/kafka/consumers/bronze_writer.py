from __future__ import annotations

import json
from datetime import UTC, datetime

import boto3

from ingestion.kafka.consumers.base_consumer import BaseConsumer
from lakehouse.runtime import load_settings


def consume_topic_to_raw(
    topic: str,
    group_id: str = "bronze-writer",
    max_records: int = 500,
    run_id: str | None = None,
    run_date: str | None = None,
) -> str | None:
    settings = load_settings()
    consumer = BaseConsumer(topic=topic, group_id=group_id)
    records = consumer.poll(max_records=max_records)
    if not records:
        return None

    now = datetime.now(UTC)
    effective_run_id = run_id or now.strftime("%Y%m%d%H%M%S")
    effective_run_date = run_date or f"{now:%Y-%m-%d}"
    key = f"{settings.raw_prefix}/{topic}/ingest_date={effective_run_date}/run_id={effective_run_id}/events.json"
    body = "\n".join(json.dumps(record) for record in records)

    client = boto3.client(
        "s3",
        endpoint_url=settings.s3_endpoint_http,
        aws_access_key_id=settings.aws_access_key_id,
        aws_secret_access_key=settings.aws_secret_access_key,
    )
    client.put_object(Bucket=settings.bucket, Key=key, Body=body.encode("utf-8"))
    return key
