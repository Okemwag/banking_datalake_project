from __future__ import annotations

import json
from datetime import UTC, datetime

import boto3

from ingestion.kafka.consumers.base_consumer import BaseConsumer
from lakehouse.runtime import load_settings


def consume_topic_to_raw(topic: str, group_id: str = "bronze-writer", max_records: int = 500) -> str | None:
    settings = load_settings()
    consumer = BaseConsumer(topic=topic, group_id=group_id)
    records = consumer.poll(max_records=max_records)
    if not records:
        return None

    now = datetime.now(UTC)
    run_id = now.strftime("%Y%m%d%H%M%S")
    key = f"{settings.raw_prefix}/{topic}/ingest_date={now:%Y-%m-%d}/run_id={run_id}/events.json"
    body = "\n".join(json.dumps(record) for record in records)

    client = boto3.client(
        "s3",
        endpoint_url=settings.s3_endpoint_http,
        aws_access_key_id=settings.aws_access_key_id,
        aws_secret_access_key=settings.aws_secret_access_key,
    )
    client.put_object(Bucket=settings.bucket, Key=key, Body=body.encode("utf-8"))
    return key

