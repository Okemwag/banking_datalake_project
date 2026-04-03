from __future__ import annotations

import argparse
import sys
from datetime import date, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from scripts.seed_kafka import PRODUCERS

from lakehouse.bronze.writer import write_transactions_bronze
from lakehouse.gold.mart_builder import build_gold_marts
from lakehouse.silver.processor import process_transactions_silver
from lakehouse.runtime import load_settings


def _daterange(start: date, end: date):
    current = start
    while current <= end:
        yield current
        current += timedelta(days=1)


def main() -> None:
    parser = argparse.ArgumentParser(description="Backfill a date range through Bronze, Silver, and Gold.")
    parser.add_argument("--start-date", required=True)
    parser.add_argument("--end-date", required=True)
    args = parser.parse_args()

    start = date.fromisoformat(args.start_date)
    end = date.fromisoformat(args.end_date)
    settings = load_settings()

    for batch_date in _daterange(start, end):
        run_date = batch_date.isoformat()
        run_id = batch_date.strftime("%Y%m%d") + "000000"
        producer = PRODUCERS["transactions"]()
        messages = producer.build_messages(250)
        for payload in messages:
            producer.producer.send(producer.topic, payload)
        producer.producer.flush()

        import boto3
        import json

        client = boto3.client(
            "s3",
            endpoint_url=settings.s3_endpoint_http,
            aws_access_key_id=settings.aws_access_key_id,
            aws_secret_access_key=settings.aws_secret_access_key,
        )
        key = f"{settings.raw_prefix}/transactions/ingest_date={run_date}/run_id={run_id}/events.json"
        client.put_object(
            Bucket=settings.bucket,
            Key=key,
            Body="\n".join(json.dumps(message) for message in messages).encode("utf-8"),
        )
        write_transactions_bronze(run_date=run_date, run_id=run_id)
        process_transactions_silver(run_id=run_id)
        build_gold_marts(run_id=run_id)


if __name__ == "__main__":
    main()
