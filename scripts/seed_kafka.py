from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import boto3

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from ingestion.kafka.producers.customers_producer import CustomersProducer
from ingestion.kafka.producers.events_producer import EventsProducer
from ingestion.kafka.producers.transactions_producer import TransactionsProducer
from lakehouse.runtime import load_settings


PRODUCERS = {
    "transactions": TransactionsProducer,
    "customers": CustomersProducer,
    "events": EventsProducer,
}


def main() -> None:
    parser = argparse.ArgumentParser(description="Seed Kafka topics and land the payloads into raw storage.")
    parser.add_argument("--topic", default="transactions", choices=PRODUCERS.keys())
    parser.add_argument("--record-count", type=int, default=250)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--run-date", required=True)
    args = parser.parse_args()

    settings = load_settings()
    producer = PRODUCERS[args.topic]()
    messages = producer.build_messages(args.record_count)
    for payload in messages:
        producer.producer.send(producer.topic, payload)
    producer.producer.flush()

    key = f"{settings.raw_prefix}/{args.topic}/ingest_date={args.run_date}/run_id={args.run_id}/events.json"
    client = boto3.client(
        "s3",
        endpoint_url=settings.s3_endpoint_http,
        aws_access_key_id=settings.aws_access_key_id,
        aws_secret_access_key=settings.aws_secret_access_key,
    )
    client.put_object(
        Bucket=settings.bucket,
        Key=key,
        Body="\n".join(json.dumps(message) for message in messages).encode("utf-8"),
    )
    print({"topic": args.topic, "records_sent": len(messages), "raw_object_key": key})


if __name__ == "__main__":
    main()
