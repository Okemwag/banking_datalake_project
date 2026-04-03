from __future__ import annotations

import json
from typing import Any

from kafka import KafkaConsumer

from lakehouse.runtime import load_settings


class BaseConsumer:
    def __init__(self, topic: str, group_id: str) -> None:
        settings = load_settings()
        self.consumer = KafkaConsumer(
            topic,
            bootstrap_servers=settings.kafka_bootstrap_servers,
            group_id=group_id,
            auto_offset_reset="earliest",
            value_deserializer=lambda value: json.loads(value.decode("utf-8")),
        )

    def poll(self, max_records: int = 500) -> list[dict[str, Any]]:
        polled = self.consumer.poll(timeout_ms=1000, max_records=max_records)
        records: list[dict[str, Any]] = []
        for _, topic_records in polled.items():
            records.extend(record.value for record in topic_records)
        return records

