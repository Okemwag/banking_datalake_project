from __future__ import annotations

import json
from abc import ABC, abstractmethod
from typing import Any

from kafka import KafkaProducer

from ingestion.utils.schema_registry import LocalSchemaRegistry
from lakehouse.runtime import load_settings


class BaseProducer(ABC):
    topic_env_var: str
    schema_file: str

    def __init__(self) -> None:
        settings = load_settings()
        self.topic = settings.kafka_topics[self.topic_env_var]
        self.registry = LocalSchemaRegistry()
        self.schema = self.registry.load(self.schema_file)
        self.producer = KafkaProducer(
            bootstrap_servers=settings.kafka_bootstrap_servers,
            value_serializer=lambda value: json.dumps(value).encode("utf-8"),
        )

    @abstractmethod
    def build_messages(self, record_count: int) -> list[dict[str, Any]]:
        raise NotImplementedError

    def publish(self, record_count: int) -> int:
        sent = 0
        for payload in self.build_messages(record_count):
            self.producer.send(self.topic, payload)
            sent += 1
        self.producer.flush()
        return sent

