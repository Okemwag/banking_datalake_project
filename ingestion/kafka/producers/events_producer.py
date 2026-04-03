from __future__ import annotations

from datetime import UTC, datetime
from uuid import uuid4

from ingestion.kafka.producers.base_producer import BaseProducer


class EventsProducer(BaseProducer):
    topic_env_var = "events"
    schema_file = "event.avsc"

    def build_messages(self, record_count: int) -> list[dict[str, object]]:
        now = datetime.now(UTC).isoformat()
        return [
            {
                "event_id": str(uuid4()),
                "event_name": "device_login",
                "customer_id": f"cust_{1000 + index}",
                "device_id": f"device_{index}",
                "event_ts": now,
            }
            for index in range(record_count)
        ]

