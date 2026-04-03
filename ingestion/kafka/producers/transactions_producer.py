from __future__ import annotations

import random
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from uuid import uuid4

from faker import Faker

from ingestion.kafka.producers.base_producer import BaseProducer


class TransactionsProducer(BaseProducer):
    topic_env_var = "transactions"
    schema_file = "transaction.avsc"

    def build_messages(self, record_count: int) -> list[dict[str, object]]:
        fake = Faker()
        now = datetime.now(UTC)
        records: list[dict[str, object]] = []
        for _ in range(record_count):
            event_time = now - timedelta(minutes=random.randint(0, 60 * 24 * 45))
            amount = Decimal(random.uniform(5, 2500)).quantize(Decimal("0.01"))
            records.append(
                {
                    "transaction_id": str(uuid4()),
                    "customer_id": f"cust_{fake.random_int(min=1000, max=9999)}",
                    "account_id": f"acct_{fake.random_int(min=10000, max=99999)}",
                    "amount": str(amount),
                    "currency": "USD",
                    "merchant_name": fake.company(),
                    "merchant_category": random.choice(["grocery", "travel", "retail", "utility"]),
                    "event_ts": event_time.isoformat(),
                    "event_timezone": random.choice(["UTC", "America/New_York", "Europe/London"]),
                    "status": random.choice(["approved", "declined", "chargeback"]),
                    "payment_channel": random.choice(["card", "wire", "mobile"]),
                    "risk_score": round(random.uniform(0.01, 0.99), 4),
                }
            )
        return records

