from __future__ import annotations

from uuid import uuid4

from faker import Faker

from ingestion.kafka.producers.base_producer import BaseProducer


class CustomersProducer(BaseProducer):
    topic_env_var = "customers"
    schema_file = "customer.avsc"

    def build_messages(self, record_count: int) -> list[dict[str, object]]:
        fake = Faker()
        return [
            {
                "customer_id": f"cust_{fake.random_int(min=1000, max=9999)}",
                "full_name": fake.name(),
                "segment": fake.random_element(elements=("retail", "affluent", "sme")),
                "country_code": fake.country_code(),
                "email": fake.email(),
                "record_id": str(uuid4()),
            }
            for _ in range(record_count)
        ]

