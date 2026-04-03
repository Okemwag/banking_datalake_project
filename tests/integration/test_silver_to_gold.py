import json
from pathlib import Path


def test_sample_fixture_contains_gold_dimensions():
    payload = json.loads(Path("tests/fixtures/sample_transactions.json").read_text())
    assert {"transaction_id", "customer_id", "account_id", "amount"} <= set(payload[0].keys())

