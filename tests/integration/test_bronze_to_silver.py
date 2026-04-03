import json
from pathlib import Path


def test_schema_drift_fixture_contains_new_column():
    payload = json.loads(Path("tests/fixtures/schema_drift_payload.json").read_text())
    assert "merchant_region" in payload

