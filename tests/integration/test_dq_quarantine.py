import json
from pathlib import Path


def test_corrupt_fixture_contains_unparseable_record():
    corrupt = json.loads(Path("tests/fixtures/corrupt_records.json").read_text())
    assert corrupt["records"][0]["_corrupt_record"] is not None

