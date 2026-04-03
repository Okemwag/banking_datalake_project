from lakehouse.silver.deduplicator import pick_latest_records


def test_pick_latest_records_keeps_newest_version():
    records = [
        {"transaction_id": "t1", "bronze_ingested_at_utc": "2026-01-01T00:00:00"},
        {"transaction_id": "t1", "bronze_ingested_at_utc": "2026-01-01T01:00:00"},
        {"transaction_id": "t2", "bronze_ingested_at_utc": "2026-01-01T00:30:00"},
    ]

    result = pick_latest_records(records)

    assert len(result) == 2
    assert any(record["transaction_id"] == "t1" and record["bronze_ingested_at_utc"] == "2026-01-01T01:00:00" for record in result)

