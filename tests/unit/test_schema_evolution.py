from lakehouse.bronze.schema_evolution import detect_new_columns


def test_detect_new_columns_returns_only_new_fields():
    assert detect_new_columns(["a", "b"], ["a", "b", "c", "d"]) == ["c", "d"]

