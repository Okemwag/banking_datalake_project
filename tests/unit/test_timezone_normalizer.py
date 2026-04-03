from lakehouse.silver.timezone_normalizer import normalize_timezone_name


def test_normalize_timezone_aliases():
    assert normalize_timezone_name("utc") == "UTC"
    assert normalize_timezone_name("EST") == "America/New_York"
    assert normalize_timezone_name(None) == "UTC"

