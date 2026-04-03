from datetime import date

from lakehouse.silver.late_arrivals import is_late_arrival


def test_late_arrival_when_older_than_watermark():
    assert is_late_arrival(date(2026, 1, 1), date(2026, 3, 1), 30) is True
    assert is_late_arrival(date(2026, 2, 15), date(2026, 3, 1), 30) is False

