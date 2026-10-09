from datetime import date

from windows import contiguous_watermark, lookback_window, missing_dates


def d(n):
    return date(2026, 8, n)


def test_lookback_window_is_symmetric():
    assert lookback_window(d(10), 5) == (d(5), d(15))


def test_watermark_stops_at_first_gap():
    assert contiguous_watermark([d(1), d(2), d(3), d(5), d(6)]) == d(3)


def test_watermark_empty_and_single():
    assert contiguous_watermark([]) is None
    assert contiguous_watermark([d(7)]) == d(7)


def test_watermark_ignores_order_and_duplicates():
    assert contiguous_watermark([d(3), d(1), d(2), d(2)]) == d(3)


def test_missing_dates():
    assert missing_dates({d(1), d(3)}, d(1), d(4)) == [d(2), d(4)]
