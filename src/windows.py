"""Pure date-window helpers (no Spark / S3) so they can be unit-tested anywhere."""
from datetime import date, timedelta
from typing import Iterable, Optional, Set


def parse_date(s: str) -> date:
    return date.fromisoformat(s)


def lookback_window(d: date, max_lag_days: int):
    """Ingest-date window that can contribute rows to the event dates touched by ingest `d`.

    Rows for event_date X arrive in ingest dates X .. X+max_lag. Ingest `d` touches event
    dates in [d-max_lag, d], so every ingest that could hold rows for those event dates lies in
    [d-max_lag, d+max_lag]. Reading that window (instead of "everything before d") makes the
    rebuild independent of the order in which ingest dates are processed.
    """
    return d - timedelta(days=max_lag_days), d + timedelta(days=max_lag_days)


def contiguous_watermark(done: Iterable[date]) -> Optional[date]:
    """Latest date D such that every ingest date from the earliest processed one up to D is done."""
    s = sorted(set(done))
    if not s:
        return None
    last = s[0]
    for x in s[1:]:
        if x == last + timedelta(days=1):
            last = x
        else:
            break
    return last


def missing_dates(have: Set[date], start: date, end: date):
    out, x = [], start
    while x <= end:
        if x not in have:
            out.append(x)
        x += timedelta(days=1)
    return out
