"""Spark rule tests. Skipped automatically where pyspark is not installed (they run in the runner image)."""
from datetime import datetime, timezone

import pytest

pytest.importorskip("pyspark")
import common as C  # noqa: E402
from rules import add_reject_reason  # noqa: E402


@pytest.fixture(scope="module")
def spark():
    s = C.get_spark("rules-test", s3=False)
    yield s
    s.stop()


def ts(day, hour=12):
    return datetime(2026, 8, day, hour, 0, 0, tzinfo=timezone.utc)


def reasons(spark, rows, ingest="2026-08-10"):
    schema = "event_id long, event_ts timestamp, user_id long, event_type string, content_id long"
    df = spark.createDataFrame(rows, schema)
    out = add_reject_reason(df, ingest, C.MAX_LAG_DAYS, C.VALID_EVENT_TYPES).collect()
    return {r["event_id"]: r["reject_reason"] for r in out}


def test_valid_rows_have_no_reason(spark):
    r = reasons(spark, [(1, ts(10), 5, "content_view", 9), (2, ts(10), 5, "app_open", None)])
    assert r == {1: None, 2: None}


def test_each_rule_fires(spark):
    r = reasons(spark, [
        (1, ts(10), None, "like", 9),               # null user
        (2, ts(10), 5, "bogus_event", 9),           # invalid type
        (3, ts(12), 5, "like", 9),                  # delivered before it happened
        (4, ts(3), 5, "like", 9),                   # 7 days late > MAX_LAG_DAYS
        (5, ts(10), 5, "comment", None),            # needs content_id
        (6, None, 5, "like", 9),                    # null timestamp
        (None, ts(10), 5, "like", 9),               # null event id
    ])
    assert r[1] == "null_user_id" and r[2] == "invalid_event_type" and r[3] == "future_timestamp"
    assert r[4] == "too_late" and r[5] == "missing_content_id" and r[6] == "null_event_ts"
    assert r[None] == "null_event_id"


def test_late_boundary_is_inclusive(spark):
    r = reasons(spark, [(1, ts(5), 5, "like", 9), (2, ts(4), 5, "like", 9)])   # lag 5 ok, lag 6 too late
    assert r[1] is None and r[2] == "too_late"
