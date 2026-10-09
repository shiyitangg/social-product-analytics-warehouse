"""Reconcile the lake against the generator's ground truth (SYNTHETIC data only).

Every number here is checked against what the generator *knowingly injected*:
  raw rows = quarantined + clean + duplicates removed
  clean rows == valid distinct events; quarantine == injected malformed rows (by reason)
  clean rows per event_date == expected per-day counts (proves late events landed in the right day)
Writes reports/lake_reconciliation.json and exits non-zero if any check fails.
"""
import json
import os
import sys

from pyspark.sql import functions as F

import common as C

GT = os.getenv("GROUND_TRUTH_DIR", "data/raw/ground_truth")
OUT = "reports/lake_reconciliation.json"


def main():
    summary = json.load(open(f"{GT}/injection_summary.json"))
    per_day = json.load(open(f"{GT}/valid_events_by_event_date.json"))
    spark = C.get_spark("verify-lake")
    lake = C.LAKE_BUCKET

    raw_rows = spark.read.parquet(C.s3a(C.RAW_BUCKET, "events")).count()
    q = spark.read.parquet(C.s3a(lake, "quarantine_events"))
    quarantine_rows = q.count()
    reasons = {r["reject_reason"]: r["count"] for r in q.groupBy("reject_reason").count().collect()}
    clean = spark.read.parquet(C.s3a(lake, "clean_events"))
    clean_rows = clean.count()
    clean_distinct = clean.select("event_id").distinct().count()
    per_day_actual = {str(r["event_date"]): r["count"] for r in clean.groupBy("event_date").count().collect()}
    content_rows = spark.read.parquet(C.s3a(lake, "content")).count()
    dup_removed = raw_rows - quarantine_rows - clean_rows

    checks = [
        ("raw rows read == rows generated", raw_rows, summary["raw_rows"]),
        ("quarantined rows == injected malformed rows", quarantine_rows, summary["bad_rows"]),
        ("  null_user_id", reasons.get("null_user_id", 0), summary["bad_null_user"]),
        ("  invalid_event_type", reasons.get("invalid_event_type", 0), summary["bad_unknown_type"]),
        ("  future_timestamp", reasons.get("future_timestamp", 0), summary["bad_future_ts"]),
        ("clean rows == valid distinct events", clean_rows, summary["valid_distinct_events"]),
        ("clean event_ids are unique", clean_distinct, clean_rows),
        ("duplicates removed == injected duplicate deliveries", dup_removed, summary["duplicate_rows"]),
        ("clean rows per event_date == expected for ALL days", per_day_actual == per_day, True),
        ("content rows == generated", content_rows, summary["content_rows"]),
    ]
    ok_all = True
    print(f"{'check':<58}{'actual':>14}{'expected':>14}  result")
    results = []
    for name, actual, expected in checks:
        ok = actual == expected
        ok_all &= ok
        print(f"{name:<58}{str(actual):>14}{str(expected):>14}  {'PASS' if ok else 'FAIL'}")
        results.append({"check": name.strip(), "actual": actual, "expected": expected, "pass": bool(ok)})
    if per_day_actual != per_day:
        bad = {k: (per_day_actual.get(k), per_day.get(k)) for k in set(per_day) | set(per_day_actual)
               if per_day_actual.get(k) != per_day.get(k)}
        print("per-day mismatches (actual, expected):", dict(list(bad.items())[:10]))
    os.makedirs("reports", exist_ok=True)
    json.dump({"all_pass": bool(ok_all), "results": results, "note": "SYNTHETIC data with injected ground truth"},
              open(OUT, "w"), indent=2)
    print("\nALL CHECKS PASSED" if ok_all else "\nSOME CHECKS FAILED")
    sys.exit(0 if ok_all else 1)


if __name__ == "__main__":
    main()
