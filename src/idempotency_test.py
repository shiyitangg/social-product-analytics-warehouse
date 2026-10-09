"""Two correctness experiments for the incremental job (run after a full incremental ingest):

 A. Re-run: processing the same ingest date again must not change clean_events.
 B. Order independence: processing a range of ingest dates NEWEST-FIRST into a scratch lake must give
    exactly the same clean_events (for fully-delivered event dates) as processing them in order.

Usage: idempotency_test.py [--rerun-date 2026-08-12] [--from 2026-08-10 --to 2026-08-22]
"""
import argparse
import json
import os
import sys
from datetime import timedelta

from pyspark.sql import functions as F

import common as C
import state
from spark_incremental import process_events
from windows import parse_date


def fingerprint(spark, lake, d_from, d_to):
    df = (spark.read.parquet(C.s3a(lake, "clean_events"))
          .filter((F.col("event_date") >= F.lit(d_from)) & (F.col("event_date") <= F.lit(d_to))))
    rows = (df.groupBy("event_date")
            .agg(F.count("*").alias("n"),
                 F.sum(F.hash("event_id", "event_ts", "user_id", "event_type", "content_id").cast("long")).alias("h"))
            .collect())
    return {str(r["event_date"]): [r["n"], r["h"]] for r in rows}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rerun-date", default="2026-08-12")
    ap.add_argument("--from", dest="d_from", default="2026-08-10")
    ap.add_argument("--to", dest="d_to", default="2026-08-22")
    ap.add_argument("--scratch-bucket", default="lake-scratch")
    a = ap.parse_args()
    spark = C.get_spark("idempotency-test")
    main_lake = C.LAKE_BUCKET
    report = {}

    # ---- A. re-run
    rd = parse_date(a.rerun_date)
    lo, hi = rd - timedelta(days=C.MAX_LAG_DAYS), rd
    before = fingerprint(spark, main_lake, lo, hi)
    process_events(spark, rd, main_lake)
    after = fingerprint(spark, main_lake, lo, hi)
    report["rerun_unchanged"] = before == after
    print(f"A. re-run of ingest_date={rd}: clean_events for {lo}..{hi} unchanged -> "
          f"{'PASS' if before == after else 'FAIL'}  ({len(before)} event dates compared)")

    # ---- B. order independence
    s, e = parse_date(a.d_from), parse_date(a.d_to)
    dates = [s + timedelta(days=i) for i in range((e - s).days + 1)]
    C.ensure_buckets([a.scratch_bucket])
    state.empty_bucket(a.scratch_bucket)
    for d in reversed(dates):
        process_events(spark, d, a.scratch_bucket)
    cmp_to = e - timedelta(days=C.MAX_LAG_DAYS)           # later event dates are not fully delivered yet
    fp_main = fingerprint(spark, main_lake, s, cmp_to)
    fp_scratch = fingerprint(spark, a.scratch_bucket, s, cmp_to)
    report["order_independent"] = fp_main == fp_scratch
    print(f"B. ingest dates {s}..{e} processed newest-first vs in-order: clean_events for {s}..{cmp_to} identical "
          f"-> {'PASS' if fp_main == fp_scratch else 'FAIL'}  ({len(fp_main)} event dates compared)")
    os.makedirs("reports", exist_ok=True)
    json.dump(report, open("reports/idempotency_report.json", "w"), indent=2)
    sys.exit(0 if all(report.values()) else 1)


if __name__ == "__main__":
    main()
