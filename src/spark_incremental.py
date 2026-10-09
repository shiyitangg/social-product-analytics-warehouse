"""Incremental PySpark processing: raw (object storage) -> validated lake tables.

Per ingest date D (one daily delivery) the job:
  1. validates events row by row; invalid rows go to quarantine with a reject_reason
  2. blocks publication if the batch's reject rate exceeds the gate (fail-fast)
  3. writes the de-duplicated valid rows of the batch to events_by_ingest/ingest_date=D (overwrite -> idempotent)
  4. rebuilds, for every event_date touched by the batch, lake/clean_events/event_date=X from the
     events_by_ingest partitions in the look-back/look-forward window, keeping the first delivery of
     each event_id. Late-arriving events therefore land in the correct (older) event_date partition and
     re-running any date, in any order, converges to the same result.
  5. writes a per-date done-marker with run statistics (the watermark is derived from the markers)
Users snapshots and content metadata are validated and copied the same way.

Examples (inside the runner container):
  spark_incremental.py --mode incremental                      # process every new ingest date
  spark_incremental.py --mode incremental --through 2026-08-10
  spark_incremental.py --mode dates --ingest-date 2026-08-12   # re-run / backfill one date
  spark_incremental.py --mode dates --from 2026-08-01 --to 2026-08-10
"""
import argparse
import sys
import time
import traceback
import uuid
from datetime import date, datetime, timedelta, timezone

from pyspark.sql import Window, functions as F

import common as C
import state
from rules import add_reject_reason
from windows import contiguous_watermark, lookback_window, parse_date


class GateFailure(Exception):
    """Raised when a batch must not be published (quality gate)."""


def _now():
    return datetime.now(timezone.utc).isoformat()


# --------------------------------------------------------------------------- events
def process_events(spark, d: date, lake: str, raw_bucket: str = None) -> dict:
    raw_bucket = raw_bucket or C.RAW_BUCKET
    ds = d.isoformat()
    t0 = time.time()
    raw = spark.read.parquet(C.s3a(raw_bucket, "events", f"ingest_date={ds}")).persist()
    try:
        flagged = (add_reject_reason(raw, ds, C.MAX_LAG_DAYS, C.VALID_EVENT_TYPES)
                   .withColumn("event_date", F.to_date("event_ts")))
        counts = {r["reject_reason"]: r["count"] for r in flagged.groupBy("reject_reason").count().collect()}
        valid_rows = counts.pop(None, 0)
        rejected_by_reason = counts
        rejected_rows = sum(rejected_by_reason.values())
        rows_in = valid_rows + rejected_rows
        if rows_in == 0:
            raise GateFailure(f"ingest_date={ds}: raw partition is empty")

        # (1) quarantine
        (flagged.filter(F.col("reject_reason").isNotNull())
         .select("event_id", "event_ts", "user_id", "event_type", "content_id", "reject_reason")
         .withColumn("_processed_at", F.current_timestamp())
         .write.mode("overwrite").parquet(C.s3a(lake, "quarantine_events", f"ingest_date={ds}")))

        # (2) quality gate: stop before anything is published
        reject_rate = rejected_rows / rows_in
        if reject_rate > C.MAX_REJECT_RATE:
            raise GateFailure(f"ingest_date={ds}: reject rate {reject_rate:.2%} > gate {C.MAX_REJECT_RATE:.2%} "
                              f"({rejected_by_reason})")
        if valid_rows == 0:
            raise GateFailure(f"ingest_date={ds}: no valid rows")

        # (3) within-batch dedupe -> events_by_ingest/ingest_date=D
        valid = flagged.filter(F.col("reject_reason").isNull()).drop("reject_reason").dropDuplicates(["event_id"])
        by_ingest_path = C.s3a(lake, "events_by_ingest", f"ingest_date={ds}")
        (valid.select("event_id", "event_ts", "user_id", "event_type", "content_id", "event_date")
         .withColumn("_processed_at", F.current_timestamp())
         .write.mode("overwrite").parquet(by_ingest_path))
        written = spark.read.parquet(by_ingest_path)
        silver_rows = written.count()
        within_batch_dups = valid_rows - silver_rows
        lag = F.datediff(F.to_date(F.lit(ds)), F.col("event_date"))
        agg = written.agg(F.max(lag).alias("max_lag"), F.sum((lag > 0).cast("int")).alias("late_rows")).collect()[0]
        affected = sorted(r["event_date"] for r in written.select("event_date").distinct().collect())

        # (4) rebuild the affected event_date partitions from the window of ingest partitions
        lo, hi = lookback_window(d, C.MAX_LAG_DAYS)
        win = (spark.read.parquet(C.s3a(lake, "events_by_ingest"))
               .filter((F.col("ingest_date") >= F.lit(lo)) & (F.col("ingest_date") <= F.lit(hi))
                       & F.col("event_date").isin(affected)).persist())
        rows_before = win.count()
        w = Window.partitionBy("event_id").orderBy(F.col("ingest_date").asc())
        final = (win.withColumn("_rn", F.row_number().over(w)).filter(F.col("_rn") == 1).drop("_rn")
                 .select("event_id", "event_ts", "user_id", "event_type", "content_id",
                         F.col("ingest_date").alias("first_ingest_date"), "_processed_at", "event_date")
                 .persist())
        rows_after = final.count()
        final.write.mode("overwrite").partitionBy("event_date").parquet(C.s3a(lake, "clean_events"))
        win.unpersist()
        final.unpersist()
    finally:
        raw.unpersist()
    return {
        "dataset": "events", "ingest_date": ds, "status": "success",
        "rows_in": rows_in, "rows_rejected": rejected_rows, "rejected_by_reason": rejected_by_reason,
        "reject_rate": round(reject_rate, 6),
        "duplicates_within_batch": within_batch_dups, "rows_written_by_ingest": silver_rows,
        "late_rows": int(agg["late_rows"] or 0), "max_lag_days": int(agg["max_lag"] or 0),
        "affected_event_dates": [x.isoformat() for x in affected],
        "window_rows_before_dedupe": rows_before, "window_rows_after_dedupe": rows_after,
        "duplicates_collapsed_in_window": rows_before - rows_after,
        "seconds": round(time.time() - t0, 1),
    }


# --------------------------------------------------------------------------- users / content
def process_users(spark, d: date, lake: str, raw_bucket: str = None) -> dict:
    raw_bucket = raw_bucket or C.RAW_BUCKET
    ds = d.isoformat()
    df = spark.read.parquet(C.s3a(raw_bucket, "users_snapshot", f"snapshot_date={ds}"))
    rows_in = df.count()
    ok = (F.col("user_id").isNotNull() & F.col("signup_date").isNotNull()
          & F.col("segment").isin(C.SEGMENTS) & F.col("device").isin(C.DEVICES) & F.col("country").isin(C.COUNTRIES))
    valid = df.filter(ok)
    dd = valid.dropDuplicates(["user_id"])
    (dd.withColumn("_processed_at", F.current_timestamp())
     .write.mode("overwrite").parquet(C.s3a(lake, "users_snapshot", f"snapshot_date={ds}")))
    rows_out = spark.read.parquet(C.s3a(lake, "users_snapshot", f"snapshot_date={ds}")).count()
    return {"dataset": "users_snapshot", "snapshot_date": ds, "status": "success", "rows_in": rows_in,
            "rows_rejected": rows_in - valid.count(), "rows_out": rows_out}


def process_content(spark, d: date, lake: str, raw_bucket: str = None) -> dict:
    raw_bucket = raw_bucket or C.RAW_BUCKET
    ds = d.isoformat()
    df = spark.read.parquet(C.s3a(raw_bucket, "content", f"ingest_date={ds}"))
    rows_in = df.count()
    ok = (F.col("content_id").isNotNull() & F.col("creator_id").isNotNull() & F.col("created_ts").isNotNull()
          & F.col("content_type").isin(C.CONTENT_TYPES))
    valid = df.filter(ok)
    dd = valid.dropDuplicates(["content_id"])
    path = C.s3a(lake, "content", f"ingest_date={ds}")
    dd.withColumn("_processed_at", F.current_timestamp()).write.mode("overwrite").parquet(path)
    rows_out = spark.read.parquet(path).count()
    return {"dataset": "content", "ingest_date": ds, "status": "success", "rows_in": rows_in,
            "rows_rejected": rows_in - valid.count(), "rows_out": rows_out}


# --------------------------------------------------------------------------- driver
def run_dates(spark, dates, lake, datasets, raw_dates, raw_bucket=None):
    run = {"run_id": f"{datetime.now(timezone.utc):%Y%m%dT%H%M%S}-{uuid.uuid4().hex[:6]}",
           "started_at": _now(), "lake_bucket": lake, "dates": [], "status": "success"}
    for d in dates:
        entry = {"ingest_date": d.isoformat()}
        try:
            if "events" in datasets:
                entry["events"] = process_events(spark, d, lake, raw_bucket)
            if "users" in datasets and d in raw_dates["users_snapshot"]:
                entry["users_snapshot"] = process_users(spark, d, lake, raw_bucket)
            if "content" in datasets and d in raw_dates["content"]:
                entry["content"] = process_content(spark, d, lake, raw_bucket)
            entry["status"] = "success"
            if "events" in datasets:
                state.mark_done(lake, "events", d, entry)
            ev = entry.get("events", {})
            print(f"  {d}  in={ev.get('rows_in', 0):>9,}  rejected={ev.get('rows_rejected', 0):>6,}  "
                  f"dup(batch)={ev.get('duplicates_within_batch', 0):>5,}  late={ev.get('late_rows', 0):>6,}  "
                  f"rebuilt_dates={len(ev.get('affected_event_dates', []))}  {ev.get('seconds', 0)}s", flush=True)
        except Exception as e:  # noqa: BLE001
            entry["status"] = "failed"
            entry["error"] = f"{type(e).__name__}: {e}"
            run["status"] = "failed"
            run["dates"].append(entry)
            print(f"  {d}  FAILED  {entry['error']}", file=sys.stderr, flush=True)
            if not isinstance(e, GateFailure):
                traceback.print_exc()
            break
        run["dates"].append(entry)
    run["finished_at"] = _now()
    state.write_run_log(lake, run)
    state.write_watermark(lake, "events", contiguous_watermark(state.list_done_dates(lake, "events")))
    return run


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--mode", choices=["incremental", "dates"], default="incremental")
    ap.add_argument("--through", help="incremental: last ingest date to process (inclusive)")
    ap.add_argument("--ingest-date")
    ap.add_argument("--from", dest="date_from")
    ap.add_argument("--to", dest="date_to")
    ap.add_argument("--reverse", action="store_true", help="process dates newest-first (order-independence test)")
    ap.add_argument("--datasets", default="events,users,content")
    ap.add_argument("--lake-bucket", default=C.LAKE_BUCKET)
    a = ap.parse_args()
    lake = a.lake_bucket
    datasets = set(a.datasets.split(","))

    C.ensure_buckets([C.RAW_BUCKET, lake])
    raw_dates = {
        "events": state.list_partition_dates(C.RAW_BUCKET, "events/", "ingest_date"),
        "users_snapshot": state.list_partition_dates(C.RAW_BUCKET, "users_snapshot/", "snapshot_date"),
        "content": state.list_partition_dates(C.RAW_BUCKET, "content/", "ingest_date"),
    }
    if not raw_dates["events"]:
        sys.exit("no raw events found - did you run upload_raw.py?")

    if a.mode == "incremental":
        wm = contiguous_watermark(state.list_done_dates(lake, "events"))
        x = (wm + timedelta(days=1)) if wm else min(raw_dates["events"])
        through = parse_date(a.through) if a.through else None
        todo = []
        while x in raw_dates["events"] and (through is None or x <= through):
            todo.append(x)
            x += timedelta(days=1)
        print(f"watermark={wm}; {len(todo)} new ingest date(s) to process")
    else:
        if a.ingest_date:
            todo = [parse_date(a.ingest_date)]
        elif a.date_from and a.date_to:
            s, e = parse_date(a.date_from), parse_date(a.date_to)
            todo = [s + timedelta(days=i) for i in range((e - s).days + 1)]
        else:
            sys.exit("--mode dates needs --ingest-date or --from/--to")
        missing = [d for d in todo if d not in raw_dates["events"]]
        if missing:
            sys.exit(f"raw events missing for: {[m.isoformat() for m in missing]}")
        if a.reverse:
            todo = list(reversed(todo))
    if not todo:
        print("nothing to do")
        return

    spark = C.get_spark("social-warehouse-incremental")
    run = run_dates(spark, todo, lake, datasets, raw_dates)
    print(f"run {run['run_id']}: {run['status']}  ({len(run['dates'])} date(s))")
    sys.exit(0 if run["status"] == "success" else 1)


if __name__ == "__main__":
    main()
