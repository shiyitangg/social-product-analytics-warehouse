"""Load the clean lake into PostgreSQL (raw.* schema), incrementally and idempotently.

For every lake partition (events: event_date, users: snapshot_date, content: ingest_date) we compute a
fingerprint (row count + order-independent content hash). A partition is (re)loaded only when it is new or its
fingerprint differs from what ops.loaded_partitions recorded. Each chunk is replaced inside ONE transaction
(delete old rows -> insert from a staging table -> record fingerprint), so a crash leaves the previous state.

    python src/load_postgres.py                    # everything that changed
    python src/load_postgres.py --datasets events  # one dataset
    python src/load_postgres.py --full-refresh     # ignore fingerprints
"""
import argparse
import json
import sys
import time
from datetime import date
from pathlib import Path

import pyspark.sql.functions as F

import common as C
import pgutil as pg
import state

SQL_DIR = Path(__file__).resolve().parent.parent / "sql"
MOD = 1_000_000_007

DATASETS = {
    # name: (lake prefix, partition column, target table, staging table, hash columns)
    "events": ("clean_events", "event_date", "raw.events", "raw.events_load",
               ["event_id", "event_ts", "user_id", "event_type", "content_id", "first_ingest_date"]),
    "users": ("users_snapshot", "snapshot_date", "raw.users_snapshot", "raw.users_snapshot_load",
              ["user_id", "signup_date", "country", "device", "segment", "is_creator"]),
    "content": ("content", "ingest_date", "raw.content", "raw.content_load",
                ["content_id", "creator_id", "community_id", "content_type", "created_ts"]),
}


def lake_frame(spark, name):
    prefix, part, *_ = DATASETS[name]
    return spark.read.parquet(C.s3a(C.LAKE_BUCKET, prefix)).drop("_processed_at")


def fingerprints(df, name):
    _, part, _, _, cols = DATASETS[name]
    rows = (df.groupBy(part)
            .agg(F.count(F.lit(1)).alias("n"),
                 F.sum(F.pmod(F.xxhash64(*cols), F.lit(MOD))).alias("chk"))
            .collect())
    return {r[part]: (int(r["n"]), int(r["chk"] or 0)) for r in rows}


def loaded(conn, name):
    rows = pg.fetch(conn, f"select part_key, row_count, checksum from ops.loaded_partitions where dataset='{name}'", 3)
    return {date.fromisoformat(a): (int(b), int(c)) for a, b, c in rows}


def load_chunk(spark, conn, name, df, parts, fps):
    prefix, part, target, staging, cols = DATASETS[name]
    lo, hi = min(parts), max(parts)
    plist = ",".join(f"'{p.isoformat()}'" for p in parts)
    pg.execute(conn, f"truncate table {staging}")
    sub = df.filter(F.col(part).isin(parts))
    if name == "events":
        sub = sub.select("event_id", "event_ts", "user_id", "event_type", "content_id",
                         "first_ingest_date", F.col("event_date"))
    elif name == "content":
        sub = sub.select("content_id", "creator_id", "community_id", "content_type", "created_ts", "ingest_date")
    else:
        sub = sub.select("user_id", "signup_date", "country", "device", "segment", "is_creator", "snapshot_date")
    pg.jdbc_write(sub.coalesce(4), staging)
    conn.setAutoCommit(False)
    try:
        pg.execute(conn, f"delete from {target} where {part} in ({plist})")
        pg.execute(conn, f"insert into {target} select * from {staging}")
        pg.execute(conn, f"delete from ops.loaded_partitions where dataset='{name}' and part_key in ({plist})")
        values = ",".join(f"('{name}','{p.isoformat()}',{fps[p][0]},{fps[p][1]})" for p in parts)
        pg.execute(conn, f"insert into ops.loaded_partitions(dataset, part_key, row_count, checksum) values {values}")
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.setAutoCommit(True)
    pg.execute(conn, f"truncate table {staging}")


def load_dataset(spark, conn, name, chunk_days, full_refresh):
    t0 = time.time()
    df = lake_frame(spark, name)
    lake = fingerprints(df, name)
    have = {} if full_refresh else loaded(conn, name)
    todo = sorted(p for p, fp in lake.items() if have.get(p) != fp)
    print(f"[{name}] lake partitions={len(lake)}  already loaded & unchanged={len(lake) - len(todo)}  to load={len(todo)}",
          flush=True)
    rows = 0
    for i in range(0, len(todo), chunk_days):
        chunk = todo[i:i + chunk_days]
        load_chunk(spark, conn, name, df, chunk, lake)
        rows += sum(lake[p][0] for p in chunk)
        print(f"  loaded {chunk[0]} .. {chunk[-1]}  ({sum(lake[p][0] for p in chunk):,} rows)  "
              f"{time.time() - t0:.0f}s elapsed", flush=True)
    return lake, rows


def verify(conn, name, lake):
    """Postgres row counts per partition must equal the lake's."""
    _, part, target, _, _ = DATASETS[name]
    got = {date.fromisoformat(a): int(b) for a, b in
           pg.fetch(conn, f"select {part}, count(*) from {target} group by 1", 2)}
    bad = [(p, lake[p][0], got.get(p, 0)) for p in lake if got.get(p, 0) != lake[p][0]]
    extra = [p for p in got if p not in lake]
    ok = not bad and not extra
    total = sum(got.values())
    print(f"[{name}] verify: postgres rows={total:,}  lake rows={sum(v[0] for v in lake.values()):,}  "
          f"{'OK' if ok else 'MISMATCH ' + str(bad[:5]) + ' extra=' + str(extra[:5])}", flush=True)
    return ok


def load_ingest_stats(spark, conn):
    dates = sorted(state.list_done_dates(C.LAKE_BUCKET, "events"))
    rows = []
    for d in dates:
        m = state.get_json(C.LAKE_BUCKET, state.marker_key("events", d)) or {}
        e = m.get("events", {})
        rows.append((d, e.get("rows_in"), e.get("rows_rejected"), e.get("duplicates_within_batch"),
                     e.get("late_rows"), e.get("max_lag_days"), e.get("rows_written_by_ingest"),
                     json.dumps(e.get("rejected_by_reason", {}), sort_keys=True), float(e.get("seconds") or 0)))
    if not rows:
        print("[ops] no done-markers found")
        return
    schema = ("ingest_date date, rows_in long, rows_rejected long, duplicates_within_batch long, late_rows long, "
              "max_lag_days int, rows_written_by_ingest long, rejected_by_reason string, seconds double")
    sdf = spark.createDataFrame(rows, schema)
    pg.execute(conn, "truncate table ops.ingest_stats_load")
    pg.jdbc_write(sdf, "ops.ingest_stats_load")
    conn.setAutoCommit(False)
    try:
        pg.execute(conn, "delete from ops.ingest_stats where ingest_date in (select ingest_date from ops.ingest_stats_load)")
        pg.execute(conn, "insert into ops.ingest_stats select * from ops.ingest_stats_load")
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.setAutoCommit(True)
    print(f"[ops] ingest_stats rows={len(rows)}")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--datasets", default="events,users,content,ops")
    ap.add_argument("--chunk-days", type=int, default=5)
    ap.add_argument("--full-refresh", action="store_true")
    a = ap.parse_args()
    wanted = [x.strip() for x in a.datasets.split(",") if x.strip()]

    spark = C.get_spark("load-postgres")
    conn = pg.connect(spark)
    pg.run_script(conn, (SQL_DIR / "00_warehouse_init.sql").read_text())
    ok = True
    for name in ("events", "users", "content"):
        if name in wanted:
            lake, _ = load_dataset(spark, conn, name, a.chunk_days, a.full_refresh)
            ok &= verify(conn, name, lake)
    if "ops" in wanted:
        load_ingest_stats(spark, conn)
    conn.close()
    print("LOAD", "COMPLETE" if ok else "FAILED")
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
