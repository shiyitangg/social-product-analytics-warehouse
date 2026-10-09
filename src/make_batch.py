"""Create an extra raw batch for the NEXT day, 2026-09-10 (the generator stops at 09-09), to demonstrate the pipeline's behaviour on a NEW day.

    python src/make_batch.py --kind bad     # 6% of rows have a NULL user_id  -> the quality gate must stop the run
    python src/make_batch.py --kind good    # the same batch without corruption -> the run succeeds and publishes

Both build the batch from the real 2026-09-09 raw data, shifted forward 1 day (new event ids, new timestamps), added as a second file in raw ingest partition 2026-09-10.
SYNTHETIC test input only; it writes to the raw bucket, never to the lake.
"""
import argparse
import io

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

import common as C

SRC_KEY_PREFIX = "events/ingest_date=2026-09-09/"
DST_DATE = "2026-09-10"
SHIFT_DAYS = 1
ID_OFFSET = 50_000_000


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--kind", choices=["good", "bad"], required=True)
    ap.add_argument("--bad-rate", type=float, default=0.06)
    a = ap.parse_args()

    s3 = C.s3_client()
    keys = [o["Key"] for o in s3.list_objects_v2(Bucket=C.RAW_BUCKET, Prefix=SRC_KEY_PREFIX).get("Contents", [])]
    if not keys:
        raise SystemExit("source raw partition not found - run `make upload` first")
    tables = []
    for k in keys:
        body = s3.get_object(Bucket=C.RAW_BUCKET, Key=k)["Body"].read()
        tables.append(pq.read_table(io.BytesIO(body)))
    df = pa.concat_tables(tables).to_pandas()

    # keep only rows whose event happened on the source ingest day itself (lag 0) so the new batch is a clean "today"
    df = df[df["event_ts"].dt.strftime("%Y-%m-%d") == "2026-09-09"].copy()
    df["event_id"] = df["event_id"] + ID_OFFSET
    df["event_ts"] = df["event_ts"] + np.timedelta64(SHIFT_DAYS, "D")
    if a.kind == "bad":
        rng = np.random.default_rng(1)
        mask = rng.random(len(df)) < a.bad_rate
        df.loc[mask, "user_id"] = None
        print(f"corrupting {int(mask.sum()):,} of {len(df):,} rows (user_id -> NULL)")

    out = pa.Table.from_pandas(df, preserve_index=False)
    key = f"events/ingest_date={DST_DATE}/events-from-{DST_DATE}.parquet"
    buf = io.BytesIO()
    pq.write_table(out, buf, compression="snappy", coerce_timestamps="us", allow_truncated_timestamps=True)
    s3.put_object(Bucket=C.RAW_BUCKET, Key=key, Body=buf.getvalue())
    print(f"wrote {len(df):,} rows to s3://{C.RAW_BUCKET}/{key}  ({a.kind} batch)")


if __name__ == "__main__":
    main()
