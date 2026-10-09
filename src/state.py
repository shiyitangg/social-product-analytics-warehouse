"""Small S3-backed state store: done-markers (per ingest date), watermark, run logs."""
import json
from datetime import date, datetime, timezone

import common as C


def put_json(bucket: str, key: str, obj) -> None:
    C.s3_client().put_object(Bucket=bucket, Key=key, Body=json.dumps(obj, indent=2, default=str).encode())


def get_json(bucket: str, key: str):
    s3 = C.s3_client()
    try:
        return json.loads(s3.get_object(Bucket=bucket, Key=key)["Body"].read())
    except s3.exceptions.NoSuchKey:
        return None


def list_partition_dates(bucket: str, prefix: str, key: str):
    """Dates of Hive-style sub-folders, e.g. events/ingest_date=2026-08-01/ -> {date}."""
    out = set()
    for page in C.s3_client().get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix, Delimiter="/"):
        for cp in page.get("CommonPrefixes", []):
            name = cp["Prefix"].rstrip("/").split("/")[-1]
            if name.startswith(key + "="):
                out.add(date.fromisoformat(name.split("=", 1)[1]))
    return out


def marker_key(dataset: str, d: date) -> str:
    return f"_state/{dataset}/ingest_date={d.isoformat()}.json"


def mark_done(bucket: str, dataset: str, d: date, stats: dict) -> None:
    put_json(bucket, marker_key(dataset, d), stats)


def list_done_dates(bucket: str, dataset: str):
    out = set()
    prefix = f"_state/{dataset}/"
    for page in C.s3_client().get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix):
        for o in page.get("Contents", []):
            name = o["Key"].split("/")[-1]
            if name.startswith("ingest_date=") and name.endswith(".json"):
                out.add(date.fromisoformat(name[len("ingest_date="):-5]))
    return out


def write_watermark(bucket: str, dataset: str, d) -> None:
    put_json(bucket, f"_state/{dataset}_watermark.json",
             {"dataset": dataset, "last_contiguous_ingest_date": d.isoformat() if d else None,
              "updated_at": datetime.now(timezone.utc).isoformat()})


def write_run_log(bucket: str, run: dict) -> None:
    put_json(bucket, f"_ops/runs/{run['run_id']}.json", run)


def empty_bucket(bucket: str) -> None:
    s3 = C.s3_client()
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket):
        objs = [{"Key": o["Key"]} for o in page.get("Contents", [])]
        if objs:
            s3.delete_objects(Bucket=bucket, Delete={"Objects": objs})
