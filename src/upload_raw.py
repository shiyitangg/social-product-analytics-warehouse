"""Upload generated raw data (data/raw) into the object-storage `raw` bucket.
ground_truth/ is deliberately NOT uploaded: the pipeline must never see the answers.
Idempotent: files already present with the same size are skipped.
"""
import os
import sys

import common as C

SRC = sys.argv[1] if len(sys.argv) > 1 else "data/raw"


def main():
    C.ensure_buckets([C.RAW_BUCKET, C.LAKE_BUCKET])
    s3 = C.s3_client()
    existing = {}
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=C.RAW_BUCKET):
        for o in page.get("Contents", []):
            existing[o["Key"]] = o["Size"]
    up = skipped = 0
    for root, dirs, files in os.walk(SRC):
        dirs[:] = [d for d in dirs if d != "ground_truth"]
        for f in files:
            if not f.endswith(".parquet"):
                continue
            path = os.path.join(root, f)
            key = os.path.relpath(path, SRC).replace(os.sep, "/")
            if existing.get(key) == os.path.getsize(path):
                skipped += 1
                continue
            s3.upload_file(path, C.RAW_BUCKET, key)
            up += 1
            if up % 50 == 0:
                print(f"  uploaded {up} files ...")
    print(f"done: uploaded {up}, skipped {skipped} (already present) -> s3://{C.RAW_BUCKET}/")


if __name__ == "__main__":
    main()
