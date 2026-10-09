"""Shared configuration and helpers (S3/MinIO client, Spark session)."""
import os
import time

S3_ENDPOINT = os.getenv("S3_ENDPOINT", "http://localhost:9000")
S3_KEY = os.getenv("S3_ACCESS_KEY", "minioadmin")
S3_SECRET = os.getenv("S3_SECRET_KEY", "minioadmin")
RAW_BUCKET = os.getenv("RAW_BUCKET", "raw")
LAKE_BUCKET = os.getenv("LAKE_BUCKET", "lake")

# Data contract -------------------------------------------------------------
VALID_EVENT_TYPES = ["app_open", "content_view", "like", "comment", "share", "follow", "content_create"]
COUNTRIES = ["US", "IN", "BR", "GB", "DE", "ID", "MX", "OTHER"]
DEVICES = ["ios", "android", "web"]
SEGMENTS = ["new", "casual", "power", "dormant"]
CONTENT_TYPES = ["video", "post", "story"]

# Events may be delivered up to MAX_LAG_DAYS after they happened. Anything later is
# quarantined ("too_late") instead of silently dropped. The incremental job relies on
# this bound: event_date X can only receive rows from ingest dates X..X+MAX_LAG_DAYS.
MAX_LAG_DAYS = 5
# A batch with more than this share of rejected rows blocks publication (fail-fast gate).
MAX_REJECT_RATE = 0.01


def s3_client():
    import boto3
    from botocore.config import Config
    return boto3.client(
        "s3", endpoint_url=S3_ENDPOINT, aws_access_key_id=S3_KEY, aws_secret_access_key=S3_SECRET,
        region_name="us-east-1", config=Config(signature_version="s3v4", s3={"addressing_style": "path"}),
    )


def ensure_buckets(names, retries=30):
    """Create buckets if missing. Retries because MinIO may still be starting."""
    s3 = s3_client()
    last = None
    for _ in range(retries):
        try:
            existing = {b["Name"] for b in s3.list_buckets()["Buckets"]}
            for n in names:
                if n not in existing:
                    s3.create_bucket(Bucket=n)
            return
        except Exception as e:  # noqa: BLE001
            last = e
            time.sleep(2)
    raise RuntimeError(f"object storage not reachable at {S3_ENDPOINT}: {last}")


def get_spark(app_name: str, s3: bool = True):
    from pyspark.sql import SparkSession
    b = (SparkSession.builder.appName(app_name)
         .master(os.getenv("SPARK_MASTER", "local[2]"))
         .config("spark.driver.memory", os.getenv("SPARK_DRIVER_MEMORY", "3g"))
         .config("spark.sql.session.timeZone", "UTC")
         .config("spark.sql.shuffle.partitions", "8")
         .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
         .config("spark.sql.parquet.outputTimestampType", "TIMESTAMP_MICROS")
         .config("spark.driver.extraJavaOptions", "-Duser.timezone=UTC")  # JDBC timestamps use the JVM zone
         .config("spark.ui.enabled", "false"))
    if s3:
        b = (b.config("spark.hadoop.fs.s3a.endpoint", S3_ENDPOINT)
             .config("spark.hadoop.fs.s3a.access.key", S3_KEY)
             .config("spark.hadoop.fs.s3a.secret.key", S3_SECRET)
             .config("spark.hadoop.fs.s3a.path.style.access", "true")
             .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
             .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem"))
    spark = b.getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    return spark


def s3a(bucket: str, *parts: str) -> str:
    return "s3a://" + bucket + "/" + "/".join(p.strip("/") for p in parts)
