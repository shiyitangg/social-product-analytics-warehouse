"""Row-level validation rules for raw events, written as Spark column expressions.

The first matching rule wins and its name becomes `reject_reason`; NULL means the row is valid.
"""
from pyspark.sql import DataFrame, functions as F


def add_reject_reason(df: DataFrame, ingest_date: str, max_lag_days: int, valid_types) -> DataFrame:
    ev_date = F.to_date(F.col("event_ts"))
    lag = F.datediff(F.to_date(F.lit(ingest_date)), ev_date)   # days between event and delivery
    reason = (
        F.when(F.col("event_id").isNull(), "null_event_id")
        .when(F.col("user_id").isNull(), "null_user_id")
        .when(F.col("event_ts").isNull(), "null_event_ts")
        .when(F.col("event_type").isNull() | (~F.col("event_type").isin(valid_types)), "invalid_event_type")
        .when(lag < 0, "future_timestamp")                 # delivered before it happened
        .when(lag > max_lag_days, "too_late")              # beyond the supported late-arrival window
        .when((F.col("event_type") != "app_open") & F.col("content_id").isNull(), "missing_content_id")
        .otherwise(F.lit(None).cast("string"))
    )
    return df.withColumn("reject_reason", reason)
