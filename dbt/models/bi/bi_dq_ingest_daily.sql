{#
  Data-quality page: one row per ingest date from the Spark job's counters.
  quarantined_rows = raw rows rejected into quarantine (null user id, invalid event type, future timestamp, or too
  late); quarantine_rate is that share of the raw rows received that day.
#}
select
    ingest_date,
    rows_in,
    rows_rejected                as quarantined_rows,
    reject_rate                  as quarantine_rate,
    duplicate_rate,
    late_row_share,
    volume_vs_prior_7d,
    flag_high_reject_rate,
    flag_high_duplicate_rate,
    flag_volume_anomaly,
    is_tail
from {{ ref('mon_ingest_quality') }}
