select
    ingest_date,
    rows_in,
    rows_rejected,
    duplicates_within_batch,
    late_rows,
    max_lag_days,
    rows_written_by_ingest,
    coalesce(nullif(rejected_by_reason, '')::jsonb, '{}'::jsonb) as rejected_by_reason,
    seconds
from {{ source('ops', 'ingest_stats') }}
