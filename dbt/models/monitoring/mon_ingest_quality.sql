{#
  Per-ingest-date data-quality and volume monitoring, from the Spark job's own counters (ops.ingest_stats).
  Rates are shares of the raw rows received that day. Thresholds are deliberately above the synthetic data's
  injected baseline (0.2% bad rows, 0.5% duplicates) so a genuine regression shows up as a flag.
  Tail ingest dates after the last event day (only late redeliveries) are not volume-checked.
#}
with s as (select * from {{ ref('stg_ingest_stats') }}),
st as (select latest_event_date from {{ ref('mon_pipeline_state') }}),

rates as (
    select
        s.ingest_date,
        s.rows_in,
        s.rows_rejected,
        s.rows_rejected::numeric / nullif(s.rows_in, 0)                                          as reject_rate,
        coalesce((s.rejected_by_reason ->> 'null_user_id')::numeric, 0)      / nullif(s.rows_in, 0) as null_user_rate,
        coalesce((s.rejected_by_reason ->> 'invalid_event_type')::numeric, 0)/ nullif(s.rows_in, 0) as invalid_type_rate,
        coalesce((s.rejected_by_reason ->> 'future_timestamp')::numeric, 0)  / nullif(s.rows_in, 0) as future_ts_rate,
        coalesce((s.rejected_by_reason ->> 'too_late')::numeric, 0)          / nullif(s.rows_in, 0) as too_late_rate,
        s.duplicates_within_batch::numeric / nullif(s.rows_in, 0)                                as duplicate_rate,
        s.late_rows::numeric / nullif(s.rows_written_by_ingest, 0)                               as late_row_share,
        s.max_lag_days,
        s.rows_in::numeric / nullif(avg(s.rows_in) over (order by s.ingest_date
                                     rows between 7 preceding and 1 preceding), 0)               as volume_vs_prior_7d,
        count(*) over (order by s.ingest_date rows between 7 preceding and 1 preceding)          as prior_days,
        (s.ingest_date > st.latest_event_date)                                                   as is_tail
    from s cross join st
)

select
    *,
    (reject_rate > 0.005)                                                         as flag_high_reject_rate,
    (duplicate_rate > 0.01)                                                       as flag_high_duplicate_rate,
    (not is_tail and prior_days = 7 and (volume_vs_prior_7d < 0.7 or volume_vs_prior_7d > 1.4)) as flag_volume_anomaly
from rates
order by ingest_date
