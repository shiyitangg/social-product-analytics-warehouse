{#
  The observability surface. One row per check with the observed value, warn/fail thresholds and a status.
  The orchestrator's final gate reads this table: any row with status = 'fail' blocks publication.
#}
with ev as (
    select
        count(*)                                                           as n,
        count(distinct event_id)                                           as n_distinct,
        count(*) filter (where user_key is null)                           as n_no_user_key,
        count(*) filter (where content_id is not null and event_type <> 'content_create') as n_content_events,
        count(*) filter (where content_id is not null and event_type <> 'content_create' and creator_id is null) as n_no_creator
    from {{ ref('fct_events') }}
),
raw_n as (select count(*) as n from {{ source('raw', 'events') }}),
loaded as (
    select coalesce(sum(row_count), 0) as n, max(loaded_at) as last_loaded_at
    from {{ source('ops', 'loaded_partitions') }} where dataset = 'events'
),
q7 as (
    select max(reject_rate) as max_reject, max(duplicate_rate) as max_dup, max(null_user_rate) as max_null_user
    from (select * from {{ ref('mon_ingest_quality') }} order by ingest_date desc limit 7) t
),
state as (select * from {{ ref('mon_pipeline_state') }}),
scd as (
    select count(*) as bad_users from (
        select user_id from {{ ref('dim_users') }} group by user_id
        having count(*) filter (where is_current) <> 1
    ) t
),
missing_days as (
    select count(*) as n
    from {{ ref('dim_date') }} d
    cross join state
    left join {{ ref('mart_daily_growth') }} g on g.event_date = d.date_day
    where d.date_day between (select min(event_date) from {{ ref('mart_daily_growth') }}) and state.complete_through
      and g.event_date is null
),

checks as (
    select 'fct_events.duplicate_event_rate' as check_name, 'fct_events' as table_name, 'duplicate rate'  as category,
           (ev.n - ev.n_distinct)::numeric / nullif(ev.n, 0) as observed, 0::numeric as warn_above, 0::numeric as fail_above from ev
    union all
    select 'fct_events.null_user_key_rate', 'fct_events', 'null rate',
           ev.n_no_user_key::numeric / nullif(ev.n, 0), 0, 0 from ev
    union all
    select 'fct_events.null_creator_rate_on_content_events', 'fct_events', 'null rate',
           ev.n_no_creator::numeric / nullif(ev.n_content_events, 0), 0.0005, 0.01 from ev
    union all
    select 'warehouse.fct_events_vs_raw_row_gap', 'fct_events', 'completeness',
           abs(ev.n - raw_n.n)::numeric, 0, 0 from ev, raw_n
    union all
    select 'warehouse.raw_vs_lake_row_gap', 'raw.events', 'completeness',
           abs(raw_n.n - loaded.n)::numeric, 0, 0 from raw_n, loaded
    union all
    select 'freshness.hours_since_last_load', 'raw.events', 'freshness',
           extract(epoch from (now() - loaded.last_loaded_at)) / 3600.0, 36, 168 from loaded
    union all
    select 'ingest.max_reject_rate_last_7_batches', 'ops.ingest_stats', 'null rate', q7.max_reject, 0.005, 0.01 from q7
    union all
    select 'ingest.max_null_user_rate_last_7_batches', 'ops.ingest_stats', 'null rate', q7.max_null_user, 0.003, 0.01 from q7
    union all
    select 'ingest.max_duplicate_rate_last_7_batches', 'ops.ingest_stats', 'duplicate rate', q7.max_dup, 0.01, 0.05 from q7
    union all
    select 'dim_users.users_without_exactly_one_current_row', 'dim_users', 'integrity',
           scd.bad_users::numeric, 0, 0 from scd
    union all
    select 'mart_daily_growth.missing_complete_days', 'mart_daily_growth', 'completeness',
           missing_days.n::numeric, 0, 0 from missing_days
)

select
    check_name, table_name, category,
    round(observed, 6) as observed,
    warn_above, fail_above,
    case when observed > fail_above then 'fail'
         when observed > warn_above then 'warn'
         else 'pass' end as status,
    now() as checked_at
from checks
order by check_name
