-- One row describing how far the pipeline has progressed and which event dates are still provisional.
-- Events can arrive up to `max_lag_days` after they happen, so an event_date is COMPLETE only once the lake has
-- ingested through event_date + max_lag_days. Metrics for later dates are provisional and must not raise alerts.
select
    (select max(ingest_date) from {{ ref('stg_ingest_stats') }})                        as latest_ingest_date,
    (select max(event_date)  from {{ ref('stg_events') }})                              as latest_event_date,
    (select max(ingest_date) from {{ ref('stg_ingest_stats') }}) - {{ var('max_lag_days') }} as complete_through,
    (select max(loaded_at) from {{ source('ops', 'loaded_partitions') }})               as last_loaded_at,
    (select count(*) from {{ ref('stg_events') }})                                      as warehouse_event_rows,
    now()                                                                               as computed_at
