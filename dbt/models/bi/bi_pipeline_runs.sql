{#
  Data-quality page: pipeline run status per ingest date, from the orchestrator's own log tables.
    ops.publish_log      one row per run that passed the quality gate and published
    ops.pipeline_alerts  one row per failed task attempt (retries and gate blocks)
  run_status: 'failed'                the latest failure is newer than the latest publish (or nothing was published)
              'published after retry' published, but at least one attempt failed before it
              'published'             published with no failed attempts
              'ingested, no run'      the batch is in the lake but no orchestrated run is recorded for it
#}
with dates as (
    select ingest_date from {{ ref('stg_ingest_stats') }}
    union select ingest_date from {{ source('ops', 'publish_log') }} where ingest_date is not null
    union select ingest_date from {{ source('ops', 'pipeline_alerts') }} where ingest_date is not null
),

pub as (
    select ingest_date,
           count(*)          as publish_runs,
           max(published_at) as last_published_at,
           (array_agg(checks_total order by published_at desc))[1] as checks_total,
           (array_agg(checks_warn  order by published_at desc))[1] as checks_warn
    from {{ source('ops', 'publish_log') }}
    group by ingest_date
),

fail as (
    select ingest_date,
           count(*)        as failed_attempts,
           max(created_at) as last_failure_at,
           (array_agg(task_id order by created_at desc))[1]       as last_failed_task,
           (array_agg(left(error, 200) order by created_at desc))[1] as last_error
    from {{ source('ops', 'pipeline_alerts') }}
    group by ingest_date
)

select
    d.ingest_date,
    s.rows_in,
    s.seconds                          as ingest_seconds,
    (s.ingest_date is not null)        as ingested,
    coalesce(p.publish_runs, 0)        as publish_runs,
    p.last_published_at,
    p.checks_total,
    p.checks_warn,
    coalesce(f.failed_attempts, 0)     as failed_attempts,
    f.last_failure_at,
    f.last_failed_task,
    f.last_error,
    case
        when f.last_failure_at is not null and (p.last_published_at is null or p.last_published_at < f.last_failure_at)
            then 'failed'
        when p.last_published_at is not null and f.failed_attempts > 0 then 'published after retry'
        when p.last_published_at is not null                           then 'published'
        else 'ingested, no run'
    end as run_status
from dates d
left join {{ ref('stg_ingest_stats') }} s on s.ingest_date = d.ingest_date
left join pub  p on p.ingest_date = d.ingest_date
left join fail f on f.ingest_date = d.ingest_date
