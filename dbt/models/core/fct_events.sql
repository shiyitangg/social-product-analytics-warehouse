{{
    config(
        materialized='incremental',
        unique_key='event_date',
        incremental_strategy='delete+insert',
        indexes=[
            {'columns': ['event_id'], 'unique': true},
            {'columns': ['event_date']}
        ]
    )
}}
{#
  Grain: one row per event. Each user is resolved to the SCD2 dimension version that was valid ON THE EVENT DATE
  (user_key), so a later change of country/device/segment never rewrites history.
  Incremental: replaces whole event_date partitions (delete+insert on event_date) from `incremental_start_date(this)`.
#}
select
    e.event_id,
    e.event_ts,
    e.event_date,
    u.user_key,
    e.user_id,
    e.event_type,
    e.content_id,
    c.creator_id,
    e.first_ingest_date
from {{ ref('stg_events') }} e
left join {{ ref('dim_users') }} u
       on u.user_id = e.user_id
      and e.event_date >= u.valid_from
      and (u.valid_to is null or e.event_date < u.valid_to)
left join {{ ref('dim_content') }} c
       on c.content_id = e.content_id
{% if is_incremental() %}
where e.event_date >= {{ incremental_start_date(this) }}
{% endif %}
