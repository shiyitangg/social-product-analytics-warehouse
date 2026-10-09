{#
  SCD Type 2 over the daily user snapshots.
  A new version starts whenever country, device or segment differs from the previous snapshot.
  - valid_from : first snapshot date showing the new values (version 1: the signup date, so events on the
                 signup day resolve). Changes are only known at snapshot granularity (daily).
  - valid_to   : exclusive end (the next version's valid_from); NULL for the current version.
  Join facts with:  event_date >= valid_from and (valid_to is null or event_date < valid_to)
#}
with flagged as (
    select
        user_id, snapshot_date, signup_date, country, device, segment, is_creator,
        case when lag(country) over w is distinct from country
               or lag(device)  over w is distinct from device
               or lag(segment) over w is distinct from segment
             then 1 else 0 end as is_change
    from {{ ref('stg_user_snapshots') }}
    window w as (partition by user_id order by snapshot_date)
),

numbered as (
    select *, sum(is_change) over (partition by user_id order by snapshot_date) as version_number
    from flagged
),

versions as (
    select
        user_id,
        version_number,
        min(snapshot_date)  as first_seen_date,
        max(snapshot_date)  as last_seen_date,
        min(signup_date)    as signup_date,
        min(country)        as country,
        min(device)         as device,
        min(segment)        as segment,
        bool_or(is_creator) as is_creator
    from numbered
    group by user_id, version_number
)

select
    md5(user_id::text || '|' || (case when version_number = 1 then least(signup_date, first_seen_date)
                                       else first_seen_date end)::text)            as user_key,
    user_id,
    version_number,
    country,
    device,
    segment,
    is_creator,
    signup_date,
    case when version_number = 1 then least(signup_date, first_seen_date) else first_seen_date end as valid_from,
    lead(first_seen_date) over (partition by user_id order by version_number)      as valid_to,
    (lead(first_seen_date) over (partition by user_id order by version_number) is null) as is_current,
    last_seen_date
from versions
