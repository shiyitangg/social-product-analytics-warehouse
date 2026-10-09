{{ config(materialized='table', schema='marts') }}
-- Dense creator x day series (zeros filled in) up to the last COMPLETE day. Dense is required so that
-- "the previous 14 rows" in the window functions really means "the previous 14 days".
with state as (select complete_through from {{ ref('mon_pipeline_state') }}),
span as (select min(event_date) as first_day from {{ ref('fct_creator_daily_metrics') }}),
creators as (select distinct user_id as creator_id from {{ ref('dim_users') }} where is_creator)

select
    c.creator_id,
    d.date_day                                        as metric_date,
    coalesce(m.engagement_events_received, 0)         as engagement_events_received,
    coalesce(m.posts_created, 0)                      as posts_created
from creators c
cross join span
cross join state st
join {{ ref('dim_date') }} d on d.date_day between span.first_day and st.complete_through
left join {{ ref('fct_creator_daily_metrics') }} m
       on m.creator_id = c.creator_id and m.event_date = d.date_day
