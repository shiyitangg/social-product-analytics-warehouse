{{ config(materialized='table', schema='marts') }}
with state as (select complete_through from {{ ref('mon_pipeline_state') }}),
span as (select min(event_date) as first_day from {{ ref('fct_content_engagement') }}),
communities as (select distinct community_id from {{ ref('dim_content') }} where community_id is not null),
daily as (
    select community_id, event_date,
           sum(views + likes + comments + shares + follows) as engagement_events
    from {{ ref('fct_content_engagement') }}
    where community_id is not null
    group by community_id, event_date
)
select c.community_id, d.date_day as metric_date, coalesce(x.engagement_events, 0) as engagement_events
from communities c
cross join span
cross join state st
join {{ ref('dim_date') }} d on d.date_day between span.first_day and st.complete_through
left join daily x on x.community_id = c.community_id and x.event_date = d.date_day
