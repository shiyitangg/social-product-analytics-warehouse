{{
    config(
        materialized='incremental',
        unique_key='event_date',
        incremental_strategy='delete+insert',
        indexes=[{'columns': ['event_date']}, {'columns': ['community_id', 'event_date']}]
    )
}}
-- Grain: content_id x event_date. Consumption of content (the content_create event itself is excluded).
select
    f.event_date,
    f.content_id,
    f.creator_id,
    c.community_id,
    count(*) filter (where f.event_type = 'content_view')  as views,
    count(*) filter (where f.event_type = 'like')          as likes,
    count(*) filter (where f.event_type = 'comment')       as comments,
    count(*) filter (where f.event_type = 'share')         as shares,
    count(*) filter (where f.event_type = 'follow')        as follows,
    count(distinct f.user_id) filter (where f.event_type = 'content_view') as unique_viewers,
    count(distinct f.user_id) filter (where f.event_type in ('like','comment','share','follow')) as unique_engagers,
    (count(*) filter (where f.event_type in ('like','comment','share','follow')))::numeric
        / nullif(count(*) filter (where f.event_type = 'content_view'), 0)      as interactions_per_view
from {{ ref('fct_events') }} f
left join {{ ref('dim_content') }} c on c.content_id = f.content_id
where f.content_id is not null
  and f.event_type <> 'content_create'
{% if is_incremental() %}
  and f.event_date >= {{ incremental_start_date(this) }}
{% endif %}
group by f.event_date, f.content_id, f.creator_id, c.community_id
