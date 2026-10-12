{#
  Dashboard fact 3: content activity.  Grain: event_date x community x content_type (events that belong to a piece
  of content only; app_open and follow events without content are not here). All measures are additive.
  interactions = likes + comments + shares + follows.
#}
with st as (select complete_through from {{ ref('mon_pipeline_state') }})

select
    f.event_date,
    coalesce(c.community_id, -1)                               as community_id,
    coalesce(c.content_type, 'unknown')                        as content_type,
    count(*) filter (where f.event_type = 'content_create')    as posts_created,
    count(*) filter (where f.event_type = 'content_view')      as views,
    count(*) filter (where f.event_type = 'like')              as likes,
    count(*) filter (where f.event_type = 'comment')           as comments,
    count(*) filter (where f.event_type = 'share')             as shares,
    count(*) filter (where f.event_type = 'follow')            as follows,
    count(*) filter (where f.event_type in ('like', 'comment', 'share', 'follow')) as interactions,
    (f.event_date > (select complete_through from st))         as is_provisional
from {{ ref('fct_events') }} f
left join {{ ref('dim_content') }} c on c.content_id = f.content_id
where f.content_id is not null
group by f.event_date, coalesce(c.community_id, -1), coalesce(c.content_type, 'unknown')
