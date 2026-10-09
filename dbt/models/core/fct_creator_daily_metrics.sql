{{
    config(
        materialized='incremental',
        unique_key='event_date',
        incremental_strategy='delete+insert',
        indexes=[{'columns': ['creator_id', 'event_date']}]
    )
}}
{#
  Grain: creator_id x event_date (days with at least one post or one received event).
  "engagement_events_received" = views + likes + comments + shares + follows on the creator's content, EXCLUDING
  the creator's own activity. This is the series the engagement-decline alert watches.
#}
with received as (
    select
        event_date,
        creator_id,
        count(*) filter (where event_type = 'content_view') as views_received,
        count(*) filter (where event_type = 'like')         as likes_received,
        count(*) filter (where event_type = 'comment')      as comments_received,
        count(*) filter (where event_type = 'share')        as shares_received,
        count(*) filter (where event_type = 'follow')       as follows_received,
        count(*)                                            as engagement_events_received,
        count(distinct user_id)                             as unique_audience
    from {{ ref('fct_events') }}
    where creator_id is not null
      and user_id <> creator_id
      and event_type <> 'content_create'
    {% if is_incremental() %}
      and event_date >= {{ incremental_start_date(this) }}
    {% endif %}
    group by event_date, creator_id
),

posted as (
    select event_date, user_id as creator_id, count(*) as posts_created
    from {{ ref('fct_events') }}
    where event_type = 'content_create'
    {% if is_incremental() %}
      and event_date >= {{ incremental_start_date(this) }}
    {% endif %}
    group by event_date, user_id
)

select
    coalesce(r.event_date, p.event_date)   as event_date,
    coalesce(r.creator_id, p.creator_id)   as creator_id,
    coalesce(p.posts_created, 0)           as posts_created,
    coalesce(r.views_received, 0)          as views_received,
    coalesce(r.likes_received, 0)          as likes_received,
    coalesce(r.comments_received, 0)       as comments_received,
    coalesce(r.shares_received, 0)         as shares_received,
    coalesce(r.follows_received, 0)        as follows_received,
    coalesce(r.engagement_events_received, 0) as engagement_events_received,
    coalesce(r.unique_audience, 0)         as unique_audience
from received r
full outer join posted p
    on p.event_date = r.event_date and p.creator_id = r.creator_id
