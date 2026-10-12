-- Content fact vs the event-level facts: posts created and views per day must match exactly,
-- and interactions must match fct_content_engagement (which also only counts events that carry a content id).
with c as (
    select event_date, sum(posts_created) as posts, sum(views) as views, sum(interactions) as interactions
    from {{ ref('bi_daily_content') }}
    group by event_date
),
m as (select event_date, posts_created, content_views from {{ ref('mart_daily_growth') }}),
e as (
    select event_date, sum(likes + comments + shares + follows) as interactions
    from {{ ref('fct_content_engagement') }}
    group by event_date
)
select c.event_date, c.posts, m.posts_created, c.views, m.content_views, c.interactions, e.interactions as engagement_interactions
from c
join m using (event_date)
left join e using (event_date)
where c.posts <> m.posts_created
   or c.views <> m.content_views
   or c.interactions <> coalesce(e.interactions, 0)
