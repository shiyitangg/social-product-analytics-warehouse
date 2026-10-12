-- The dashboard's activity fact, summed over segments and devices, must equal mart_daily_growth day by day.
select g.event_date, g.dau, a.active_users, g.events_total, a.events, g.posts_created, a.posts_created
from {{ ref('mart_daily_growth') }} g
join (
    select event_date, sum(active_users) as active_users, sum(events) as events, sum(posts_created) as posts_created,
           sum(content_views) as content_views, sum(interactions) as interactions, sum(new_users) as new_users
    from {{ ref('bi_daily_activity') }}
    group by event_date
) a using (event_date)
where g.dau <> a.active_users
   or g.events_total <> a.events
   or g.posts_created <> a.posts_created
   or g.content_views <> a.content_views
   or g.interactions <> a.interactions
   or g.new_users <> a.new_users
