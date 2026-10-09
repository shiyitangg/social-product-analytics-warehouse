{#
  Product-growth dashboard table. Grain: one row per event_date.
  DAU            = distinct users with >= 1 valid event that day
  content_creation_rate = posts created per DAU;  creator_share_of_dau = creators who posted / DAU
  is_provisional = the date is still inside the late-arrival window, so its counts can still grow.
#}
with state as (select complete_through from {{ ref('mon_pipeline_state') }}),

daily as (
    select
        event_date,
        count(*)                                  as dau,
        sum(events)                               as events_total,
        sum(app_opens)                            as app_opens,
        sum(content_views)                        as content_views,
        sum(interactions_made)                    as interactions,
        sum(posts_created)                        as posts_created,
        count(*) filter (where posts_created > 0) as creators_posting
    from {{ ref('int_user_activity_daily') }}
    group by event_date
),

signups as (
    select signup_date as event_date, count(*) as new_users
    from {{ ref('dim_users') }}
    where version_number = 1
    group by signup_date
)

select
    d.event_date,
    d.dau,
    coalesce(s.new_users, 0)                                   as new_users,
    d.events_total,
    d.app_opens,
    d.content_views,
    d.interactions,
    d.posts_created,
    d.creators_posting,
    round(d.posts_created::numeric / nullif(d.dau, 0), 5)      as content_creation_rate,
    round(d.creators_posting::numeric / nullif(d.dau, 0), 5)   as creator_share_of_dau,
    round(d.interactions::numeric / nullif(d.content_views, 0), 5) as interactions_per_view,
    round(avg(d.dau) over (order by d.event_date rows between 6 preceding and current row), 1) as dau_7d_avg,
    (d.event_date > st.complete_through)                        as is_provisional
from daily d
cross join state st
left join signups s on s.event_date = d.event_date
order by d.event_date
