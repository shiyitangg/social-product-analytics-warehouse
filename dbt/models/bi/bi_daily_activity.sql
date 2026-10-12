{#
  Dashboard fact 1: user activity.  Grain: event_date x segment x device.
  Segment and device are the user's values ON THAT DAY (the SCD2 version valid on event_date), and each user is in
  exactly one version per day, so active_users and every other measure can be summed across segments/devices.
  Ratios (interaction rate, content-creation rate, creator share) must be computed from the sums in the BI tool.
  new_users counts signups of that day by the segment/device at signup.
#}
with st as (select complete_through from {{ ref('mon_pipeline_state') }}),

act as (
    select a.event_date, a.user_id, a.events, a.app_opens, a.content_views, a.interactions_made, a.posts_created,
           coalesce(u.segment, 'unknown') as segment, coalesce(u.device, 'unknown') as device
    from {{ ref('int_user_activity_daily') }} a
    left join {{ ref('dim_users') }} u
           on u.user_id = a.user_id
          and a.event_date >= u.valid_from
          and (u.valid_to is null or a.event_date < u.valid_to)
),

activity as (
    select
        event_date, segment, device,
        count(*)                                  as active_users,
        sum(events)                               as events,
        sum(app_opens)                            as app_opens,
        sum(content_views)                        as content_views,
        sum(interactions_made)                    as interactions,
        sum(posts_created)                        as posts_created,
        count(*) filter (where posts_created > 0) as creators_posting
    from act
    group by event_date, segment, device
),

bounds as (select min(event_date) as first_day, max(event_date) as last_day from activity),

signups as (
    select signup_date as event_date, coalesce(segment, 'unknown') as segment, coalesce(device, 'unknown') as device,
           count(*) as new_users
    from {{ ref('dim_users') }}
    where version_number = 1
    group by signup_date, coalesce(segment, 'unknown'), coalesce(device, 'unknown')
)

select
    coalesce(a.event_date, s.event_date)  as event_date,
    coalesce(a.segment, s.segment)        as segment,
    coalesce(a.device, s.device)          as device,
    coalesce(a.active_users, 0)           as active_users,
    coalesce(s.new_users, 0)              as new_users,
    coalesce(a.events, 0)                 as events,
    coalesce(a.app_opens, 0)              as app_opens,
    coalesce(a.content_views, 0)          as content_views,
    coalesce(a.interactions, 0)           as interactions,
    coalesce(a.posts_created, 0)          as posts_created,
    coalesce(a.creators_posting, 0)       as creators_posting,
    (coalesce(a.event_date, s.event_date) > st.complete_through) as is_provisional
from activity a
full outer join signups s
     on s.event_date = a.event_date and s.segment = a.segment and s.device = a.device
cross join st
cross join bounds b
where coalesce(a.event_date, s.event_date) between b.first_day and b.last_day
