{#
  Dashboard fact 2: weekly active users.  Grain: ISO week (Monday) x segment.
  A user active on several days is counted ONCE per week, in the segment they had on their last active day of that
  week, so WAU can be summed across segments. is_full_week is false for the first/last partial weeks and for weeks
  that are not yet complete (late events can still arrive), and those weeks should not be compared with full ones.
#}
with st as (select complete_through from {{ ref('mon_pipeline_state') }}),
bounds as (select min(event_date) as first_day from {{ ref('int_user_activity_daily') }}),

act as (
    select date_trunc('week', a.event_date)::date as week_start_date, a.event_date, a.user_id,
           coalesce(u.segment, 'unknown') as segment
    from {{ ref('int_user_activity_daily') }} a
    left join {{ ref('dim_users') }} u
           on u.user_id = a.user_id
          and a.event_date >= u.valid_from
          and (u.valid_to is null or a.event_date < u.valid_to)
),

user_week as (
    select distinct on (week_start_date, user_id) week_start_date, user_id, segment
    from act
    order by week_start_date, user_id, event_date desc
)

select
    w.week_start_date,
    w.segment,
    count(*) as wau,
    (w.week_start_date >= b.first_day and w.week_start_date + 6 <= st.complete_through) as is_full_week
from user_week w
cross join st
cross join bounds b
group by w.week_start_date, w.segment, b.first_day, st.complete_through
