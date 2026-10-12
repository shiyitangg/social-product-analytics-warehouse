-- WAU summed over segments must equal the number of distinct users active in that week.
select w.week_start_date, w.wau, d.distinct_users
from (select week_start_date, sum(wau) as wau from {{ ref('bi_weekly_active') }} group by week_start_date) w
join (
    select date_trunc('week', event_date)::date as week_start_date, count(distinct user_id) as distinct_users
    from {{ ref('int_user_activity_daily') }}
    group by 1
) d using (week_start_date)
where w.wau <> d.distinct_users
