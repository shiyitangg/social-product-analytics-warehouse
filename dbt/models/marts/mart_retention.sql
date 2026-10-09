{#
  Cohort retention. Cohort = users by signup date (only users who signed up inside the data window; earlier
  signups are "existing users", not a cohort). Day-N retention = share of the cohort active on EXACTLY day N.
  A cohort is reported only once day N is COMPLETE (not provisional), otherwise late events would understate it.
#}
with state as (select complete_through from {{ ref('mon_pipeline_state') }}),
first_day as (select min(event_date) as d from {{ ref('int_user_activity_daily') }}),

cohort as (
    select user_id, signup_date
    from {{ ref('dim_users') }}
    where version_number = 1
      and signup_date >= (select d from first_day)
),

sized as (
    select signup_date as cohort_date, count(*) as cohort_size from cohort group by signup_date
),

retained as (
    select
        c.signup_date as cohort_date,
        count(*) filter (where a1.user_id is not null)  as retained_d1,
        count(*) filter (where a7.user_id is not null)  as retained_d7
    from cohort c
    left join {{ ref('int_user_activity_daily') }} a1
           on a1.user_id = c.user_id and a1.event_date = c.signup_date + 1
    left join {{ ref('int_user_activity_daily') }} a7
           on a7.user_id = c.user_id and a7.event_date = c.signup_date + 7
    group by c.signup_date
)

select
    s.cohort_date,
    s.cohort_size,
    r.retained_d1,
    r.retained_d7,
    round(r.retained_d1::numeric / s.cohort_size, 4) as d1_retention,
    round(r.retained_d7::numeric / s.cohort_size, 4) as d7_retention
from sized s
join retained r using (cohort_date)
cross join state st
where s.cohort_date + 7 <= st.complete_through
order by s.cohort_date
