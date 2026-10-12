{#
  Dashboard fact 4: retention by signup cohort.  Grain: cohort (signup) date x device at signup.
  (Everyone starts in the 'new' segment, so segment is not a useful cut for cohorts; device is.)
  Day-N retention = share of the cohort active on EXACTLY day N. A cohort counts towards day N only once day N is
  COMPLETE (not provisional): dN_eligible is the cohort size in that case, else 0, and retained_dN is 0 in that case.
  Retention rate in the BI tool = SUM(retained_dN) / SUM(dN_eligible).
#}
with state as (select complete_through from {{ ref('mon_pipeline_state') }}),
bounds as (select min(event_date) as first_day, max(event_date) as last_day from {{ ref('int_user_activity_daily') }}),

cohort as (
    select user_id, signup_date, coalesce(device, 'unknown') as device
    from {{ ref('dim_users') }}
    where version_number = 1
      and signup_date between (select first_day from bounds) and (select last_day from bounds)
),

per_user as (
    select c.signup_date as cohort_date, c.device,
           (a1.user_id is not null) as active_d1,
           (a7.user_id is not null) as active_d7
    from cohort c
    left join {{ ref('int_user_activity_daily') }} a1 on a1.user_id = c.user_id and a1.event_date = c.signup_date + 1
    left join {{ ref('int_user_activity_daily') }} a7 on a7.user_id = c.user_id and a7.event_date = c.signup_date + 7
)

select
    cohort_date,
    device,
    count(*) as cohort_size,
    case when cohort_date + 1 <= (select complete_through from state) then count(*) else 0 end as d1_eligible,
    case when cohort_date + 1 <= (select complete_through from state)
         then count(*) filter (where active_d1) else 0 end                                    as retained_d1,
    case when cohort_date + 7 <= (select complete_through from state) then count(*) else 0 end as d7_eligible,
    case when cohort_date + 7 <= (select complete_through from state)
         then count(*) filter (where active_d7) else 0 end                                    as retained_d7
from per_user
group by cohort_date, device
