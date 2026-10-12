{# Calendar for the dashboard: from the Monday of the first event week to the latest event date. #}
select
    d.date_day,
    d.iso_day_of_week,
    d.day_name,
    d.is_weekend,
    d.week_start_date,
    d.month_start_date,
    d.year,
    (d.date_day > st.complete_through) as is_provisional
from {{ ref('dim_date') }} d
cross join {{ ref('mon_pipeline_state') }} st
where d.date_day >= (select date_trunc('week', min(event_date))::date from {{ ref('mart_daily_growth') }})
  and d.date_day <= st.latest_event_date
