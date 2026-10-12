{#
  Dashboard fact 6: daily engagement received by each alerted creator, for the drill-down line chart.
  trailing_baseline_avg is the mean of the previous alert_baseline_days days (the alert's own baseline).
#}
select
    d.creator_id,
    d.metric_date,
    d.engagement_events_received,
    d.posts_created,
    round(avg(d.engagement_events_received) over (
        partition by d.creator_id order by d.metric_date
        rows between {{ var('alert_baseline_days') }} preceding and 1 preceding), 1) as trailing_baseline_avg,
    s.first_alert_date,
    (d.metric_date > st.complete_through) as is_provisional
from {{ ref('int_creator_daily_series') }} d
join {{ ref('mart_creator_alert_summary') }} s on s.creator_id = d.creator_id
cross join {{ ref('mon_pipeline_state') }} st
