{# Dashboard fact 5: one row per creator that triggered the engagement-decline alert (the alert list). #}
select
    s.creator_id,
    s.main_community_id,
    s.first_alert_date,
    s.last_alert_date,
    s.days_flagged,
    s.baseline_events_per_day,
    s.recent_events_per_day_at_alert,
    s.drop_pct_at_alert,
    s.severity,
    s.alert_active_on_latest_day,
    (st.complete_through - s.first_alert_date) as days_since_first_alert
from {{ ref('mart_creator_alert_summary') }} s
cross join {{ ref('mon_pipeline_state') }} st
