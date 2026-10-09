-- Alerts must never be raised on provisional days (late events still arriving).
select a.creator_id, a.alert_date
from {{ ref('mart_creator_engagement_alerts') }} a
cross join {{ ref('mon_pipeline_state') }} s
where a.alert_date > s.complete_through
