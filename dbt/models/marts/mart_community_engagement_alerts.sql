{# Community-level engagement-decline alerts: the same detector on a smoother series, with community thresholds. #}
select
    entity_id              as community_id,
    metric_date            as alert_date,
    value                  as engagement_today,
    round(recent_avg, 1)   as recent_avg,
    round(baseline_avg, 1) as baseline_avg,
    round(1 - recent_avg / nullif(baseline_avg, 0), 3) as drop_pct
from (
    {{ engagement_decline_scored(
        ref('int_community_daily_series'), 'community_id', 'metric_date', 'engagement_events',
        var('alert_recent_days'), var('alert_baseline_days'), var('community_alert_min_baseline'), var('community_alert_ratio')) }}
) s
where is_decline
