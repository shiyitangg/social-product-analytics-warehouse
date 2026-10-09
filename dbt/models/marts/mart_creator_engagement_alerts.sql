{#
  Creator engagement-decline ALERTS. Grain: one row per creator per day on which the rule fires.
  Rule (see macros/engagement_decline.sql): every one of the last alert_recent_days days is <= alert_ratio x the
  mean of the alert_baseline_days days before them, and that baseline is >= alert_min_baseline events/day.
  Only COMPLETE days are scored (late events could otherwise trigger false alarms).
#}
select
    entity_id                       as creator_id,
    metric_date                     as alert_date,
    value                           as engagement_today,
    round(recent_avg, 1)            as recent_avg,
    round(baseline_avg, 1)          as baseline_avg,
    round(1 - recent_avg / nullif(baseline_avg, 0), 3) as drop_pct
from (
    {{ engagement_decline_scored(
        ref('int_creator_daily_series'), 'creator_id', 'metric_date', 'engagement_events_received',
        var('alert_recent_days'), var('alert_baseline_days'), var('alert_min_baseline'), var('alert_ratio')) }}
) s
where is_decline
