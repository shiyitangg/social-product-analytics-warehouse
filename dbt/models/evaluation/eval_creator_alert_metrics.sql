-- Precision / recall / detection delay of the creator engagement-decline alert against the planted ground truth.
select
    count(*) filter (where onset_date is not null)                                        as planted_declines,
    count(*) filter (where first_alert_date is not null)                                  as alerts_raised,
    count(*) filter (where outcome = 'true_positive')                                     as true_positives,
    count(*) filter (where outcome in ('false_positive', 'early_alert'))                  as false_alarms,
    count(*) filter (where outcome = 'missed')                                            as missed,
    round((count(*) filter (where outcome = 'true_positive'))::numeric
          / nullif(count(*) filter (where first_alert_date is not null), 0), 3)           as alert_precision,
    round((count(*) filter (where outcome = 'true_positive'))::numeric
          / nullif(count(*) filter (where onset_date is not null), 0), 3)                 as alert_recall,
    percentile_cont(0.5) within group (order by detection_delay_days)
        filter (where outcome = 'true_positive')                                          as median_detection_delay_days
from {{ ref('eval_creator_alert_detail') }}
