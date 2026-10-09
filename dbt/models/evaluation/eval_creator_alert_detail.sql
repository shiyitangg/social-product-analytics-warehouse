{#
  EVALUATION ONLY (synthetic data). Compares the alert table with the declines the generator planted
  (seeds/planted_creator_declines.csv, which the pipeline itself never reads). One row per creator that either
  has a planted decline or raised an alert.
    true_positive  : planted decline AND first alert on/after the onset date
    early_alert    : planted creator alerted BEFORE the onset (counted as a false alarm)
    false_positive : alert on a creator with no planted decline
    missed         : planted decline with no alert
#}
select
    coalesce(g.creator_id, a.creator_id)                         as creator_id,
    g.onset_date,
    g.decline_factor,
    a.first_alert_date,
    (a.first_alert_date - g.onset_date)                          as detection_delay_days,
    case
        when g.creator_id is not null and a.creator_id is null          then 'missed'
        when g.creator_id is null                                       then 'false_positive'
        when a.first_alert_date >= g.onset_date                         then 'true_positive'
        else 'early_alert'
    end                                                          as outcome
from {{ ref('planted_creator_declines') }} g
full outer join {{ ref('mart_creator_alert_summary') }} a
    on a.creator_id = g.creator_id
