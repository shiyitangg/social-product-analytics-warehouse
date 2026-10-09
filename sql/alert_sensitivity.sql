-- Sensitivity of the creator engagement-decline alert to its ratio threshold (SYNTHETIC data).
-- Re-scores marts.int_creator_daily_series with several ratios using the same rule as
-- dbt/macros/engagement_decline.sql (all of the last 3 days <= ratio x mean of the 14 days before,
-- baseline >= 20 events/day) and compares with the planted ground truth in evaluation.planted_creator_declines.
-- NOTE: the shipped ratio (0.5) was chosen while looking at this same dataset, so its numbers are in-sample.
with ratios(ratio) as (values (0.4), (0.5), (0.6), (0.7)),
scored as (
    select s.creator_id, s.metric_date,
           max(s.engagement_events_received) over w_recent as recent_max,
           avg(s.engagement_events_received) over w_base   as baseline_avg,
           count(*) over w_recent as recent_n,
           count(*) over w_base   as baseline_n
    from marts.int_creator_daily_series s
    window w_recent as (partition by s.creator_id order by s.metric_date rows between 2 preceding and current row),
           w_base   as (partition by s.creator_id order by s.metric_date rows between 16 preceding and 3 preceding)
),
first_alert as (
    select r.ratio, c.creator_id, min(c.metric_date) as first_alert_date
    from scored c cross join ratios r
    where c.baseline_n = 14 and c.recent_n = 3 and c.baseline_avg >= 20 and c.recent_max <= r.ratio * c.baseline_avg
    group by r.ratio, c.creator_id
),
planted as (select creator_id, onset_date from evaluation.planted_creator_declines),
joined as (
    select r.ratio, coalesce(p.creator_id, a.creator_id) as creator_id, p.onset_date, a.first_alert_date,
           case when p.creator_id is not null and a.creator_id is null then 'missed'
                when p.creator_id is null then 'false_positive'
                when a.first_alert_date >= p.onset_date then 'true_positive'
                else 'early_alert' end as outcome
    from ratios r
    cross join planted p
    left join first_alert a on a.ratio = r.ratio and a.creator_id = p.creator_id
    union all
    select a.ratio, a.creator_id, null, a.first_alert_date, 'false_positive'
    from first_alert a where not exists (select 1 from planted p where p.creator_id = a.creator_id)
)
select ratio,
       count(*) filter (where first_alert_date is not null)                         as alerts_raised,
       count(*) filter (where outcome = 'true_positive')                            as true_positives,
       count(*) filter (where outcome in ('false_positive','early_alert'))          as false_alarms,
       count(*) filter (where outcome = 'missed')                                   as missed,
       round(count(*) filter (where outcome = 'true_positive')::numeric
             / nullif(count(*) filter (where first_alert_date is not null), 0), 3)  as precision,
       round(count(*) filter (where outcome = 'true_positive')::numeric
             / nullif(count(*) filter (where onset_date is not null), 0), 3)        as recall
from joined
group by ratio
order by ratio;
