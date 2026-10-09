{#
  One row per creator that has triggered the engagement-decline alert: the table a creator-partnerships team would
  work from. The alert fires when a drop STARTS; once the 14-day baseline has absorbed the lower level the rule stops
  firing, so alert_active_on_latest_day is true only for recent onsets - first_alert_date is the useful field.
#}
with alerts as (select * from {{ ref('mart_creator_engagement_alerts') }}),
state  as (select complete_through from {{ ref('mon_pipeline_state') }}),

per_creator as (
    select
        creator_id,
        min(alert_date) as first_alert_date,
        max(alert_date) as last_alert_date,
        count(*)        as days_flagged
    from alerts
    group by creator_id
),

first_row as (
    select distinct on (creator_id) creator_id, baseline_avg, recent_avg, drop_pct
    from alerts
    order by creator_id, alert_date
),

community as (
    select creator_id, mode() within group (order by community_id) as main_community_id
    from {{ ref('dim_content') }}
    group by creator_id
)

select
    p.creator_id,
    c.main_community_id,
    p.first_alert_date,
    p.last_alert_date,
    p.days_flagged,
    f.baseline_avg                                  as baseline_events_per_day,
    f.recent_avg                                    as recent_events_per_day_at_alert,
    f.drop_pct                                      as drop_pct_at_alert,
    case when f.drop_pct >= 0.7 then 'critical' else 'high' end as severity,
    (p.last_alert_date = st.complete_through)       as alert_active_on_latest_day
from per_creator p
join first_row f using (creator_id)
left join community c using (creator_id)
cross join state st
order by p.first_alert_date, p.creator_id
