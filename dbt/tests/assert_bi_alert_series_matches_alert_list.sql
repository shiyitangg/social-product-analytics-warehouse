-- Every alerted creator has a daily series, and every alert date falls inside that creator's series.
select a.creator_id
from {{ ref('bi_creator_alerts') }} a
left join (select creator_id, min(metric_date) as first_day, max(metric_date) as last_day
           from {{ ref('bi_creator_alert_series') }} group by creator_id) s using (creator_id)
where s.creator_id is null or a.first_alert_date < s.first_day or a.last_alert_date > s.last_day
