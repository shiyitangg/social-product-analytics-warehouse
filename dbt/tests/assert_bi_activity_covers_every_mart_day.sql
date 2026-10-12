-- Every day of mart_daily_growth must exist in the dashboard fact (a missing day would silently drop out of charts).
select g.event_date
from {{ ref('mart_daily_growth') }} g
left join (select distinct event_date from {{ ref('bi_daily_activity') }}) a using (event_date)
where a.event_date is null
