-- Reconciliation: every event_date has exactly as many rows in fct_events as in raw.events (no row lost or doubled by modelling).
with a as (select event_date, count(*) n from {{ ref('stg_events') }} group by 1),
     b as (select event_date, count(*) n from {{ ref('fct_events') }} group by 1)
select coalesce(a.event_date, b.event_date) as event_date, a.n as raw_rows, b.n as fact_rows
from a full outer join b using (event_date)
where a.n is distinct from b.n
