{# Data-quality page: one row of freshness figures plus the check tally, as of the last dbt run. #}
with fr as (select * from {{ ref('mon_data_quality_checks') }} where check_name = 'freshness.hours_since_last_load'),
tally as (
    select count(*) filter (where status = 'pass') as checks_pass,
           count(*) filter (where status = 'warn') as checks_warn,
           count(*) filter (where status = 'fail') as checks_fail
    from {{ ref('mon_data_quality_checks') }}
)
select
    st.latest_event_date,
    st.complete_through,
    (st.latest_event_date - st.complete_through) as provisional_days,
    st.latest_ingest_date,
    st.last_loaded_at,
    st.warehouse_event_rows,
    fr.observed   as hours_since_last_load,
    fr.warn_above as freshness_warn_hours,
    fr.fail_above as freshness_fail_hours,
    fr.status     as freshness_status,
    t.checks_pass, t.checks_warn, t.checks_fail,
    now()         as snapshot_at
from {{ ref('mon_pipeline_state') }} st
cross join fr
cross join tally t
