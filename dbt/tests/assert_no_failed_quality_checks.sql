-- Quality gate: fails when any observability check is in 'fail' status.
{{ config(severity='error') }}
select check_name, observed, fail_above
from {{ ref('mon_data_quality_checks') }}
where status = 'fail'
