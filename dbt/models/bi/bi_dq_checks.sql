{# Data-quality page: the observability checks as they stood at the last dbt run (the publish gate reads the same table). #}
select check_name, table_name, category, observed, warn_above, fail_above, status, checked_at
from {{ ref('mon_data_quality_checks') }}
