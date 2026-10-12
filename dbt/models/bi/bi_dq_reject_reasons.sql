{# Data-quality page: quarantined rows by reason and ingest date. #}
select
    s.ingest_date,
    r.key            as reason,
    r.value::bigint  as quarantined_rows
from {{ ref('stg_ingest_stats') }} s
cross join lateral jsonb_each_text(s.rejected_by_reason) as r(key, value)
