select
    event_id,
    event_ts,
    event_date,
    user_id,
    event_type,
    content_id,
    first_ingest_date
from {{ source('raw', 'events') }}
