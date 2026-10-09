-- If a content_id was ever delivered twice, keep the latest delivery.
select distinct on (content_id)
    content_id,
    creator_id,
    community_id,
    content_type,
    created_ts,
    (created_ts at time zone 'UTC')::date as created_date,
    ingest_date
from {{ source('raw', 'content') }}
order by content_id, ingest_date desc
