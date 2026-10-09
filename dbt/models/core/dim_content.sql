select
    content_id,
    creator_id,
    community_id,
    content_type,
    created_ts,
    created_date
from {{ ref('stg_content') }}
