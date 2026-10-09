select
    user_id,
    snapshot_date,
    signup_date,
    country,
    device,
    segment,
    is_creator
from {{ source('raw', 'users_snapshot') }}
