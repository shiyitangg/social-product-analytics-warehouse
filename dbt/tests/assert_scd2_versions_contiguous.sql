-- SCD2 integrity: for every user the versions must tile time exactly (each valid_to equals the next valid_from, no overlaps/gaps).
select user_id, version_number, valid_to, next_valid_from
from (
    select user_id, version_number, valid_to,
           lead(valid_from) over (partition by user_id order by version_number) as next_valid_from
    from {{ ref('dim_users') }}
) t
where valid_to is distinct from next_valid_from
