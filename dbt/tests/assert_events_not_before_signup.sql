-- An event can never happen before the user signed up (guards the SCD2 as-of join and the generator contract).
select f.event_id, f.event_date, u.signup_date
from {{ ref('fct_events') }} f
join {{ ref('dim_users') }} u using (user_key)
where f.event_date < u.signup_date
