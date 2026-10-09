with bounds as (
    select
        least((select min(signup_date) from {{ ref('stg_user_snapshots') }}),
              (select min(event_date)  from {{ ref('stg_events') }}))  as first_day,
        (select max(event_date) from {{ ref('stg_events') }}) + 14    as last_day
)
select
    d::date                                         as date_day,
    extract(isodow from d)::int                     as iso_day_of_week,
    to_char(d, 'Dy')                                as day_name,
    (extract(isodow from d) in (6, 7))              as is_weekend,
    date_trunc('week', d)::date                     as week_start_date,
    date_trunc('month', d)::date                    as month_start_date,
    extract(year from d)::int                       as year
from bounds, generate_series(first_day, last_day, interval '1 day') as d
