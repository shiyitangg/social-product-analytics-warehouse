{# Segment labels are synthetic: 'new' = first 7 days after signup, the others follow the generator's activity levels. #}
select
    s.segment,
    case s.segment when 'new' then 1 when 'casual' then 2 when 'power' then 3 when 'dormant' then 4 else 99 end as sort_order
from (select distinct segment from {{ ref('dim_users') }} where segment is not null) s
