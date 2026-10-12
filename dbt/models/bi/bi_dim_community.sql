{# Communities only have numeric ids in the source data, so the label is generated. #}
select
    c.community_id,
    'Community ' || lpad(c.community_id::text, 3, '0') as community_label
from (select distinct community_id from {{ ref('dim_content') }} where community_id is not null) c
