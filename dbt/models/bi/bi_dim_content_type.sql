select t.content_type
from (select distinct content_type from {{ ref('dim_content') }} where content_type is not null) t
