select d.device
from (select distinct device from {{ ref('dim_users') }} where device is not null) d
