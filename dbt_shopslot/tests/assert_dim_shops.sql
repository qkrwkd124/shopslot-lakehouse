select
    shop_id,
    opened_on,
    closed_on
from {{ ref('dim_shops') }}
where closed_on < opened_on