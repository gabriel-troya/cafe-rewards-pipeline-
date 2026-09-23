select
    merchant_id,
    name,
    category,
    country
from {{ ref('raw_merchants') }}
