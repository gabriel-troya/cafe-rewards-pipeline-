select
    account_id,
    limit_type,
    cast(limit_amount as float) as limit_amount,
    cast(effective_date as date) as effective_date
from {{ ref('raw_limits') }}
