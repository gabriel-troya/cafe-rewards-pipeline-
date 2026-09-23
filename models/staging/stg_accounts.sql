select
    account_id,
    account_type
from {{ ref('raw_accounts') }}
