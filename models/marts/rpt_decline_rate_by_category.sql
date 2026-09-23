-- Equivalente a decline_rate_by_category() de decline.py.

with agg as (
    select
        category,
        count(transaction_id) as total_transactions,
        sum(case when status = 'declined' then 1 else 0 end) as declined_count
    from {{ ref('fct_transactions') }}
    group by category
)

select
    category,
    total_transactions,
    declined_count,
    round(declined_count / total_transactions, 4) as decline_rate,
    rank() over (order by declined_count / total_transactions desc) as decline_rank
from agg
order by decline_rate desc
