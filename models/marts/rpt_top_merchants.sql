-- Equivalente a top_merchants_by_volume() de merchants.py

select
    m.merchant_id,
    m.name,
    m.category,
    count(f.transaction_id) as total_transactions,
    round(sum(f.amount), 2) as total_amount
from {{ ref('fct_transactions') }} f
join {{ ref('stg_merchants') }} m on f.merchant_id = m.merchant_id
where f.status != 'declined'
group by m.merchant_id, m.name, m.category
order by total_transactions desc
limit 5
