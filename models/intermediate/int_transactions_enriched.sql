-- Equivalente a enrichment.py + limits.py combinados: join con merchants
-- para traer category/country, y join con el límite diario más reciente
-- por cuenta para calcular exceeds_daily_limit.

with transactions as (
    select * from {{ ref('int_transactions_deduped') }}
),

merchants as (
    select * from {{ ref('stg_merchants') }}
),

-- "el límite diario más reciente por cuenta" -- mismo patrón QUALIFY que en dedup,
-- aplicado ahora sobre limits en vez de sobre transactions.
latest_daily_limit as (
    select
        account_id,
        limit_amount as daily_limit
    from {{ ref('stg_limits') }}
    where limit_type = 'daily'
    qualify row_number() over (
        partition by account_id
        order by effective_date desc
    ) = 1
)

select
    t.*,
    m.category,
    m.country,
    l.daily_limit,
    -- Igual que en limits.py: sin límite diario registrado -> exceeds_daily_limit = false,
    -- nunca NULL. El "and" corto-circuita antes de comparar contra un daily_limit nulo.
    case
        when l.daily_limit is not null and t.amount > l.daily_limit then true
        else false
    end as exceeds_daily_limit
from transactions t
left join merchants m on t.merchant_id = m.merchant_id
left join latest_daily_limit l on t.account_id = l.account_id
