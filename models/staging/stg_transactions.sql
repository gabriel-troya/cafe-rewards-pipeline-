-- Capa "staging": el equivalente conceptual a Bronze en tu pipeline de PySpark.
-- Solo tipifica y limpia nombres de columnas, no aplica ninguna regla de negocio todavía.

with source as (
    select * from {{ ref('raw_transactions') }}
)

select
    transaction_id,
    account_id,
    nullif(merchant_id, '') as merchant_id,   -- el seed trae '' en vez de NULL real para simular el dato sucio
    cast(amount as float) as amount,
    upper(trim(currency)) as currency,
    status,
    cast(transaction_date as timestamp_ntz) as transaction_date
from source
