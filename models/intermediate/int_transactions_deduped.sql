-- Equivalente a dedup.py (Window.partitionBy + row_number + filter(rn == 1)).
-- QUALIFY es un idiomatismo propio de Snowflake (no es SQL estándar): te deja
-- filtrar directamente el resultado de una función de ventana sin envolver
-- la consulta en un CTE/subquery solo para poder filtrar por "rn = 1".
-- El equivalente sin QUALIFY sería exactamente lo que ya conoces de PySpark:
--   with ranked as (select *, row_number() over (...) as rn from t)
--   select * from ranked where rn = 1

select *
from {{ ref('int_transactions_quarantined') }}
qualify row_number() over (
    partition by transaction_id
    order by transaction_date asc
) = 1
