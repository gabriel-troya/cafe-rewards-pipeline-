-- Equivalente exacto a quarantine.py: descarta registros con amount <= 0
-- o merchant_id nulo, antes de cualquier transformación más costosa.

select *
from {{ ref('stg_transactions') }}
where amount > 0
  and merchant_id is not null
