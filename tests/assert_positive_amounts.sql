-- Test "singular": un archivo .sql normal en tests/ que dbt corre con `dbt test`.
-- La convención es: SI LA CONSULTA DEVUELVE FILAS, EL TEST FALLA.
-- Aquí: si queda alguna transacción con amount <= 0 en la tabla final, algo se rompió
-- en el pipeline (equivalente a un assert manual al final de tu pipeline de PySpark).

select *
from {{ ref('fct_transactions') }}
where amount <= 0
