-- La tabla "Silver" final, materializada como tabla real (ver dbt_project.yml)
-- porque de aquí en adelante todos los reportes Gold la consultan repetidamente
-- -- el mismo motivo por el que en Spark cachearías silver_sdf con .cache().

select * from {{ ref('int_transactions_enriched') }}
