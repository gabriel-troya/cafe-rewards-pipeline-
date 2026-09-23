-- Snapshot = Slowly Changing Dimension tipo 2: cada vez que corras `dbt snapshot`,
-- dbt compara el estado actual de stg_limits contra la última foto guardada y,
-- si algo cambió para una misma clave, cierra la fila vieja (dbt_valid_to) y abre
-- una nueva (dbt_valid_from) -- sin que tengas que escribir esa lógica a mano.
-- Es la respuesta natural si te preguntan "¿cómo harías un SCD tipo 2 en dbt?"

{% snapshot limits_snapshot %}

{{
    config(
        target_schema='snapshots',
        unique_key="account_id || '-' || limit_type",
        strategy='timestamp',
        updated_at='effective_date',
    )
}}

select * from {{ ref('stg_limits') }}

{% endsnapshot %}
