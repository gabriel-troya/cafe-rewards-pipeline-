{#
    Macro de ejemplo: centraliza la lista de monedas soportadas en un solo lugar
    (el mismo espíritu que SUPPORTED_CURRENCIES en fintech/models.py del repo de PySpark),
    para no repetir la lista en cada schema.yml que la necesite.
#}
{% macro supported_currencies() %}
    {{ return(['USD', 'EUR', 'GBP', 'CAD', 'MXN']) }}
{% endmacro %}
