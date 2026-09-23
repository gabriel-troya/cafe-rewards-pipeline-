-- Equivalente a category_spend_report() de category_report.py --
-- pero aquí NO hace falta createOrReplaceTempView + spark.sql(): en dbt,
-- cada modelo YA es una consulta SQL pura por definición.

select
    category,
    count(transaction_id) as transaction_count,
    round(sum(amount), 2) as total_spend
from {{ ref('fct_transactions') }}
where status = 'approved'
group by category
order by total_spend desc
