-- Equivalente a monthly_transaction_trends() de trends.py.
-- to_char(fecha, 'YYYY-MM') es el DATE_FORMAT de Snowflake; LAG funciona
-- exactamente igual que en Spark SQL y en F.lag().

with monthly as (
    select
        to_char(transaction_date, 'YYYY-MM') as year_month,
        count(transaction_id) as transaction_count,
        round(sum(amount), 2) as total_amount
    from {{ ref('fct_transactions') }}
    group by 1
)

select
    year_month,
    transaction_count,
    total_amount,
    lag(transaction_count) over (order by year_month) as prev_month_count
from monthly
order by year_month
