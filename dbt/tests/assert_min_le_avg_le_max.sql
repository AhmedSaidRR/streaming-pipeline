select * from {{ ref('fct_price_1m') }}
where min_price > avg_price or avg_price > max_price
