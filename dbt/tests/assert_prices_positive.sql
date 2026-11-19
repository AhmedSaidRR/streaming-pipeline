select * from {{ ref('fct_price_1m') }}
where avg_price <= 0 or min_price <= 0 or max_price <= 0
