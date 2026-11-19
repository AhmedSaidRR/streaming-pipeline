select
    d.symbol,
    f.window_start,
    f.avg_price,
    f.price_range_pct,
    f.change_vs_prev_pct
from {{ ref('fct_price_1m') }} f
join {{ ref('dim_symbol') }} d using (symbol_key)
where f.is_volatile
  and f.window_start > (now() at time zone 'utc') - interval '24 hours'
order by f.window_start desc
