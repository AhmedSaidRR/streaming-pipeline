select
    d.symbol,
    date_trunc('hour', f.window_start) as hour_start,
    (array_agg(f.avg_price order by f.window_start asc))[1]  as open_price,
    (array_agg(f.avg_price order by f.window_start desc))[1] as close_price,
    min(f.min_price) as low_price,
    max(f.max_price) as high_price,
    round((sum(f.avg_price * f.tick_count) / nullif(sum(f.tick_count), 0))::numeric, 8) as vwap_like_price,
    sum(f.tick_count) as ticks,
    count(*) as windows,
    count(*) filter (where f.is_volatile) as volatile_windows
from {{ ref('fct_price_1m') }} f
join {{ ref('dim_symbol') }} d using (symbol_key)
group by d.symbol, date_trunc('hour', f.window_start)
