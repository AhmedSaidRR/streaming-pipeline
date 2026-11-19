-- Grain: one row per symbol per 1-minute window.
select
    d.symbol_key,
    s.window_start,
    s.window_end,
    s.avg_price,
    s.min_price,
    s.max_price,
    s.tick_count,
    round(((s.max_price - s.min_price) / nullif(s.avg_price, 0) * 100)::numeric, 4) as price_range_pct,
    round(((s.max_price - s.min_price) / nullif(s.avg_price, 0) * 100)::numeric, 4)
        > {{ var('volatility_threshold_pct') }} as is_volatile,
    round(((s.avg_price / nullif(lag(s.avg_price) over (
        partition by s.symbol order by s.window_start), 0) - 1) * 100)::numeric, 4) as change_vs_prev_pct
from {{ ref('stg_price_windows') }} s
join {{ ref('dim_symbol') }} d on d.symbol = s.symbol
