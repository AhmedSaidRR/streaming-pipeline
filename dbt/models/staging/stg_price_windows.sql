-- Spark delivers at-least-once, so the same (symbol, window_start) can appear twice
-- after a restart. Keep the most recently ingested row.
with ranked as (
    select
        upper(symbol) as symbol,
        window_start,
        window_end,
        avg_price,
        min_price,
        max_price,
        tick_count,
        ingested_at,
        row_number() over (
            partition by upper(symbol), window_start
            order by ingested_at desc
        ) as rn
    from {{ source('raw', 'price_windows') }}
    where avg_price > 0
)

select
    symbol,
    window_start,
    window_end,
    cast(avg_price as numeric(20, 8)) as avg_price,
    cast(min_price as numeric(20, 8)) as min_price,
    cast(max_price as numeric(20, 8)) as max_price,
    tick_count,
    ingested_at
from ranked
where rn = 1
