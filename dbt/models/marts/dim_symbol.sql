-- Current view of each symbol. History lives in mart_symbol_history (SCD Type 2).
with symbols as (
    select distinct symbol from {{ ref('stg_price_windows') }}
),
current_meta as (
    select * from {{ ref('symbol_metadata_snapshot') }}
    where dbt_valid_to is null
)
select
    dense_rank() over (order by s.symbol) as symbol_key,
    s.symbol,
    case when s.symbol like '%USDT' then replace(s.symbol, 'USDT', '') else s.symbol end as base_asset,
    case when s.symbol like '%USDT' then 'USDT' else 'UNKNOWN' end as quote_asset,
    coalesce(m.display_name, s.symbol) as display_name,
    coalesce(m.asset_class, 'Unknown') as asset_class,
    coalesce(m.is_active, true) as is_active
from symbols s
left join current_meta m on m.symbol = s.symbol
