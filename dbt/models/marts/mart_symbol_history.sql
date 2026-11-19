-- Full SCD Type 2 history: one row per version of a symbol's attributes.
select
    symbol,
    display_name,
    asset_class,
    is_active,
    dbt_valid_from as valid_from,
    dbt_valid_to   as valid_to,
    dbt_valid_to is null as is_current
from {{ ref('symbol_metadata_snapshot') }}
order by symbol, dbt_valid_from
