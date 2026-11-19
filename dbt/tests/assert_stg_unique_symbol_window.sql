select symbol, window_start, count(*) as n
from {{ ref('stg_price_windows') }}
group by symbol, window_start
having count(*) > 1
