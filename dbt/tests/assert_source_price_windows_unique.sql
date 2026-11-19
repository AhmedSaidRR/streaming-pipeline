-- Spark writes raw.price_windows at-least-once: after a restart the same
-- (symbol, window_start) can legitimately appear twice. stg_price_windows
-- deduplicates, so testing uniqueness there can never fail -- test the
-- append-only source instead and report duplicates as a warning.
--
-- The relation is a var so the exact same test can be pointed at a scratch
-- copy for the proof, without inserting into or deleting from real rows.
{{ config(severity='warn') }}

select
    upper(symbol) as symbol,
    window_start,
    count(*) as duplicate_count
from {{ var('price_windows_uniqueness_relation', source('raw', 'price_windows')) }}
group by 1, 2
having count(*) > 1
