{{ config(materialized='view') }}

-- Ingestion health, computed at query time (a view, not a stale table).
-- "Commit lag" is how long Spark took to write a closed 1-minute window:
--   ingested_at - window_end (expected ~30 s: watermark + trigger + JDBC).
-- "Ingestion lag" is how long ago the newest row landed: now() - ingested_at.
-- Dead-letter count is intentionally omitted: dead letters are Parquet objects in the
-- S3 lake, not a SQL relation, so counting them from dbt would require an S3 scan.
with w as (
    select ingested_at, window_end
    from {{ ref('stg_price_windows') }}
),
stats as (
    select
        max(ingested_at) as latest_ingested_at,
        count(*) filter (
            where ingested_at > (now() at time zone 'utc') - interval '24 hours'
        ) as windows_last_24h,
        avg(extract(epoch from (ingested_at - window_end))) as avg_commit_lag_seconds,
        max(extract(epoch from (ingested_at - window_end))) as max_commit_lag_seconds
    from w
)
select
    latest_ingested_at,
    round(extract(epoch from ((now() at time zone 'utc') - latest_ingested_at))::numeric, 1)
        as ingestion_lag_seconds,
    windows_last_24h,
    round((windows_last_24h::numeric / 24.0), 2) as windows_per_hour_last_24h,
    round(avg_commit_lag_seconds::numeric, 1) as avg_commit_lag_seconds,
    round(max_commit_lag_seconds::numeric, 1) as max_commit_lag_seconds
from stats
