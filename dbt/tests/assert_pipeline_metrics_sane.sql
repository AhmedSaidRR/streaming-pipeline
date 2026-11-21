-- Metric invariants: lag cannot be negative, and the worst commit lag cannot be
-- smaller than the average. Also asserts windows were ingested in the last 24 h.
select
    latest_ingested_at,
    ingestion_lag_seconds,
    avg_commit_lag_seconds,
    max_commit_lag_seconds,
    windows_last_24h
from {{ ref('pipeline_metrics') }}
where ingestion_lag_seconds < 0
   or avg_commit_lag_seconds < 0
   or max_commit_lag_seconds < avg_commit_lag_seconds
   or windows_last_24h <= 0
