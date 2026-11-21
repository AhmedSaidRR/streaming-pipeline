"""Dashboard SQL, importable without starting Streamlit (used by verification too)."""

PIPELINE_METRICS_SQL = """
    select latest_ingested_at, ingestion_lag_seconds, windows_last_24h,
           windows_per_hour_last_24h, avg_commit_lag_seconds, max_commit_lag_seconds
    from marts.pipeline_metrics
"""

PIPELINE_WINDOWS_PER_HOUR_SQL = """
    select hour_start, windows
    from marts.pipeline_windows_per_hour
    order by hour_start
"""
