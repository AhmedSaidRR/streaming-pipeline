"""Pure helpers for the Airflow freshness check (no Airflow or database dependency).

`ingested_at` is the time Spark committed a window to Postgres. Measuring lag from it
rather than from `window_end` avoids counting the deliberate 1-minute watermark plus
trigger delay as "staleness".
"""

import os

DEFAULT_MAX_LAG_SECONDS = 300  # 5 minutes
MAX_LAG_ENV_VAR = "FRESHNESS_MAX_LAG_SECONDS"


def configured_max_lag_seconds(environ=None) -> int:
    """Return the ingestion-lag threshold in seconds (env var, default 5 minutes)."""
    env = os.environ if environ is None else environ
    return int(env.get(MAX_LAG_ENV_VAR, DEFAULT_MAX_LAG_SECONDS))


def ingestion_lag_seconds(latest, now) -> float:
    """Seconds between the newest ingestion timestamp and `now` (naive UTC)."""
    return (now - latest).total_seconds()


def assert_fresh(latest, now, max_lag_seconds: int) -> float:
    """Raise ValueError when the newest ingestion is older than the threshold."""
    if latest is None:
        raise ValueError("raw.price_windows is empty: is the streaming job running?")
    lag = ingestion_lag_seconds(latest, now)
    if lag > max_lag_seconds:
        raise ValueError(
            f"Stream is stale: newest ingested_at was {lag:.0f}s ago "
            f"(limit {max_lag_seconds}s)"
        )
    return lag
