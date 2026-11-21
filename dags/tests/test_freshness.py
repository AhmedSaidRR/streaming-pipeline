"""Unit tests for the freshness helpers used by the Airflow DAG."""

from datetime import datetime, timedelta

import pytest

from freshness import (
    DEFAULT_MAX_LAG_SECONDS,
    MAX_LAG_ENV_VAR,
    assert_fresh,
    configured_max_lag_seconds,
    ingestion_lag_seconds,
)

NOW = datetime(2026, 11, 21, 12, 0, 0)


def test_default_threshold_is_five_minutes():
    assert configured_max_lag_seconds({}) == DEFAULT_MAX_LAG_SECONDS == 300


def test_threshold_comes_from_environment():
    assert configured_max_lag_seconds({MAX_LAG_ENV_VAR: "1"}) == 1


def test_ingestion_lag_seconds():
    assert ingestion_lag_seconds(NOW - timedelta(seconds=42), NOW) == 42.0


def test_fresh_passes_within_threshold():
    assert assert_fresh(NOW - timedelta(seconds=35), NOW, 300) == 35.0


def test_stale_fails_over_threshold():
    with pytest.raises(ValueError, match="Stream is stale"):
        assert_fresh(NOW - timedelta(seconds=301), NOW, 300)


def test_empty_table_fails():
    with pytest.raises(ValueError, match="raw.price_windows is empty"):
        assert_fresh(None, NOW, 300)
