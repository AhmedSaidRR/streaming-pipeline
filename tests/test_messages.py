from datetime import datetime, timezone

from producer.messages import build_tick, validate_tick


def test_build_tick_shape():
    t = build_tick("btcusdt", 100.5, "binance", now=datetime(2026, 11, 19, tzinfo=timezone.utc))
    assert t == {"symbol": "BTCUSDT", "price": 100.5,
                 "event_time": "2026-11-19T00:00:00+00:00", "source": "binance"}


def test_valid_tick_has_no_errors():
    assert validate_tick(build_tick("ETHUSDT", 3000, "x")) == []


def test_rejects_non_positive_price():
    assert "price must be > 0" in validate_tick({"symbol": "A", "price": 0, "event_time": "x"})


def test_rejects_missing_fields_and_bad_types():
    errs = validate_tick({"price": "12"})
    assert "missing symbol" in errs and "price is not a number" in errs and "missing event_time" in errs
