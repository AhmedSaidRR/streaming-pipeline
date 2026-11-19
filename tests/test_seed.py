import csv
from pathlib import Path

SEED = Path(__file__).resolve().parents[1] / "dbt" / "seeds" / "symbol_metadata.csv"


def _rows():
    with SEED.open() as f:
        return list(csv.DictReader(f))


def test_seed_columns():
    assert set(_rows()[0]) == {"symbol", "display_name", "asset_class", "is_active"}


def test_seed_symbols_unique_and_uppercase():
    symbols = [r["symbol"] for r in _rows()]
    assert len(symbols) == len(set(symbols))
    assert all(s == s.upper() for s in symbols)


def test_seed_is_active_is_boolean():
    assert all(r["is_active"] in ("true", "false") for r in _rows())
