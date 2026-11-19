"""Price sources. Each source returns {symbol: price} for the current moment."""
import json
import random
from abc import ABC, abstractmethod

import requests

BASE_PRICES = {"BTCUSDT": 65000.0, "ETHUSDT": 3200.0, "BNBUSDT": 600.0,
               "SOLUSDT": 150.0, "XRPUSDT": 0.55}


class PriceSource(ABC):
    name = "base"

    @abstractmethod
    def fetch(self) -> dict[str, float]:
        ...


class BinanceSource(PriceSource):
    """Binance public REST API (no API key). One request returns all symbols."""
    name = "binance"
    URL = "https://api.binance.com/api/v3/ticker/price"

    def __init__(self, symbols: list[str], session: requests.Session | None = None, timeout: int = 5):
        self.symbols = symbols
        self.session = session or requests.Session()
        self.timeout = timeout

    @staticmethod
    def parse(payload: list[dict]) -> dict[str, float]:
        return {item["symbol"]: float(item["price"]) for item in payload}

    def fetch(self) -> dict[str, float]:
        params = {"symbols": json.dumps(self.symbols, separators=(",", ":"))}
        resp = self.session.get(self.URL, params=params, timeout=self.timeout)
        resp.raise_for_status()
        return self.parse(resp.json())


class SimulatedSource(PriceSource):
    """Random-walk prices with rare spikes. Works offline and scales to many symbols."""
    name = "simulated"

    def __init__(self, symbols: list[str], seed: int | None = None,
                 spike_prob: float = 0.02, spike_size: float = 0.004):
        self.rng = random.Random(seed)
        self.spike_prob = spike_prob
        self.spike_size = spike_size
        self.prices = {s: BASE_PRICES.get(s, self.rng.uniform(1, 500)) for s in symbols}

    def fetch(self) -> dict[str, float]:
        for sym, price in self.prices.items():
            ret = self.rng.gauss(0, 0.0002)
            if self.rng.random() < self.spike_prob:
                ret += self.rng.choice([-1, 1]) * self.spike_size
            self.prices[sym] = max(price * (1 + ret), 1e-6)
        return dict(self.prices)


def make_symbols(base: list[str], extra_synthetic: int = 0) -> list[str]:
    return base + [f"SIM{i:04d}USDT" for i in range(extra_synthetic)]
