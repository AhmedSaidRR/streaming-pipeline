from producer.main import PriceProducer
from producer.sources import BinanceSource, SimulatedSource, make_symbols


def test_binance_parse():
    payload = [{"symbol": "BTCUSDT", "price": "65000.10"}, {"symbol": "ETHUSDT", "price": "3200.5"}]
    assert BinanceSource.parse(payload) == {"BTCUSDT": 65000.10, "ETHUSDT": 3200.5}


def test_simulated_is_deterministic_with_seed():
    a = SimulatedSource(["BTCUSDT"], seed=1)
    b = SimulatedSource(["BTCUSDT"], seed=1)
    assert [a.fetch()["BTCUSDT"] for _ in range(5)] == [b.fetch()["BTCUSDT"] for _ in range(5)]


def test_simulated_prices_stay_positive():
    s = SimulatedSource(["XRPUSDT"], seed=3, spike_prob=1.0)
    assert all(s.fetch()["XRPUSDT"] > 0 for _ in range(1000))


def test_make_symbols():
    assert make_symbols(["BTCUSDT"], 2) == ["BTCUSDT", "SIM0000USDT", "SIM0001USDT"]


class FakeKafka:
    def __init__(self):
        self.messages = []

    def produce(self, topic, key, value, callback=None):
        self.messages.append((topic, key, value))

    def poll(self, _):
        pass

    def flush(self, _):
        pass


class BadSource:
    name = "bad"

    def fetch(self):
        return {"GOOD": 10.0, "BAD": -1.0}


def test_producer_sends_valid_and_drops_invalid():
    kafka = FakeKafka()
    p = PriceProducer(BadSource(), "t", "x", kafka_producer=kafka)
    assert p.poll_once() == 1
    assert p.rejected == 1
    assert kafka.messages[0][1] == "GOOD"
