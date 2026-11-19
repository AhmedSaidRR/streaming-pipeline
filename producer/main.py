"""Kafka producer: polls a price source and publishes one JSON tick per symbol."""
import json
import logging
import os
import signal
import time

from producer.messages import build_tick, validate_tick
from producer.sources import BinanceSource, PriceSource, SimulatedSource, make_symbols

log = logging.getLogger("producer")


class PriceProducer:
    def __init__(self, source: PriceSource, topic: str, bootstrap: str,
                 interval: float = 1.0, kafka_producer=None):
        self.source = source
        self.topic = topic
        self.bootstrap = bootstrap
        self.interval = interval
        self._producer = kafka_producer
        self._stop = False
        self.sent = 0
        self.rejected = 0

    def _kafka(self):
        if self._producer is None:
            from confluent_kafka import Producer  # imported lazily so unit tests need no Kafka lib
            self._producer = Producer({
                "bootstrap.servers": self.bootstrap,
                "enable.idempotence": True,
                "acks": "all",
                "linger.ms": 50,
            })
        return self._producer

    @staticmethod
    def _on_delivery(err, msg):
        if err is not None:
            log.error("delivery failed: %s", err)

    def poll_once(self) -> int:
        prices = self.source.fetch()
        kafka = self._kafka()
        count = 0
        for symbol, price in prices.items():
            tick = build_tick(symbol, price, self.source.name)
            problems = validate_tick(tick)
            if problems:
                self.rejected += 1
                log.warning("dropping %s: %s", symbol, problems)
                continue
            kafka.produce(self.topic, key=symbol, value=json.dumps(tick), callback=self._on_delivery)
            kafka.poll(0)
            count += 1
        self.sent += count
        return count

    def stop(self, *_):
        self._stop = True

    def run(self):
        signal.signal(signal.SIGINT, self.stop)
        signal.signal(signal.SIGTERM, self.stop)
        backoff = 1
        while not self._stop:
            started = time.monotonic()
            try:
                n = self.poll_once()
                backoff = 1
                if self.sent % 100 < n:
                    log.info("sent=%d rejected=%d", self.sent, self.rejected)
            except Exception as exc:  # network / API errors: retry with exponential backoff
                log.error("poll failed (%s); retrying in %ss", exc, backoff)
                time.sleep(backoff)
                backoff = min(backoff * 2, 30)
                continue
            time.sleep(max(0.0, self.interval - (time.monotonic() - started)))
        log.info("flushing and shutting down")
        self._kafka().flush(10)


def build_source() -> PriceSource:
    base = [s.strip().upper() for s in os.getenv("SYMBOLS", "BTCUSDT,ETHUSDT").split(",") if s.strip()]
    kind = os.getenv("SOURCE", "binance").lower()
    if kind == "binance":
        return BinanceSource(base)
    symbols = make_symbols(base, int(os.getenv("SIM_EXTRA_SYMBOLS", "0")))
    return SimulatedSource(symbols)


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s - %(message)s")
    PriceProducer(
        source=build_source(),
        topic=os.getenv("KAFKA_TOPIC", "crypto.prices"),
        bootstrap=os.getenv("KAFKA_BOOTSTRAP", "kafka:9092"),
        interval=float(os.getenv("POLL_INTERVAL_SEC", "1")),
    ).run()


if __name__ == "__main__":
    main()
