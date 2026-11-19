"""Message contract for the price ticks sent to Kafka."""
from datetime import datetime, timezone


def build_tick(symbol: str, price: float, source: str, now: datetime | None = None) -> dict:
    now = now or datetime.now(timezone.utc)
    return {
        "symbol": symbol.upper(),
        "price": float(price),
        "event_time": now.isoformat(),
        "source": source,
    }


def validate_tick(tick: dict) -> list[str]:
    """Return a list of problems; an empty list means the tick is valid."""
    errors = []
    if not tick.get("symbol"):
        errors.append("missing symbol")
    price = tick.get("price")
    if not isinstance(price, (int, float)) or isinstance(price, bool):
        errors.append("price is not a number")
    elif price <= 0:
        errors.append("price must be > 0")
    if not tick.get("event_time"):
        errors.append("missing event_time")
    return errors
