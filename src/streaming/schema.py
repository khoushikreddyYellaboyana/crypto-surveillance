"""Canonical trade schema shared by every exchange adapter and by the processor.

Every message on ``market.trades.raw`` is a JSON object with exactly these fields:

    exchange      str    "coinbase" | "binance"
    symbol        str    canonical symbol, e.g. "BTC-USD" (Binance USDT pairs are mapped to -USD)
    trade_id      str    exchange trade id (unique per exchange + symbol)
    price         float  > 0
    size          float  > 0, base-asset quantity
    side          str    taker side: "buy" | "sell"
    event_ts_ms   int    time the trade happened on the exchange (event time)
    ingest_ts_ms  int    time our producer received it (ingestion time)
"""
from __future__ import annotations

import json
import time

REQUIRED = {
    "exchange": str,
    "symbol": str,
    "trade_id": str,
    "price": float,
    "size": float,
    "side": str,
    "event_ts_ms": int,
    "ingest_ts_ms": int,
}


class InvalidTrade(ValueError):
    pass


def now_ms() -> int:
    return int(time.time() * 1000)


def make_trade(exchange, symbol, trade_id, price, size, side, event_ts_ms, ingest_ts_ms=None) -> dict:
    return validate({
        "exchange": exchange,
        "symbol": symbol,
        "trade_id": str(trade_id),
        "price": float(price),
        "size": float(size),
        "side": side,
        "event_ts_ms": int(event_ts_ms),
        "ingest_ts_ms": int(ingest_ts_ms if ingest_ts_ms is not None else now_ms()),
    })


def validate(t: dict) -> dict:
    for field, typ in REQUIRED.items():
        if field not in t:
            raise InvalidTrade(f"missing field {field}")
        v = t[field]
        if typ is float and isinstance(v, int) and not isinstance(v, bool):
            t[field] = v = float(v)
        if not isinstance(v, typ) or isinstance(v, bool):
            raise InvalidTrade(f"bad type for {field}: {type(v).__name__}")
    if t["price"] <= 0 or t["size"] <= 0:
        raise InvalidTrade("price and size must be positive")
    if t["side"] not in ("buy", "sell"):
        raise InvalidTrade(f"bad side {t['side']}")
    # reject obviously broken clocks (more than 1 day off)
    if abs(t["event_ts_ms"] - t["ingest_ts_ms"]) > 86_400_000:
        raise InvalidTrade("event timestamp is more than 1 day from ingest time")
    return t


def encode(obj: dict) -> bytes:
    return json.dumps(obj, separators=(",", ":")).encode()


def decode(raw: bytes) -> dict:
    return json.loads(raw)
