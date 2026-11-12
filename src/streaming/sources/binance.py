"""Binance public trade streams (combined stream endpoint).

Docs: https://developers.binance.com/docs/binance-spot-api-docs/web-socket-streams
Example message on /stream?streams=btcusdt@trade:
  {"stream":"btcusdt@trade","data":{"e":"trade","E":1672515782136,"s":"BTCUSDT","t":12345,
   "p":"0.001","q":"100","T":1672515782136,"m":true,"M":true}}
"m" = buyer is the market maker -> the taker SOLD.
USDT pairs are mapped to the canonical -USD symbol so they can be compared with Coinbase.
"""
from __future__ import annotations

import json

import time
import urllib.error

from ..schema import make_trade
from .http import get_json


class BinanceSource:
    name = "binance"

    def __init__(self, url: str, symbols: list[str]):
        self.symbols = symbols
        # BTC-USD -> BTCUSDT
        self.to_native = {s: s.split("-")[0] + "USDT" for s in symbols}
        self.to_canonical = {v: k for k, v in self.to_native.items()}
        streams = "/".join(f"{n.lower()}@trade" for n in self.to_native.values())
        self.url = f"{url}?streams={streams}"

    def subscribe_messages(self) -> list[str]:
        return []  # subscription is in the URL

    def parse(self, raw: str, ingest_ts_ms: int) -> list[dict]:
        msg = json.loads(raw)
        d = msg.get("data", msg)
        if d.get("e") != "trade":
            return []
        symbol = self.to_canonical.get(d["s"])
        if symbol is None:
            return []
        return [make_trade(
            exchange=self.name,
            symbol=symbol,
            trade_id=d["t"],
            price=d["p"],
            size=d["q"],
            side="sell" if d["m"] else "buy",
            event_ts_ms=d["T"],
            ingest_ts_ms=ingest_ts_ms,
        )]

    # ---- crash recovery: fetch trades we missed while disconnected ------------------------
    def backfill(self, rest_url: str, symbol: str, last_id: int, min_event_ms: int,
                 ingest_ts_ms: int, max_pages: int = 10) -> list[dict]:
        """All trades with id > last_id. Uses GET /api/v3/historicalTrades?fromId= (paged,
        1000 per call); if that endpoint is not available it falls back to the most recent
        1000 trades from GET /api/v3/trades."""
        native = self.to_native[symbol]
        out: list[dict] = []

        def conv(r):
            return make_trade(self.name, symbol, r["id"], r["price"], r["qty"],
                              "sell" if r["isBuyerMaker"] else "buy", r["time"], ingest_ts_ms)
        try:
            from_id = last_id + 1
            for _ in range(max_pages):
                rows, _h = get_json(f"{rest_url}/api/v3/historicalTrades?symbol={native}&fromId={from_id}&limit=1000")
                if not rows:
                    break
                out.extend(conv(r) for r in rows if r["id"] > last_id)
                if len(rows) < 1000:
                    break
                from_id = rows[-1]["id"] + 1
                time.sleep(0.15)
        except urllib.error.HTTPError:
            rows, _h = get_json(f"{rest_url}/api/v3/trades?symbol={native}&limit=1000")
            out = [conv(r) for r in rows if r["id"] > last_id]
        return [t for t in out if t["event_ts_ms"] >= min_event_ms]
