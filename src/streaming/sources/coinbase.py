"""Coinbase Exchange public WebSocket feed — "matches" channel (every executed trade).

Docs: https://docs.cdp.coinbase.com/exchange/docs/websocket-channels
Example message:
  {"type":"match","trade_id":10,"sequence":50,"maker_order_id":"..","taker_order_id":"..",
   "time":"2014-11-07T08:19:27.028459Z","product_id":"BTC-USD","size":"5.23512",
   "price":"400.23","side":"sell"}
Note: Coinbase's "side" is the MAKER side, so the taker (aggressor) side is the opposite.
"""
from __future__ import annotations

import json
from datetime import datetime

import time

from ..schema import make_trade
from .http import get_json


class CoinbaseSource:
    name = "coinbase"

    def __init__(self, url: str, symbols: list[str]):
        self.symbols = symbols
        self.url = url

    def subscribe_messages(self) -> list[str]:
        return [json.dumps({
            "type": "subscribe",
            "product_ids": self.symbols,
            "channels": ["matches", "heartbeat"],
        })]

    @staticmethod
    def _ts_ms(iso: str) -> int:
        # Python 3.11+ understands the trailing Z; trim nanoseconds if present.
        if "." in iso:
            head, frac = iso.rstrip("Z").split(".", 1)
            iso = f"{head}.{frac[:6]}+00:00"
        else:
            iso = iso.replace("Z", "+00:00")
        return int(datetime.fromisoformat(iso).timestamp() * 1000)

    def parse(self, raw: str, ingest_ts_ms: int) -> list[dict]:
        msg = json.loads(raw)
        typ = msg.get("type")
        if typ == "error":
            raise RuntimeError(f"coinbase error: {msg}")
        # "last_match" is a replay of the previous trade sent on subscribe — skip it.
        if typ != "match":
            return []
        maker_side = msg["side"]
        taker_side = "buy" if maker_side == "sell" else "sell"
        return [make_trade(
            exchange=self.name,
            symbol=msg["product_id"],
            trade_id=msg["trade_id"],
            price=msg["price"],
            size=msg["size"],
            side=taker_side,
            event_ts_ms=self._ts_ms(msg["time"]),
            ingest_ts_ms=ingest_ts_ms,
        )]

    # ---- crash recovery: fetch trades we missed while disconnected ------------------------
    def backfill(self, rest_url: str, symbol: str, last_id: int, min_event_ms: int,
                 ingest_ts_ms: int, max_pages: int = 10) -> list[dict]:
        """All trades with trade_id > last_id via GET /products/{id}/trades (newest first,
        1000 per page, older pages via the `after` cursor). Stops at last_id, at min_event_ms
        or after max_pages."""
        out, after = [], None
        for _ in range(max_pages):
            url = f"{rest_url}/products/{symbol}/trades?limit=1000" + (f"&after={after}" if after else "")
            rows, headers = get_json(url)
            if not rows:
                break
            reached = False
            for r in rows:
                tid = int(r["trade_id"])
                ev = self._ts_ms(r["time"])
                if tid <= last_id or ev < min_event_ms:
                    reached = True
                    continue
                taker = "buy" if r["side"] == "sell" else "sell"   # REST side is the maker side too
                out.append(make_trade(self.name, symbol, tid, r["price"], r["size"], taker, ev, ingest_ts_ms))
            after = headers.get("CB-AFTER")
            if reached or not after:
                break
            time.sleep(0.15)  # stay well under the public rate limit
        out.sort(key=lambda t: int(t["trade_id"]))
        return out
