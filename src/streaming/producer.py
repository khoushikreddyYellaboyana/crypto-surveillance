"""Producer: live exchange WebSockets -> Kafka topic ``market.trades.raw``.

Reliability features
  * idempotent producer (enable.idempotence=true, acks=all) -> no duplicates from retries
  * automatic reconnect with exponential backoff + jitter per exchange
  * malformed exchange messages go to the dead-letter topic instead of crashing the producer
  * trade-id gap detection -> counts trades the exchange feed skipped (data completeness)
  * CRASH RECOVERY: the last trade id per exchange/symbol is checkpointed to disk every 5 s.
    After a restart (or any reconnect) the producer first fetches every trade it missed from
    the exchange's public REST API ("backfill"), then continues with the live stream
  * DUPLICATE PROTECTION: a bounded cache of recently sent trade ids drops trades that would be
    sent twice (backfill overlapping the live stream, exchange re-sends after reconnect)
Scalability
  * key = symbol, so all trades of one symbol (from every exchange) land in the same
    partition -> per-symbol ordering and stateful processing can scale out by partition.

Run:  python -m streaming.producer            (sources/symbols from env or flags)
      python -m streaming.producer --record data/recording.jsonl   (also save to a file)
"""
from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import random
import signal
import socket
import time
from collections import OrderedDict

import websockets
from confluent_kafka import Producer

from . import config
from .schema import InvalidTrade, encode, now_ms
from .sources import REGISTRY

log = logging.getLogger("producer")


class Stats:
    def __init__(self):
        self.reset()
        self.reconnects = 0
        self.gaps = 0           # trades skipped by the exchange feed (trade-id holes)
        self.out_of_order = 0   # trade id <= last seen id
        self.delivery_errors = 0
        self.backfilled = 0     # missed trades recovered from the REST API after a restart/reconnect
        self.skipped_dupes = 0  # trades NOT sent because they were already sent

    def reset(self):
        self.sent = 0
        self.invalid = 0
        self.t0 = time.time()


class TradeProducer:
    def __init__(self, record_path: str | None = None):
        self.p = Producer({
            "bootstrap.servers": config.KAFKA_BOOTSTRAP,
            "client.id": f"producer-{socket.gethostname()}",
            "enable.idempotence": True,
            "acks": "all",
            "compression.type": "lz4",
            "linger.ms": 20,
            "batch.num.messages": 10000,
            "message.timeout.ms": 120000,
        })
        self.stats = Stats()
        self.started_ms = now_ms()
        self.last_id: dict[tuple[str, str], int] = {}
        self.last_ts: dict[tuple[str, str], int] = {}
        self.acked: dict[tuple[str, str], tuple[int, int]] = {}  # only trades Kafka confirmed
        self.recent: OrderedDict[tuple[str, str, str], None] = OrderedDict()
        self.record = open(record_path, "a", encoding="utf-8") if record_path else None
        self.running = True
        self.resumed_from = self.load_checkpoint()

    # -- checkpoint (crash recovery) -----------------------------------------------
    def load_checkpoint(self) -> int:
        try:
            with open(config.PRODUCER_STATE_PATH, encoding="utf-8") as f:
                st = json.load(f)
        except (OSError, ValueError):
            log.info("no checkpoint found – starting fresh")
            return 0
        for k, v in st.get("last", {}).items():
            exch, sym = k.split("|", 1)
            self.last_id[(exch, sym)] = int(v["id"])
            self.last_ts[(exch, sym)] = int(v["ts"])
            self.acked[(exch, sym)] = (int(v["id"]), int(v["ts"]))
        age = (now_ms() - st.get("saved_ms", now_ms())) / 1000
        log.info("RECOVERY: loaded checkpoint of %d series, saved %.0fs ago – missed trades will be backfilled",
                 len(self.last_id), age)
        return st.get("saved_ms", 0)

    def save_checkpoint(self):
        path = config.PRODUCER_STATE_PATH
        os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
        tmp = f"{path}.{socket.gethostname()}.tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            # checkpoint = last trade Kafka has ACKNOWLEDGED (acks=all), not merely queued: if we crash
            # with un-delivered messages in memory, those trades are re-fetched by the backfill
            json.dump({"saved_ms": now_ms(), "last": {f"{e}|{s}": {"id": i, "ts": ts}
                                                      for (e, s), (i, ts) in self.acked.items()}}, f)
        os.replace(tmp, path)  # atomic: a crash while writing never leaves a broken checkpoint

    # -- kafka ---------------------------------------------------------------
    def _on_delivery(self, err, msg, key=None, tid=None, ev=None):
        if err is not None:
            self.stats.delivery_errors += 1
            log.error("delivery failed: %s", err)
        elif key is not None and tid is not None and tid > self.acked.get(key, (-1, 0))[0]:
            self.acked[key] = (tid, ev)

    def send_trade(self, t: dict, backfill: bool = False):
        k3 = (t["exchange"], t["symbol"], t["trade_id"])
        if k3 in self.recent:            # already sent -> never send the same trade twice
            self.stats.skipped_dupes += 1
            return
        self.recent[k3] = None
        if len(self.recent) > 100_000:
            self.recent.popitem(last=False)
        key = (t["exchange"], t["symbol"])
        tid = None
        try:
            tid = int(t["trade_id"])
            last = self.last_id.get(key)
            if last is not None and not backfill:
                if tid > last + 1:
                    self.stats.gaps += tid - last - 1
                elif tid <= last:
                    self.stats.out_of_order += 1
            if last is None or tid > last:
                self.last_id[key] = tid
                self.last_ts[key] = t["event_ts_ms"]
        except ValueError:
            pass
        if backfill:
            t["backfill"] = True         # lets the processor exclude it from live-latency figures
            self.stats.backfilled += 1
        ev = t["event_ts_ms"]
        self.p.produce(config.TOPIC_RAW, key=t["symbol"], value=encode(t),
                       on_delivery=lambda err, msg: self._on_delivery(err, msg, key, tid, ev))
        self.p.poll(0)
        self.stats.sent += 1
        if self.record:
            self.record.write(json.dumps(t) + "\n")

    async def backfill(self, source):
        """Fetch the trades we missed while the producer was down / disconnected."""
        if not config.BACKFILL:
            return
        url = config.COINBASE_REST_URL if source.name == "coinbase" else config.BINANCE_REST_URL
        min_ev = now_ms() - config.BACKFILL_MAX_AGE_MS
        for sym in source.symbols:
            last = self.last_id.get((source.name, sym))
            if last is None:
                continue
            try:
                trades = await asyncio.to_thread(source.backfill, url, sym, last, min_ev, now_ms())
            except Exception as e:  # REST problems must never stop the live stream
                log.warning("[%s] backfill %s failed: %r", source.name, sym, e)
                continue
            for t in trades:
                self.send_trade(t, backfill=True)
            if trades:
                log.info("RECOVERY [%s] %s: backfilled %d missed trades (ids %s..%s)", source.name, sym,
                         len(trades), trades[0]["trade_id"], trades[-1]["trade_id"])

    def send_dlq(self, source: str, raw: str, error: str):
        self.stats.invalid += 1
        self.p.produce(config.TOPIC_DLQ, key=source, value=encode({
            "source": source, "error": error, "raw": raw[:2000], "ts_ms": now_ms()}))
        self.p.poll(0)

    # -- websockets ------------------------------------------------------------
    async def run_source(self, source):
        backoff = 1.0
        while self.running:
            try:
                log.info("[%s] connecting to %s", source.name, source.url)
                async with websockets.connect(source.url, ping_interval=20, ping_timeout=20,
                                              max_size=2**22, open_timeout=15, max_queue=None) as ws:
                    for m in source.subscribe_messages():
                        await ws.send(m)
                    log.info("[%s] connected, streaming %s", source.name, ",".join(source.symbols))
                    # live messages are buffered by the websocket while we backfill the gap first,
                    # so the order is: missed trades -> live trades, with overlaps removed
                    await self.backfill(source)
                    backoff = 1.0
                    async for raw in ws:
                        ingest = now_ms()
                        try:
                            for t in source.parse(raw, ingest):
                                self.send_trade(t)
                        except (InvalidTrade, KeyError, ValueError, TypeError) as e:
                            self.send_dlq(source.name, raw if isinstance(raw, str) else raw.decode(), repr(e))
                        if not self.running:
                            break
            except asyncio.CancelledError:
                raise
            except Exception as e:  # network errors, server closes, exchange errors
                if not self.running:
                    break
                self.stats.reconnects += 1
                wait = min(backoff, 60) * (0.5 + random.random())
                log.warning("[%s] connection lost (%r); reconnecting in %.1fs", source.name, e, wait)
                await asyncio.sleep(wait)
                backoff *= 2

    async def report(self):
        while self.running:
            await asyncio.sleep(config.METRICS_INTERVAL_S)
            s = self.stats
            dt = time.time() - s.t0
            m = {
                "component": "producer",
                "instance": socket.gethostname(),
                "ts_ms": now_ms(),
                "msgs_per_s": round(s.sent / dt, 2),
                "msgs": s.sent,
                "invalid": s.invalid,
                "feed_gaps_total": s.gaps,
                "out_of_order_total": s.out_of_order,
                "reconnects_total": s.reconnects,
                "delivery_errors_total": s.delivery_errors,
                "backfilled_total": s.backfilled,
                "skipped_duplicates_total": s.skipped_dupes,
                "started_ms": self.started_ms,
                "resumed_from_checkpoint_ms": self.resumed_from,
                "queue_len": len(self.p),
            }
            self.p.produce(config.TOPIC_METRICS, key="producer", value=encode(m))
            self.p.poll(0)
            log.info("sent=%d (%.1f/s) invalid=%d gaps=%d backfilled=%d dup_skipped=%d reconnects=%d queue=%d",
                     s.sent, m["msgs_per_s"], s.invalid, s.gaps, s.backfilled, s.skipped_dupes,
                     s.reconnects, len(self.p))
            s.reset()
            try:
                self.save_checkpoint()
            except OSError as e:
                log.warning("checkpoint not saved: %s", e)
            if self.record:
                self.record.flush()

    async def main(self, sources: list[str], symbols: list[str]):
        loop = asyncio.get_running_loop()
        tasks = []

        def stop():
            log.info("shutting down…")
            self.running = False
            for t in tasks:
                t.cancel()

        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                loop.add_signal_handler(sig, stop)
            except NotImplementedError:  # Windows
                pass

        for name in sources:
            cls = REGISTRY[name]
            url = config.COINBASE_WS_URL if name == "coinbase" else config.BINANCE_WS_URL
            tasks.append(asyncio.create_task(self.run_source(cls(url, symbols))))
        tasks.append(asyncio.create_task(self.report()))
        try:
            await asyncio.gather(*tasks)
        except asyncio.CancelledError:
            pass
        finally:
            remaining = self.p.flush(15)
            try:
                self.save_checkpoint()
            except OSError:
                pass
            log.info("flushed producer (%d messages not delivered)", remaining)
            if self.record:
                self.record.close()


def cli():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--sources", default=",".join(config.SOURCES), help="coinbase,binance")
    ap.add_argument("--symbols", default=",".join(config.SYMBOLS), help="BTC-USD,ETH-USD,…")
    ap.add_argument("--record", default=None, help="also append every trade to this JSONL file")
    a = ap.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")
    srcs = [s.strip() for s in a.sources.split(",") if s.strip()]
    syms = [s.strip() for s in a.symbols.split(",") if s.strip()]
    try:
        asyncio.run(TradeProducer(a.record).main(srcs, syms))
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    cli()
