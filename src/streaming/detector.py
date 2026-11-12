"""Stateful stream-processing logic (pure Python, no Kafka) so it can be unit-tested.

Business objective: real-time crypto MARKET SURVEILLANCE / anomaly detection.
For every symbol the engine keeps per-exchange state and raises these alerts:

  WHALE_TRADE          single trade whose USD notional is above an absolute floor AND far
                       above the rolling median trade size (possible large player / manipulation)
  PRICE_SPIKE          5-second bar return whose z-score vs. an EWMA of past returns is extreme
                       (flash crash / pump)
  VOLUME_BURST         trades-per-bar far above normal (bot activity, news, wash trading)
  EXCHANGE_DIVERGENCE  the same asset trades at materially different prices on Coinbase and
                       Binance at the same moment (arbitrage / broken feed / localized manipulation)

It also produces 5-second OHLCV bars using EVENT TIME with a watermark and allowed lateness,
de-duplicates trades by (exchange, symbol, trade_id) and tracks latency / quality metrics.
"""
from __future__ import annotations

import bisect
import hashlib
import math
from collections import OrderedDict, deque
from dataclasses import dataclass, field

from . import config as default_config


# --------------------------------------------------------------------------- helpers
class Ewma:
    """Exponentially weighted mean / variance (West 1979 incremental form)."""

    def __init__(self, alpha: float = 0.05, min_std: float = 0.0):
        self.alpha = alpha
        self.min_std = min_std  # floor so a perfectly flat history can't make every change infinite/zero
        self.mean: float | None = None
        self.var = 0.0
        self.n = 0

    def z(self, x: float) -> float:
        if self.mean is None:
            return 0.0
        std = max(math.sqrt(self.var), self.min_std)
        return (x - self.mean) / std if std > 0 else 0.0

    def update(self, x: float):
        if self.mean is None:
            self.mean = x
        else:
            diff = x - self.mean
            incr = self.alpha * diff
            self.mean += incr
            self.var = (1 - self.alpha) * (self.var + diff * incr)
        self.n += 1


class RollingMedian:
    """Exact median over the last ``maxlen`` values (sorted list + FIFO)."""

    def __init__(self, maxlen: int = 2000):
        self.fifo: deque[float] = deque()
        self.sorted: list[float] = []
        self.maxlen = maxlen

    def add(self, x: float):
        self.fifo.append(x)
        bisect.insort(self.sorted, x)
        if len(self.fifo) > self.maxlen:
            old = self.fifo.popleft()
            del self.sorted[bisect.bisect_left(self.sorted, old)]

    def __len__(self):
        return len(self.fifo)

    def median(self) -> float:
        s, n = self.sorted, len(self.sorted)
        if n == 0:
            return 0.0
        return s[n // 2] if n % 2 else (s[n // 2 - 1] + s[n // 2]) / 2


def percentile(sorted_vals: list[float], q: float) -> float:
    if not sorted_vals:
        return 0.0
    i = min(len(sorted_vals) - 1, max(0, int(round(q * (len(sorted_vals) - 1)))))
    return sorted_vals[i]


@dataclass
class Bar:
    symbol: str
    exchange: str
    start_ms: int
    end_ms: int
    open: float
    high: float
    low: float
    close: float
    volume: float = 0.0
    notional: float = 0.0
    trades: int = 0
    buy_volume: float = 0.0
    sell_volume: float = 0.0
    last_event_ms: int = 0

    def add(self, t: dict):
        p, q = t["price"], t["size"]
        if t["event_ts_ms"] >= self.last_event_ms:  # close = latest trade by event time
            self.close = p
            self.last_event_ms = t["event_ts_ms"]
        self.high = max(self.high, p)
        self.low = min(self.low, p)
        self.volume += q
        self.notional += p * q
        self.trades += 1
        if t["side"] == "buy":
            self.buy_volume += q
        else:
            self.sell_volume += q

    def to_dict(self) -> dict:
        return {
            "symbol": self.symbol, "exchange": self.exchange,
            "start_ms": self.start_ms, "end_ms": self.end_ms,
            "open": self.open, "high": self.high, "low": self.low, "close": self.close,
            "volume": round(self.volume, 8), "notional": round(self.notional, 2),
            "vwap": round(self.notional / self.volume, 8) if self.volume else None,
            "trades": self.trades,
            "buy_volume": round(self.buy_volume, 8), "sell_volume": round(self.sell_volume, 8),
        }


@dataclass
class SeriesState:
    """Per (symbol, exchange) statistical state."""
    sizes: RollingMedian = field(default_factory=lambda: RollingMedian(2000))
    returns: Ewma = field(default_factory=lambda: Ewma(0.05, min_std=1e-5))   # ≥0.1 bps
    counts: Ewma = field(default_factory=lambda: Ewma(0.05, min_std=2.0))     # ≥2 trades
    prev_close: float | None = None
    last_price: float | None = None
    last_event_ms: int = 0


# --------------------------------------------------------------------------- engine
class SurveillanceEngine:
    def __init__(self, cfg=default_config):
        self.cfg = cfg
        self.bar_ms = cfg.BAR_SECONDS * 1000
        self.state: dict[tuple[str, str], SeriesState] = {}
        self.open_bars: dict[tuple[str, str, int], Bar] = {}
        # bars already emitted are kept for an hour so LATE trades (e.g. backfilled after a producer
        # crash) can still be added: the corrected bar is re-emitted and the sink UPSERTs it
        self.closed_bars: dict[tuple[str, str, int], Bar] = {}
        self.dirty_bars: set[tuple[str, str, int]] = set()
        self._last_evict = 0
        self.seen: OrderedDict[tuple[str, str, str], None] = OrderedDict()
        self.max_event_ms = 0
        self.watermark = 0
        self.cooldown: dict[tuple[str, str, str], int] = {}
        self._reset_metrics()

    # ---- metrics ---------------------------------------------------------
    def _reset_metrics(self):
        self.m_processed = 0
        self.m_duplicates = 0
        self.m_late = 0
        self.m_corrections = 0
        self.m_backfilled = 0
        self.m_bars = 0
        self.m_alerts: dict[str, int] = {}
        self.lat_e2e: list[float] = []      # now - exchange event time
        self.lat_kafka: list[float] = []    # now - producer ingest time (Kafka hop + processing)

    def snapshot_metrics(self, now_ms: int, elapsed_s: float, extra: dict | None = None) -> dict:
        e2e, kfk = sorted(self.lat_e2e), sorted(self.lat_kafka)
        m = {
            "component": "processor",
            "ts_ms": now_ms,
            "msgs_per_s": round(self.m_processed / elapsed_s, 2) if elapsed_s > 0 else 0,
            "msgs": self.m_processed,
            "duplicates": self.m_duplicates,
            "late_events": self.m_late,
            "bar_corrections": self.m_corrections,
            "backfilled_processed": self.m_backfilled,
            "bars": self.m_bars,
            "alerts": dict(self.m_alerts),
            "latency_e2e_ms_p50": round(percentile(e2e, .50), 1),
            "latency_e2e_ms_p95": round(percentile(e2e, .95), 1),
            "latency_e2e_ms_p99": round(percentile(e2e, .99), 1),
            "latency_kafka_ms_p50": round(percentile(kfk, .50), 1),
            "latency_kafka_ms_p95": round(percentile(kfk, .95), 1),
            "latency_kafka_ms_p99": round(percentile(kfk, .99), 1),
            "open_bars": len(self.open_bars),
            "tracked_series": len(self.state),
        }
        if extra:
            m.update(extra)
        self._reset_metrics()
        return m

    # ---- crash recovery ----------------------------------------------------
    def restore(self, trades, now_ms: int) -> int:
        """Rebuild in-memory state after a restart by re-playing trades that were ALREADY processed
        (read back from Kafka, up to the committed offset). Everything is recomputed exactly as
        before – dedup cache, rolling statistics, alert cooldowns, open bars – but the outputs are
        thrown away, because they were already published in an earlier committed transaction.
        Returns the number of trades replayed."""
        n = 0
        for t in trades:
            self.process(t, now_ms)  # outputs intentionally discarded
            n += 1
        dup = self.m_duplicates
        self._reset_metrics()        # replay must not distort live throughput / latency figures
        self.restored_duplicates = dup
        return n

    # ---- alerts ------------------------------------------------------------
    def _alert(self, rule, severity, symbol, exchange, event_ms, now_ms, value, threshold, message,
               trigger, details=None, cooldown_ms=None):
        ck = (rule, symbol, exchange)
        cd = self.cfg.ALERT_COOLDOWN_MS if cooldown_ms is None else cooldown_ms
        if cd and event_ms - self.cooldown.get(ck, -10**15) < cd:
            return None
        self.cooldown[ck] = event_ms
        self.m_alerts[rule] = self.m_alerts.get(rule, 0) + 1
        aid = hashlib.sha1(f"{rule}|{symbol}|{exchange}|{trigger}".encode()).hexdigest()[:16]
        return (self.cfg.TOPIC_ALERTS, symbol, {
            "alert_id": aid, "rule": rule, "severity": severity,
            "symbol": symbol, "exchange": exchange,
            "event_ts_ms": event_ms, "detected_ts_ms": now_ms,
            "value": round(value, 4), "threshold": round(threshold, 4),
            "message": message, "details": details or {},
        })

    # ---- core --------------------------------------------------------------
    def _is_duplicate(self, t: dict) -> bool:
        k = (t["exchange"], t["symbol"], t["trade_id"])
        if k in self.seen:
            self.seen.move_to_end(k)
            return True
        self.seen[k] = None
        if len(self.seen) > self.cfg.DEDUP_CACHE_SIZE:
            self.seen.popitem(last=False)
        return False

    def process(self, t: dict, now_ms: int) -> list[tuple[str, str, dict]]:
        """Process one validated trade. Returns a list of (topic, key, value) to emit."""
        if self._is_duplicate(t):
            self.m_duplicates += 1
            return []
        out: list = []
        cfg = self.cfg
        sym, exch, ev = t["symbol"], t["exchange"], t["event_ts_ms"]
        st = self.state.setdefault((sym, exch), SeriesState())
        self.m_processed += 1
        if t.get("backfill"):
            self.m_backfilled += 1   # recovered history: not part of live latency
        else:
            self.lat_e2e.append(now_ms - ev)
            self.lat_kafka.append(now_ms - t["ingest_ts_ms"])

        notional = t["price"] * t["size"]

        # -- rule: whale trade (checked against history BEFORE adding this trade)
        if len(st.sizes) >= 200:
            med = st.sizes.median()
            thr = max(cfg.WHALE_MIN_USD, cfg.WHALE_MEDIAN_MULT * med)
            if notional >= thr:
                sev = "high" if notional >= 4 * thr else "medium" if notional >= 2 * thr else "low"
                a = self._alert("WHALE_TRADE", sev, sym, exch, ev, now_ms, notional, thr,
                                f"{t['side'].upper()} {t['size']:g} {sym.split('-')[0]} "
                                f"(${notional:,.0f}) on {exch} = {notional / med:,.0f}x median trade",
                                trigger=t["trade_id"],
                                details={"price": t["price"], "size": t["size"], "side": t["side"],
                                         "median_notional": round(med, 2)},
                                cooldown_ms=0)  # every whale trade is its own event
                if a:
                    out.append(a)
        st.sizes.add(notional)

        # -- rule: cross-exchange divergence
        for (osym, oexch), ost in self.state.items():
            if osym != sym or oexch == exch or ost.last_price is None:
                continue
            if abs(ev - ost.last_event_ms) > cfg.DIVERGENCE_MAX_AGE_MS:
                continue
            mid = (t["price"] + ost.last_price) / 2
            bps = (t["price"] - ost.last_price) / mid * 1e4
            if abs(bps) >= cfg.DIVERGENCE_BPS:
                sev = "high" if abs(bps) >= 3 * cfg.DIVERGENCE_BPS else "medium"
                pair = "-vs-".join(sorted((exch, oexch)))  # one cooldown per exchange pair
                a = self._alert("EXCHANGE_DIVERGENCE", sev, sym, pair, ev, now_ms,
                                bps, cfg.DIVERGENCE_BPS,
                                f"{sym} {exch} {t['price']:g} vs {oexch} {ost.last_price:g} "
                                f"({bps:+.1f} bps)", trigger=t["trade_id"],
                                details={"price": t["price"], "other_price": ost.last_price})
                if a:
                    out.append(a)
        if ev >= st.last_event_ms:
            st.last_price, st.last_event_ms = t["price"], ev

        # -- event-time bars with watermark
        start = ev - ev % self.bar_ms
        if start + self.bar_ms <= self.watermark:
            # window already closed -> late event. Correct the bar instead of dropping the trade.
            self.m_late += 1
            key = (sym, exch, start)
            if start >= self.watermark - cfg.LATE_CORRECTION_MS:
                bar = self.closed_bars.get(key)
                if bar is None:
                    bar = self.closed_bars[key] = Bar(sym, exch, start, start + self.bar_ms,
                                                      t["price"], t["price"], t["price"], t["price"])
                bar.add(t)
                self.dirty_bars.add(key)
        else:
            key = (sym, exch, start)
            bar = self.open_bars.get(key)
            if bar is None:
                bar = self.open_bars[key] = Bar(sym, exch, start, start + self.bar_ms,
                                                t["price"], t["price"], t["price"], t["price"])
            bar.add(t)

        self.max_event_ms = max(self.max_event_ms, ev)
        self.watermark = max(self.watermark, self.max_event_ms - cfg.ALLOWED_LATENESS_MS)
        out.extend(self._close_bars(now_ms))
        return out

    def advance(self, now_ms: int) -> list[tuple[str, str, dict]]:
        """Called periodically: lets the watermark move with wall-clock time when a feed is idle."""
        self.watermark = max(self.watermark, now_ms - self.cfg.IDLE_FLUSH_MS)
        return self._close_bars(now_ms)

    def _close_bars(self, now_ms: int) -> list:
        out = []
        ready = [k for k, b in self.open_bars.items() if b.end_ms <= self.watermark]
        for k in sorted(ready, key=lambda k: k[2]):
            bar = self.open_bars.pop(k)
            self.closed_bars[k] = bar
            self.m_bars += 1
            out.append((self.cfg.TOPIC_BARS, bar.symbol, bar.to_dict()))
            out.extend(self._bar_rules(bar, now_ms))
        for k in sorted(self.dirty_bars, key=lambda k: k[2]):      # late-data corrections
            bar = self.closed_bars[k]
            self.m_corrections += 1
            out.append((self.cfg.TOPIC_BARS, bar.symbol, {**bar.to_dict(), "corrected": True}))
        self.dirty_bars.clear()
        if self.watermark - self._last_evict >= 60_000:              # evict old bars once a minute
            self._last_evict = self.watermark
            horizon = self.watermark - self.cfg.LATE_CORRECTION_MS
            self.closed_bars = {k: b for k, b in self.closed_bars.items() if k[2] >= horizon}
        return out

    def _bar_rules(self, bar: Bar, now_ms: int) -> list:
        cfg = self.cfg
        st = self.state[(bar.symbol, bar.exchange)]
        out = []
        # -- rule: price spike on bar log-return
        if st.prev_close:
            r = math.log(bar.close / st.prev_close)
            z = st.returns.z(r)
            bps = r * 1e4
            if st.returns.n >= cfg.WARMUP_BARS and abs(z) >= cfg.SPIKE_Z and abs(bps) >= cfg.SPIKE_MIN_BPS:
                sev = "high" if abs(z) >= 2 * cfg.SPIKE_Z else "medium"
                a = self._alert("PRICE_SPIKE", sev, bar.symbol, bar.exchange, bar.end_ms, now_ms, z,
                                cfg.SPIKE_Z,
                                f"{bar.symbol} moved {bps:+.1f} bps in {cfg.BAR_SECONDS}s on "
                                f"{bar.exchange} (z={z:+.1f})", trigger=bar.start_ms,
                                details={"prev_close": st.prev_close, "close": bar.close,
                                         "return_bps": round(bps, 2)})
                if a:
                    out.append(a)
            st.returns.update(r)
        st.prev_close = bar.close

        # -- rule: volume (trade count) burst
        z = st.counts.z(bar.trades)
        if st.counts.n >= cfg.WARMUP_BARS and z >= cfg.BURST_Z and bar.trades >= cfg.BURST_MIN_TRADES:
            a = self._alert("VOLUME_BURST", "high" if z >= 2 * cfg.BURST_Z else "medium",
                            bar.symbol, bar.exchange, bar.end_ms, now_ms, z, cfg.BURST_Z,
                            f"{bar.trades} trades in {cfg.BAR_SECONDS}s on {bar.exchange} "
                            f"(normal ≈ {st.counts.mean:.0f}, z={z:+.1f})", trigger=bar.start_ms,
                            details={"trades": bar.trades, "notional": round(bar.notional, 2)})
            if a:
                out.append(a)
        st.counts.update(bar.trades)
        return out
