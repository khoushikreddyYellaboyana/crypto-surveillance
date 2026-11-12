"""Unit tests for parsing, validation and detection logic (no Kafka / network needed).

    pytest -q
"""
import json
import os
import sys
import types

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

import pytest  # noqa: E402

from streaming import config  # noqa: E402
from streaming.detector import SurveillanceEngine  # noqa: E402
from streaming.schema import InvalidTrade, make_trade, validate  # noqa: E402
from streaming.sources.binance import BinanceSource  # noqa: E402
from streaming.sources.coinbase import CoinbaseSource  # noqa: E402

T0 = 1_762_600_000_000  # fixed epoch ms, aligned to a 5 s bar boundary
T0 -= T0 % 5000


def cfg(**over):
    c = types.SimpleNamespace(**{k: getattr(config, k) for k in dir(config) if k.isupper()})
    for k, v in over.items():
        setattr(c, k, v)
    return c


def trade(i, price=100.0, size=1.0, ts=None, exch="coinbase", sym="BTC-USD", side="buy"):
    ts = T0 + i * 100 if ts is None else ts
    return make_trade(exch, sym, i, price, size, side, ts, ts + 50)


# ---------------------------------------------------------------- source parsing
def test_coinbase_match_parsing_and_taker_side():
    src = CoinbaseSource("wss://x", ["BTC-USD"])
    raw = json.dumps({"type": "match", "trade_id": 42, "sequence": 1, "time": "2026-11-08T10:00:00.123456Z",
                      "product_id": "BTC-USD", "size": "0.5", "price": "70000.10", "side": "sell"})
    [t] = src.parse(raw, 1_794_132_000_500)  # 2026-11-08T10:00:00.5Z
    assert t["exchange"] == "coinbase" and t["symbol"] == "BTC-USD" and t["trade_id"] == "42"
    assert t["price"] == 70000.10 and t["size"] == 0.5
    assert t["side"] == "buy"            # maker sold -> taker bought
    assert t["event_ts_ms"] % 1000 == 123


def test_coinbase_ignores_heartbeats_and_last_match():
    src = CoinbaseSource("wss://x", ["BTC-USD"])
    assert src.parse(json.dumps({"type": "heartbeat"}), 1) == []
    assert src.parse(json.dumps({"type": "last_match", "trade_id": 1}), 1) == []


def test_binance_trade_parsing_maps_usdt_to_usd():
    src = BinanceSource("wss://x/stream", ["BTC-USD", "ETH-USD"])
    assert "btcusdt@trade/ethusdt@trade" in src.url
    raw = json.dumps({"stream": "ethusdt@trade", "data": {"e": "trade", "E": T0, "s": "ETHUSDT", "t": 7,
                      "p": "3000.5", "q": "2", "T": T0, "m": True}})
    [t] = src.parse(raw, T0 + 10)
    assert t["symbol"] == "ETH-USD" and t["side"] == "sell" and t["event_ts_ms"] == T0


def test_validation_rejects_bad_records():
    with pytest.raises(InvalidTrade):
        make_trade("coinbase", "BTC-USD", 1, -5, 1, "buy", T0, T0)
    with pytest.raises(InvalidTrade):
        make_trade("coinbase", "BTC-USD", 1, 5, 1, "hold", T0, T0)
    with pytest.raises(InvalidTrade):
        validate({"exchange": "x"})


# ---------------------------------------------------------------- engine
def test_dedup_drops_same_trade_twice():
    e = SurveillanceEngine(cfg())
    t = trade(1)
    e.process(dict(t), T0)
    assert e.process(dict(t), T0) == []
    assert e.m_duplicates == 1 and e.m_processed == 1


def test_bars_close_on_watermark_with_correct_ohlcv():
    e = SurveillanceEngine(cfg(ALLOWED_LATENESS_MS=1000))
    out = []
    prices = [100, 105, 95, 102]
    for i, p in enumerate(prices):
        out += e.process(trade(i, price=p, ts=T0 + i * 1000), T0)
    assert not [o for o in out if o[0] == config.TOPIC_BARS]       # window still open
    out += e.process(trade(99, price=101, ts=T0 + 6500), T0)        # watermark passes T0+5000
    bars = [o[2] for o in out if o[0] == config.TOPIC_BARS]
    assert len(bars) == 1
    b = bars[0]
    assert (b["open"], b["high"], b["low"], b["close"], b["trades"]) == (100, 105, 95, 102, 4)


def test_late_event_is_counted_not_added_to_closed_bar():
    e = SurveillanceEngine(cfg(ALLOWED_LATENESS_MS=1000))
    e.process(trade(1, ts=T0 + 100), T0)
    e.process(trade(2, ts=T0 + 7000), T0)        # closes first bar
    e.process(trade(3, ts=T0 + 200), T0)         # belongs to the closed bar -> late
    assert e.m_late == 1


def test_whale_trade_alert():
    e = SurveillanceEngine(cfg(WHALE_MIN_USD=100_000, WHALE_MEDIAN_MULT=50))
    for i in range(300):
        e.process(trade(i, price=100, size=10), T0)     # $1,000 trades
    out = e.process(trade(1000, price=100, size=5000), T0)  # $500,000
    alerts = [o[2] for o in out if o[0] == config.TOPIC_ALERTS]
    assert alerts and alerts[0]["rule"] == "WHALE_TRADE" and alerts[0]["severity"] == "high"  # 5x the floor


def test_cross_exchange_divergence_alert():
    e = SurveillanceEngine(cfg(DIVERGENCE_BPS=40))
    e.process(trade(1, price=100.0, exch="coinbase"), T0)
    out = e.process(trade(1, price=100.1, exch="binance", ts=T0 + 200), T0)   # 10 bps -> fine
    assert not [o for o in out if o[0] == config.TOPIC_ALERTS]
    out = e.process(trade(2, price=101.0, exch="binance", ts=T0 + 300), T0)   # ~100 bps
    alerts = [o[2] for o in out if o[0] == config.TOPIC_ALERTS]
    assert alerts and alerts[0]["rule"] == "EXCHANGE_DIVERGENCE"


def test_price_spike_and_volume_burst_after_warmup():
    e = SurveillanceEngine(cfg(ALLOWED_LATENESS_MS=0, WARMUP_BARS=30, SPIKE_Z=5, SPIKE_MIN_BPS=15,
                               BURST_Z=5, BURST_MIN_TRADES=50))
    out, tid, price = [], 0, 100.0
    for b in range(60):                       # 60 calm bars: tiny alternating moves, 10 trades each
        price *= 1.0002 if b % 2 else 0.9998
        for k in range(10):
            tid += 1
            out += e.process(trade(tid, price=price, ts=T0 + b * 5000 + k * 400), T0)
    assert not [o for o in out if o[0] == config.TOPIC_ALERTS]
    b = 60                                     # crash bar: -3% with 400 trades
    for k in range(400):
        tid += 1
        out += e.process(trade(tid, price=price * 0.97, ts=T0 + b * 5000 + k * 10), T0)
    out += e.advance(T0 + 10**9)              # flush via idle watermark
    rules = {o[2]["rule"] for o in out if o[0] == config.TOPIC_ALERTS}
    assert {"PRICE_SPIKE", "VOLUME_BURST"} <= rules


def test_alert_ids_are_deterministic_for_idempotent_sink():
    def run():
        e = SurveillanceEngine(cfg(DIVERGENCE_BPS=40))
        e.process(trade(1, price=100.0), T0)
        return [o[2]["alert_id"] for o in e.process(trade(2, price=105, exch="binance"), T0)
                if o[0] == config.TOPIC_ALERTS]
    assert run() == run() and len(run()) == 1


def test_metrics_snapshot():
    e = SurveillanceEngine(cfg())
    for i in range(10):
        e.process(trade(i), T0 + 1000 + i * 100)
    m = e.snapshot_metrics(T0 + 5000, 5.0)
    assert m["msgs"] == 10 and m["msgs_per_s"] == 2.0 and m["latency_e2e_ms_p50"] > 0
    assert e.m_processed == 0  # reset after snapshot


# ---------------------------------------------------------------- crash recovery & duplicates
def test_restore_rebuilds_state_so_resent_trades_are_still_duplicates():
    history = [trade(i, price=100 + (i % 7), size=1 + i % 3) for i in range(500)]
    live = SurveillanceEngine(cfg())
    for t in history:
        live.process(dict(t), T0 + 60_000)

    # processor crashes; a new instance restores from Kafka history
    fresh = SurveillanceEngine(cfg())
    n = fresh.restore([dict(t) for t in history], T0 + 60_000)
    assert n == 500
    assert fresh.m_processed == 0                      # restore does not pollute live metrics
    key = ("BTC-USD", "coinbase")
    assert len(fresh.state[key].sizes) == len(live.state[key].sizes)
    assert fresh.state[key].sizes.median() == live.state[key].sizes.median()
    # producer re-sends an already processed trade after the restart -> dropped
    assert fresh.process(dict(history[123]), T0 + 61_000) == []
    assert fresh.m_duplicates == 1


def test_two_producers_sending_everything_twice_give_identical_results():
    trades = [trade(i, price=100 + (i % 11) * 0.5, ts=T0 + i * 250) for i in range(400)]
    once, twice = SurveillanceEngine(cfg()), SurveillanceEngine(cfg())
    out1, out2 = [], []
    for t in trades:
        out1 += once.process(dict(t), T0)
    for t in trades:                       # producer A and producer B interleaved
        out2 += twice.process(dict(t), T0)
        out2 += twice.process(dict(t), T0)
    out1 += once.advance(T0 + 10**9)
    out2 += twice.advance(T0 + 10**9)
    assert twice.m_duplicates == 400
    bars1 = [o[2] for o in out1 if o[0] == config.TOPIC_BARS]
    bars2 = [o[2] for o in out2 if o[0] == config.TOPIC_BARS]
    assert bars1 == bars2 and len(bars1) > 0


def test_late_backfilled_trade_corrects_already_emitted_bar():
    e = SurveillanceEngine(cfg(ALLOWED_LATENESS_MS=1000))
    out = e.process(trade(1, price=100, ts=T0 + 100), T0)
    out += e.process(trade(2, price=101, ts=T0 + 8000), T0)          # closes bar [T0, T0+5s)
    first = [o[2] for o in out if o[0] == config.TOPIC_BARS][0]
    assert first["trades"] == 1
    late = trade(3, price=120, ts=T0 + 200)
    late["backfill"] = True
    out = e.process(late, T0 + 60_000)
    corrected = [o[2] for o in out if o[0] == config.TOPIC_BARS]
    assert corrected and corrected[0]["corrected"] and corrected[0]["trades"] == 2
    assert corrected[0]["start_ms"] == first["start_ms"] and corrected[0]["high"] == 120
    assert e.lat_e2e and len(e.lat_e2e) == 2           # backfilled trade not in live latency


def test_producer_skips_duplicates_and_checkpoints_only_acked(tmp_path, monkeypatch):
    from streaming import producer as prod
    monkeypatch.setattr(prod.config, "PRODUCER_STATE_PATH", str(tmp_path / "state.json"))
    monkeypatch.setattr(prod.config, "KAFKA_BOOTSTRAP", "127.0.0.1:1")   # never connects
    p = prod.TradeProducer()
    t = trade(10)
    p.send_trade(dict(t))
    p.send_trade(dict(t))                                  # e.g. backfill overlapping live stream
    assert p.stats.sent == 1 and p.stats.skipped_dupes == 1
    p.save_checkpoint()
    assert json.load(open(tmp_path / "state.json"))["last"] == {}      # not acknowledged yet
    p._on_delivery(None, None, ("coinbase", "BTC-USD"), 10, t["event_ts_ms"])
    p.save_checkpoint()
    restarted = prod.TradeProducer()                       # simulated crash + restart
    assert restarted.last_id[("coinbase", "BTC-USD")] == 10


def test_coinbase_backfill_returns_only_missed_trades(monkeypatch):
    from streaming.sources import coinbase as cb
    rows = [{"trade_id": i, "time": "2026-11-08T10:00:00.000Z", "price": "100", "size": "1", "side": "buy"}
            for i in range(110, 95, -1)]                    # newest first, like the real API
    monkeypatch.setattr(cb, "get_json", lambda url: (rows, {}))
    got = CoinbaseSource("wss://x", ["BTC-USD"]).backfill("https://x", "BTC-USD", 100, 0, 1_794_132_000_500)
    assert [int(t["trade_id"]) for t in got] == list(range(101, 111))
    assert got[0]["side"] == "sell"


class _Msg:
    def __init__(self, p, off, val):
        self._p, self._o, self._v = p, off, val

    def error(self): return None
    def partition(self): return self._p
    def offset(self): return self._o
    def value(self): return self._v


class FakeKafka:
    """In-memory stand-in for a Kafka consumer (enough for the restore logic)."""
    log = {}          # partition -> list of encoded trades
    committed_at = {}

    def __init__(self, conf): self.pos = {}

    def committed(self, tps, timeout=None):
        from confluent_kafka import TopicPartition
        return [TopicPartition(tp.topic, tp.partition, self.committed_at.get(tp.partition, -1001)) for tp in tps]

    def offsets_for_times(self, tps, timeout=None):
        from confluent_kafka import TopicPartition
        return [TopicPartition(tp.topic, tp.partition, 0 if self.log.get(tp.partition) else -1) for tp in tps]

    def get_watermark_offsets(self, tp, timeout=None): return 0, len(self.log.get(tp.partition, []))

    def assign(self, tps): self.pos = {tp.partition: tp.offset for tp in tps}

    def consume(self, num_messages=1, timeout=None):
        out = []
        for p, off in list(self.pos.items()):
            if off < len(self.log[p]):
                out.append(_Msg(p, off, self.log[p][off]))
                self.pos[p] = off + 1
        return out

    def position(self, tps):
        from confluent_kafka import TopicPartition
        return [TopicPartition(tp.topic, tp.partition, self.pos.get(tp.partition, -1)) for tp in tps]

    def close(self): pass


def test_processor_restore_reads_only_up_to_committed_offset(monkeypatch):
    from confluent_kafka import TopicPartition
    from streaming import processor
    from streaming.schema import encode
    FakeKafka.log = {0: [encode(trade(i)) for i in range(100)], 1: [encode(trade(i, sym="ETH-USD")) for i in range(50)]}
    FakeKafka.committed_at = {0: 80, 1: -1001}   # p0 processed up to 80; p1 never committed
    monkeypatch.setattr(processor, "Consumer", FakeKafka)
    eng = SurveillanceEngine(cfg())
    info = processor.restore_state(eng, "g", [TopicPartition("market.trades.raw", 0),
                                              TopicPartition("market.trades.raw", 1)])
    assert info["restored_trades"] == 80 + 50
    assert info["resume_offsets"] == {"0": 80, "1": 50}
    # trade 79 was already processed -> duplicate; trade 80 was not -> processed normally
    assert eng.process(validate(json.loads(FakeKafka.log[0][79])), T0) == [] and eng.m_duplicates == 1
    eng.process(validate(json.loads(FakeKafka.log[0][80])), T0)
    assert eng.m_processed == 1


def test_producer_restart_backfills_gap_then_live_without_duplicates(tmp_path, monkeypatch):
    import asyncio
    from streaming import producer as prod
    monkeypatch.setattr(prod.config, "PRODUCER_STATE_PATH", str(tmp_path / "state.json"))
    monkeypatch.setattr(prod.config, "KAFKA_BOOTSTRAP", "127.0.0.1:1")
    json.dump({"saved_ms": T0, "last": {"coinbase|BTC-USD": {"id": 10, "ts": T0}}},
              open(tmp_path / "state.json", "w"))

    class Src:
        name, symbols = "coinbase", ["BTC-USD"]

        def backfill(self, url, sym, last_id, min_ev, ingest):
            assert last_id == 10                         # resumes exactly after the checkpoint
            return [trade(i, ts=T0 + i) for i in range(11, 16)]

    p = prod.TradeProducer()                             # "restarted" producer
    monkeypatch.setattr(prod.config, "BACKFILL_MAX_AGE_MS", 10**15)
    asyncio.run(p.backfill(Src()))
    for i in range(14, 21):                              # live stream overlaps the backfill (14, 15)
        p.send_trade(trade(i, ts=T0 + i))
    assert p.stats.backfilled == 5 and p.stats.skipped_dupes == 2
    assert p.stats.sent == 10 and p.stats.gaps == 0      # ids 11..20 sent exactly once, no hole
