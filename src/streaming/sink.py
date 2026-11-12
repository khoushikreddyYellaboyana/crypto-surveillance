"""Sink: Kafka (bars, alerts, metrics) -> SQLite, read by the Streamlit dashboard.

Delivery guarantee: at-least-once from Kafka + idempotent UPSERTs keyed on natural ids
(alert_id, symbol/exchange/start_ms, component/instance/ts_ms) = effectively exactly-once in the DB.
Offsets are committed only after the SQLite transaction is committed.

CRASH RECOVERY: if the sink (or the whole machine) goes down, Kafka keeps buffering bars/alerts.
On restart the sink continues from its last committed offset and writes everything it missed, so
the dashboard shows no hole. Anything written to SQLite but not yet committed in Kafka is simply
written again – harmless, because every write is an idempotent UPSERT.
"""
from __future__ import annotations

import json
import logging
import os
import signal
import socket
import sqlite3
import time

from confluent_kafka import Consumer

from . import config
from .schema import decode, now_ms

log = logging.getLogger("sink")

DDL = """
CREATE TABLE IF NOT EXISTS bars (
  symbol TEXT, exchange TEXT, start_ms INTEGER, end_ms INTEGER,
  open REAL, high REAL, low REAL, close REAL, volume REAL, notional REAL, vwap REAL,
  trades INTEGER, buy_volume REAL, sell_volume REAL,
  PRIMARY KEY (symbol, exchange, start_ms));
CREATE TABLE IF NOT EXISTS alerts (
  alert_id TEXT PRIMARY KEY, rule TEXT, severity TEXT, symbol TEXT, exchange TEXT,
  event_ts_ms INTEGER, detected_ts_ms INTEGER, value REAL, threshold REAL,
  message TEXT, details TEXT);
CREATE TABLE IF NOT EXISTS metrics (
  component TEXT, instance TEXT, ts_ms INTEGER, payload TEXT,
  PRIMARY KEY (component, instance, ts_ms));
CREATE INDEX IF NOT EXISTS ix_bars_time ON bars(start_ms);
CREATE INDEX IF NOT EXISTS ix_alerts_time ON alerts(event_ts_ms);
CREATE INDEX IF NOT EXISTS ix_metrics_time ON metrics(ts_ms);
"""


def connect(path: str) -> sqlite3.Connection:
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    db = sqlite3.connect(path, timeout=30)
    db.execute("PRAGMA journal_mode=WAL")      # dashboard can read while we write
    db.execute("PRAGMA synchronous=NORMAL")
    db.executescript(DDL)
    return db


def write(db: sqlite3.Connection, topic: str, v: dict):
    if topic == config.TOPIC_BARS:
        db.execute("""INSERT OR REPLACE INTO bars VALUES
            (:symbol,:exchange,:start_ms,:end_ms,:open,:high,:low,:close,:volume,:notional,:vwap,
             :trades,:buy_volume,:sell_volume)""", v)
    elif topic == config.TOPIC_ALERTS:
        db.execute("""INSERT OR IGNORE INTO alerts VALUES
            (:alert_id,:rule,:severity,:symbol,:exchange,:event_ts_ms,:detected_ts_ms,:value,:threshold,
             :message,:details)""", {**v, "details": json.dumps(v.get("details", {}))})
    elif topic == config.TOPIC_METRICS:
        db.execute("INSERT OR REPLACE INTO metrics VALUES (?,?,?,?)",
                   (v.get("component"), v.get("instance", ""), v.get("ts_ms"), json.dumps(v)))


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")
    db = connect(config.SQLITE_PATH)
    c = Consumer({
        "bootstrap.servers": config.KAFKA_BOOTSTRAP,
        "group.id": os.getenv("SINK_GROUP", "sqlite-sink"),
        "enable.auto.commit": False,
        "auto.offset.reset": "earliest",
        "isolation.level": "read_committed",
    })
    c.subscribe([config.TOPIC_BARS, config.TOPIC_ALERTS, config.TOPIC_METRICS])
    running = True

    def stop(*_):
        nonlocal running
        running = False

    signal.signal(signal.SIGINT, stop)
    signal.signal(signal.SIGTERM, stop)
    log.info("sink writing to %s", config.SQLITE_PATH)
    total, window, started, last_report = 0, 0, now_ms(), time.time()
    first_report = True
    while running:
        msgs = c.consume(num_messages=1000, timeout=1.0)
        good = [m for m in msgs if not m.error()]
        if time.time() - last_report >= config.METRICS_INTERVAL_S:
            lag = 0
            for tp in c.assignment():
                try:
                    _, high = c.get_watermark_offsets(tp, timeout=1)
                    pos = c.position([tp])[0].offset
                    lag += max(0, high - pos) if pos >= 0 else 0
                except Exception:  # noqa: BLE001 – metrics are best effort
                    pass
            m = {"component": "sink", "instance": socket.gethostname(), "ts_ms": now_ms(),
                 "records_per_s": round(window / (time.time() - last_report), 2),
                 "records_total": total, "consumer_lag": lag, "started_ms": started}
            with db:
                write(db, config.TOPIC_METRICS, m)
            if first_report or lag > 1000:
                log.info("RECOVERY: sink resumed from committed offsets – %d records still to catch up", lag)
                first_report = False
            window, last_report = 0, time.time()
        if not good:
            continue
        with db:  # one SQLite transaction per batch
            for m in good:
                try:
                    write(db, m.topic(), decode(m.value()))
                except (ValueError, KeyError, sqlite3.Error) as e:
                    log.error("skipping bad record on %s: %r", m.topic(), e)
        c.commit(asynchronous=False)
        total += len(good)
        window += len(good)
        if total % 500 < len(good):
            log.info("written %d records", total)
    c.close()
    db.close()


if __name__ == "__main__":
    main()
