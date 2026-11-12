"""Load test: replay a RECORDING of real trades (made with `producer --record`) into Kafka
as fast as possible (or at a fixed rate) to measure throughput / scalability.

The live feed only delivers ~20-200 trades/s, which does not stress Kafka. Replaying real
recorded trades lets us push 10k-100k msg/s through the same pipeline.

  python scripts/replay.py data/recording.jsonl --loops 20 --rate 0
  python scripts/replay.py data/recording.jsonl --loops 5 --rate 5000

Timestamps are shifted to "now" so latency metrics stay meaningful, and --new-ids makes each
loop's trade ids unique (otherwise the processor would correctly drop them as duplicates —
run once WITHOUT --new-ids to demonstrate de-duplication!).
"""
import argparse
import json
import os
import sys
import time

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from confluent_kafka import Producer  # noqa: E402

from streaming import config  # noqa: E402
from streaming.schema import encode, now_ms  # noqa: E402


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("file")
    ap.add_argument("--loops", type=int, default=1)
    ap.add_argument("--rate", type=float, default=0, help="messages/s, 0 = as fast as possible")
    ap.add_argument("--new-ids", action="store_true", default=True)
    ap.add_argument("--same-ids", dest="new_ids", action="store_false",
                    help="re-send identical trade ids (demonstrates de-duplication)")
    a = ap.parse_args()

    trades = []
    for line in open(a.file, encoding="utf-8"):
        try:
            trades.append(json.loads(line))
        except ValueError:   # e.g. two producers appending to the same file at once
            pass
    if not trades:
        sys.exit("recording is empty")
    print(f"loaded {len(trades):,} real trades from {a.file}")

    p = Producer({"bootstrap.servers": config.KAFKA_BOOTSTRAP, "enable.idempotence": True, "acks": "all",
                  "compression.type": "lz4", "linger.ms": 50, "batch.num.messages": 20000,
                  "queue.buffering.max.messages": 1_000_000})
    sent, t0 = 0, time.time()
    for loop in range(a.loops):
        for t in trades:
            t2 = dict(t)
            t2.pop("backfill", None)
            t2["event_ts_ms"] = t2["ingest_ts_ms"] = now_ms()  # re-timestamp: replayed "now"
            if a.new_ids:
                t2["trade_id"] = f"{t['trade_id']}-r{loop}"
            while True:
                try:
                    p.produce(config.TOPIC_RAW, key=t2["symbol"], value=encode(t2))
                    break
                except BufferError:
                    p.poll(0.05)
            p.poll(0)
            sent += 1
            if a.rate:
                ahead = sent / a.rate - (time.time() - t0)
                if ahead > 0:
                    time.sleep(ahead)
        print(f"loop {loop + 1}/{a.loops}: {sent:,} sent, {sent / (time.time() - t0):,.0f} msg/s")
    p.flush(60)
    dt = time.time() - t0
    print(f"DONE: {sent:,} messages in {dt:.1f}s = {sent / dt:,.0f} msg/s (acks=all, idempotent)")


if __name__ == "__main__":
    main()
