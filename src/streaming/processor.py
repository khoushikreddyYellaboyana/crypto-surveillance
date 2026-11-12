"""Stream processor: market.trades.raw -> {market.bars.5s, market.alerts, pipeline.metrics, dlq}.

Exactly-once consume-transform-produce using Kafka transactions:
  each poll batch = one transaction containing all output records AND the consumer offsets.
  Downstream consumers use isolation.level=read_committed, so a crash mid-batch never
  produces duplicates or partial results.

Scale out: run more instances (docker compose up --scale processor=3). They share the
consumer group, Kafka splits the 6 partitions between them, and because the key is the
symbol each instance owns complete per-symbol state.
"""
from __future__ import annotations

import logging
import os
import signal
import socket
import sys
import time

from confluent_kafka import Consumer, KafkaException, Producer, TopicPartition

from . import config
from .detector import SurveillanceEngine
from .schema import InvalidTrade, decode, encode, now_ms, validate

log = logging.getLogger("processor")


def restore_state(engine: SurveillanceEngine, group: str, partitions) -> dict:
    """CRASH RECOVERY. Called when partitions are assigned (start-up, restart after a crash, or a
    rebalance when instances are added/removed).

    Kafka still holds every trade (24 h retention), so we re-read the last RESTORE_WINDOW_MS of
    trades that this group already processed – i.e. up to the committed offset – and replay them
    through the engine with outputs discarded. Afterwards the engine is in exactly the state it had
    before the crash: the dedup cache knows every recent trade id (so trades the producer sends
    again are still recognised as duplicates), and the rolling statistics need no new warm-up.
    Processing then continues from the committed offset, so nothing is lost and nothing is
    emitted twice (outputs + offsets were committed atomically in the same transaction)."""
    t0 = time.time()
    parts = [p.partition for p in partitions]
    reader = Consumer({
        "bootstrap.servers": config.KAFKA_BOOTSTRAP,
        "group.id": group,               # same group -> can read its committed offsets,
        "enable.auto.commit": False,     # but never commits and never joins the group
        "isolation.level": "read_committed",
    })
    try:
        tps = [TopicPartition(config.TOPIC_RAW, p) for p in parts]
        committed = {tp.partition: tp.offset for tp in reader.committed(tps, timeout=10)}
        since = now_ms() - config.RESTORE_WINDOW_MS
        starts = reader.offsets_for_times([TopicPartition(config.TOPIC_RAW, p, since) for p in parts], timeout=10)
        assign, ends = [], {}
        for tp in starts:
            low, high = reader.get_watermark_offsets(TopicPartition(config.TOPIC_RAW, tp.partition), timeout=10)
            end = committed.get(tp.partition, -1)
            ends[tp.partition] = end if end >= 0 else high   # no commit yet -> warm up on history
            if tp.offset >= 0 and tp.offset < ends[tp.partition]:
                assign.append(TopicPartition(config.TOPIC_RAW, tp.partition, tp.offset))
        trades, done = [], set()
        if assign:
            reader.assign(assign)
            todo = {tp.partition for tp in assign}
            idle = 0
            while done != todo and idle < 5:
                batch = reader.consume(num_messages=5000, timeout=1.0)
                idle = idle + 1 if not batch else 0
                for m in batch:
                    if m.error():
                        continue
                    if m.offset() >= ends[m.partition()]:
                        done.add(m.partition())
                        continue
                    try:
                        trades.append(validate(decode(m.value())))
                    except (InvalidTrade, ValueError, TypeError):
                        pass
                for pos in reader.position(assign):
                    if pos.offset >= ends[pos.partition]:
                        done.add(pos.partition)
        trades.sort(key=lambda t: t["ingest_ts_ms"])      # approximately the original order
        n = engine.restore(trades, now_ms())
    finally:
        reader.close()
    info = {"partitions": parts, "restored_trades": n, "restore_ms": int((time.time() - t0) * 1000),
            "restored_duplicates": getattr(engine, "restored_duplicates", 0),
            "resume_offsets": {str(k): v for k, v in ends.items()}}
    log.info("RECOVERY: rebuilt state for partitions %s from %d already-processed trades in %d ms; "
             "resuming at committed offsets %s", parts, n, info["restore_ms"], info["resume_offsets"])
    return info


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")
    instance = socket.gethostname()
    group = os.getenv("PROCESSOR_GROUP", "surveillance-processor")

    consumer = Consumer({
        "bootstrap.servers": config.KAFKA_BOOTSTRAP,
        "group.id": group,
        "client.id": f"processor-{instance}",
        "enable.auto.commit": False,
        "auto.offset.reset": os.getenv("OFFSET_RESET", "latest"),
        "isolation.level": "read_committed",
        "partition.assignment.strategy": "cooperative-sticky",
    })
    producer = Producer({
        "bootstrap.servers": config.KAFKA_BOOTSTRAP,
        "transactional.id": f"{group}-{instance}",
        "enable.idempotence": True,
        "compression.type": "lz4",
        "linger.ms": 10,
    })
    producer.init_transactions()

    engine = SurveillanceEngine()
    running = True
    invalid = 0
    recovery_events: list[dict] = []
    started_ms = now_ms()

    def on_assign(c, ps):
        log.info("assigned partitions %s", [p.partition for p in ps])
        if ps:
            try:
                recovery_events.append(restore_state(engine, group, ps))
            except KafkaException as e:   # recovery is best effort; processing still resumes correctly
                log.warning("state restore failed (%s) – continuing with cold state", e)

    consumer.subscribe([config.TOPIC_RAW], on_assign=on_assign,
                       on_revoke=lambda c, ps: log.info("revoked partitions %s", [p.partition for p in ps]))

    def stop(*_):
        nonlocal running
        running = False

    signal.signal(signal.SIGINT, stop)
    signal.signal(signal.SIGTERM, stop)

    last_metrics = time.time()
    log.info("processor %s started (group=%s)", instance, group)

    while running:
        msgs = consumer.consume(num_messages=2000, timeout=0.5)
        now = now_ms()
        outputs: list[tuple[str, str, dict]] = []
        consumed = False
        for msg in msgs:
            if msg.error():
                log.warning("consumer error: %s", msg.error())
                continue
            consumed = True
            try:
                t = validate(decode(msg.value()))
            except (InvalidTrade, ValueError, TypeError) as e:
                invalid += 1
                outputs.append((config.TOPIC_DLQ, "processor", {
                    "source": "processor", "error": repr(e),
                    "raw": msg.value()[:2000].decode(errors="replace"), "ts_ms": now}))
                continue
            outputs.extend(engine.process(t, now))
        outputs.extend(engine.advance(now))

        elapsed = time.time() - last_metrics
        if elapsed >= config.METRICS_INTERVAL_S:
            lag = 0
            for tp in consumer.assignment():
                try:
                    _, high = consumer.get_watermark_offsets(tp, timeout=1)
                    pos = consumer.position([tp])[0].offset
                    if pos >= 0:
                        lag += max(0, high - pos)
                except KafkaException:
                    pass
            m = engine.snapshot_metrics(now, elapsed, {
                "instance": instance, "invalid": invalid, "consumer_lag": lag,
                "started_ms": started_ms,
                "recovery": recovery_events[:],
                "partitions": sorted(tp.partition for tp in consumer.assignment())})
            invalid = 0
            recovery_events.clear()
            outputs.append((config.TOPIC_METRICS, "processor", m))
            last_metrics = time.time()
            log.info("%.0f msg/s | e2e p50=%sms p95=%sms | kafka p95=%sms | lag=%d | dup=%d late=%d | alerts=%s",
                     m["msgs_per_s"], m["latency_e2e_ms_p50"], m["latency_e2e_ms_p95"],
                     m["latency_kafka_ms_p95"], lag, m["duplicates"], m["late_events"], m["alerts"])

        if not outputs and not consumed:
            continue
        try:
            producer.begin_transaction()
            for topic, key, value in outputs:
                producer.produce(topic, key=key, value=encode(value))
                if topic == config.TOPIC_ALERTS:
                    log.warning("ALERT %-20s %-6s %s", value["rule"], value["severity"], value["message"])
            if consumed:
                positions = consumer.position(consumer.assignment())
                producer.send_offsets_to_transaction(positions, consumer.consumer_group_metadata())
            producer.commit_transaction()
        except KafkaException as e:
            # Fail fast: the open transaction is aborted by the broker / fenced, nothing partial is
            # visible to read_committed readers, and Docker restarts us from the last committed offset.
            log.error("transaction failed (%s) – aborting and exiting for a clean restart", e)
            try:
                producer.abort_transaction()
            except KafkaException:
                pass
            consumer.close()
            sys.exit(1)

    log.info("shutting down")
    consumer.close()
    producer.flush(10)


if __name__ == "__main__":
    main()
