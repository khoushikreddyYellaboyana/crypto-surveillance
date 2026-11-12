# Presentation / demo script (~10 minutes)

Start the stack **before** presenting (`docker compose up -d --build`) so the statistical rules are warmed up (≈3 min) and a recording exists for the load test.

1. **Problem (1 min)** — Crypto markets trade 24/7 across many exchanges. Surveillance teams need to see manipulation and abnormal activity in seconds, not in tomorrow's batch report.
2. **Data source (1 min)** — Real live trades from Coinbase and Binance public WebSockets (from the conduktor public-streaming-api list). Show `docker compose logs producer` → `connected`, and a raw message in Kafka UI → topic `market.trades.raw`.
3. **Architecture (2 min)** — README diagram: producer → Kafka → processor (consumer group) → alerts/bars/metrics topics → sink → dashboard. Explain key = symbol, 6 partitions, event time, exactly-once transactions.
4. **Live dashboard (2 min)** — http://localhost:8501 → *Business overview*: volume monitored, suspicious events, whale value flagged, risk per asset, arbitrage gap, sentiment; then the alert list – each alert has an owner team and a recommended action.
5. **The five measures (3 min)**
   * *Throughput*: `docker compose run --rm producer python scripts/replay.py /data/recording.jsonl --loops 30` → rate goes from ~100 msg/s to tens of thousands.
   * *Latency*: e2e p95 tile; alert "detect ms" column.
   * *Scalability*: `docker compose up -d --scale processor=3` → Kafka UI consumer group shows 3 members / partitions split; dashboard instances = 3.
   * *Reliability*: `docker compose kill processor` → comes back, resumes from committed offset, lag drains.
   * *Accuracy*: `... replay.py /data/recording.jsonl --same-ids` → duplicates counter jumps, no new bars/alerts; mention validation, DLQ, late events, feed-gap counter.
   * *Crash recovery* (README 3a): `docker compose stop producer`, wait 60 s, `docker compose start producer` → logs show the backfill; dashboard → Pipeline health → Recovery log.
   * *Duplicates* (README 3b): `docker compose up -d --scale producer=2` → "Duplicates blocked" climbs, business numbers unchanged.
6. **Limitations & future work (1 min)** — see README section 7.
