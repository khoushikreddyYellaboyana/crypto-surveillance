Real-Time Crypto Market Surveillance with Apache Kafka

Student: Koushik reddy
Course: Data Management

1. Project idea

Domain: Finance - cryptocurrency markets
Business objective: Fraud and anomaly detection (market surveillance)
Data source: Real, live trades from the public WebSockets of Coinbase and Binance (from the public-streaming-api list: [https://github.com/conduktor/public-streaming-api](https://github.com/conduktor/public-streaming-api)). No synthetic data. No API key needed.
Coins: BTC, ETH, SOL, XRP, DOGE

Crypto markets trade 24/7 on many exchanges. Large players can move prices, bots can create fake volume, and prices can crash within seconds. This project streams every trade through Apache Kafka and raises an alert within seconds when something suspicious happens.

2. What the system detects

Alert: Whale trade
Meaning: One very large trade (>= $250k and >= 50x the normal trade size)
Who acts: Compliance

Alert: Price spike
Meaning: Price jumps or crashes abnormally within 5 seconds
Who acts: Risk management

Alert: Volume burst
Meaning: Far more trades than normal (bots, wash trading, news)
Who acts: Compliance

Alert: Exchange price gap
Meaning: Same coin has a different price on Coinbase and Binance (>= 0.40%)
Who acts: Trading desk

3. Architecture

Data flow:
Coinbase and Binance -> Producer -> Kafka (trades) -> Processor (consumer) -> Kafka (alerts, bars, metrics) -> Sink -> SQLite (database) -> Dashboard (website)

Part: Producer
File: src/streaming/producer.py
Job: Receives live trades, cleans them, sends them to Kafka

Part: Kafka
File: docker-compose.yml
Job: Stores and delivers the streams (5 topics, 6 partitions for trades)

Part: Processor
File: src/streaming/processor.py, detector.py
Job: Reads trades, removes duplicates, builds 5-second price bars, detects the 4 alerts

Part: Sink
File: src/streaming/sink.py
Job: Saves bars, alerts and metrics to a SQLite database

Part: Dashboard
File: dashboard/app.py
Job: Website with a Business overview and a Pipeline health and recovery tab

Kafka topics: market.trades.raw, market.bars.5s, market.alerts, pipeline.metrics, market.trades.dlq (broken messages)

4. How to run

Requirements: Docker Desktop (running) and an internet connection.

cd crypto-surveillance
docker compose up -d --build

Wait about 1 minute, then open:

* Dashboard: http://localhost:8501
* Kafka UI: http://localhost:8080

Useful commands:
docker compose ps (are all parts running?)
docker compose logs -f producer (live trades going into Kafka, Ctrl+C to stop watching)
docker compose logs -f processor (consumer + alerts)
docker compose down (stop everything)

If Binance is blocked on your network:
SOURCES=coinbase docker compose up -d

Run the tests:
docker compose run --rm producer pytest -q tests

5. The five pipeline measures

Measure: Throughput
How it is achieved: Batching and compression
How to show it: docker compose run --rm producer python scripts/replay.py /data/recording.jsonl --loops 20

Measure: Latency
How it is achieved: Each trade carries its exchange time, so the real delay is measured (p50/p95/p99)
How to show it: Pipeline health tab, or the processor logs

Measure: Scalability
How it is achieved: 6 partitions keyed by coin + consumer group
How to show it: docker compose up -d --scale processor=3

Measure: Reliability
How it is achieved: Kafka stores everything, exactly-once transactions, automatic restart and recovery
How to show it: Section 6

Measure: Accuracy
How it is achieved: Validation, duplicate removal, late data correction, gap counting
How to show it: Section 7

6. What happens if something crashes?

Kafka keeps all messages for 24 hours, so nothing is lost while a part is down.

If Producer crashes: It remembers the last trade Kafka confirmed and downloads the trades it missed from the exchange's REST API, then continues live.
If Processor crashes: It continues exactly where it stopped and re-reads the last 10 minutes from Kafka to rebuild its memory.
If Sink or website crashes: It writes everything that waited in Kafka - the chart has no gap.
If Dashboard crashes: Data is stored in a Docker volume, so all history is still there.

Demo:

1. docker compose stop producer
2. Wait 60 s
3. docker compose start producer
4. docker compose logs producer (shows backfilled N missed trades)
5. What if the producer sends data twice?

Four layers of protection:

1. Kafka idempotent producer - ignores resent messages after network errors
2. Producer filter - does not send a trade it already sent
3. Processor - drops any trade with an already seen ID (also after a restart)
4. Database - repeated writes overwrite the same row instead of adding a new one

Demo:

1. docker compose up -d --scale producer=2 (every trade is sent twice; Duplicates blocked rises, but prices, volumes and alerts stay the same)
2. Back to normal: docker compose up -d --scale producer=1
3. Project structure

crypto-surveillance/

* docker-compose.yml: all services
* Dockerfile: Python image
* requirements.txt: Python libraries
* src/streaming/: producer, processor, detector, sink, exchange adapters
* dashboard/app.py: website
* scripts/: topic setup and load test
* tests/: unit tests
* docs/PRESENTATION.md: demo script
* PROMPTS.md: AI prompts used

9. Limitations

* One Kafka broker (fine for a laptop; production would use 3).
* After a long outage, exchanges only return a limited history, so some trades may not be recoverable (shown as unrecovered feed gaps).
* Alerts are rule-based; machine learning could be added later.
