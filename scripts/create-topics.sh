#!/bin/bash
# Creates all topics (idempotent). Runs inside the apache/kafka container.
set -e
BS=${BOOTSTRAP:-kafka:29092}
K=/opt/kafka/bin/kafka-topics.sh

create () {  # name partitions extra-configs...
  local name=$1 parts=$2; shift 2
  $K --bootstrap-server "$BS" --create --if-not-exists --topic "$name" \
     --partitions "$parts" --replication-factor 1 "$@"
}

# raw trades: 6 partitions keyed by symbol -> up to 6 parallel processor instances
create market.trades.raw   6 --config retention.ms=86400000  --config compression.type=lz4
create market.bars.5s      3 --config retention.ms=604800000
create market.alerts       3 --config retention.ms=2592000000
create pipeline.metrics    1 --config retention.ms=604800000
create market.trades.dlq   1 --config retention.ms=604800000

$K --bootstrap-server "$BS" --list
echo "topics ready"
