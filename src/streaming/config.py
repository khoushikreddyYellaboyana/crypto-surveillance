"""Central configuration. Every value can be overridden with an environment variable."""
import os


def _env(name: str, default: str) -> str:
    return os.getenv(name, default)


def _env_float(name: str, default: float) -> float:
    return float(os.getenv(name, default))


def _env_int(name: str, default: int) -> int:
    return int(os.getenv(name, default))


# ---- Kafka -----------------------------------------------------------------
KAFKA_BOOTSTRAP = _env("KAFKA_BOOTSTRAP", "localhost:9092")

TOPIC_RAW = _env("TOPIC_RAW", "market.trades.raw")          # normalized trades from exchanges
TOPIC_BARS = _env("TOPIC_BARS", "market.bars.5s")           # 5-second OHLCV bars
TOPIC_ALERTS = _env("TOPIC_ALERTS", "market.alerts")        # anomaly / surveillance alerts
TOPIC_METRICS = _env("TOPIC_METRICS", "pipeline.metrics")   # throughput / latency / quality metrics
TOPIC_DLQ = _env("TOPIC_DLQ", "market.trades.dlq")          # malformed or rejected messages

# ---- Data sources (real, public, no API key) --------------------------------
SOURCES = [s.strip() for s in _env("SOURCES", "coinbase,binance").split(",") if s.strip()]
# Canonical symbols. Each exchange adapter maps them to its own naming.
SYMBOLS = [s.strip() for s in _env("SYMBOLS", "BTC-USD,ETH-USD,SOL-USD,XRP-USD,DOGE-USD").split(",") if s.strip()]

COINBASE_WS_URL = _env("COINBASE_WS_URL", "wss://ws-feed.exchange.coinbase.com")
# data-stream.binance.vision is Binance's market-data-only endpoint (no account, works in the EU).
BINANCE_WS_URL = _env("BINANCE_WS_URL", "wss://data-stream.binance.vision/stream")

# ---- Stream processing -----------------------------------------------------
BAR_SECONDS = _env_int("BAR_SECONDS", 5)
ALLOWED_LATENESS_MS = _env_int("ALLOWED_LATENESS_MS", 3000)   # event-time lateness tolerance
IDLE_FLUSH_MS = _env_int("IDLE_FLUSH_MS", 10000)              # close bars even if a symbol goes quiet
LATE_CORRECTION_MS = _env_int("LATE_CORRECTION_MS", 3_600_000)  # late trades up to 1 h old correct their bar

WHALE_MIN_USD = _env_float("WHALE_MIN_USD", 250_000)          # absolute floor for a "whale" trade
WHALE_MEDIAN_MULT = _env_float("WHALE_MEDIAN_MULT", 50)       # ...and must be N x the rolling median
SPIKE_Z = _env_float("SPIKE_Z", 5.0)                          # z-score of a bar return
SPIKE_MIN_BPS = _env_float("SPIKE_MIN_BPS", 15)               # ignore tiny moves even if z is high
BURST_Z = _env_float("BURST_Z", 5.0)                          # z-score of trades-per-bar
BURST_MIN_TRADES = _env_int("BURST_MIN_TRADES", 50)
DIVERGENCE_BPS = _env_float("DIVERGENCE_BPS", 40)             # cross-exchange price gap
DIVERGENCE_MAX_AGE_MS = _env_int("DIVERGENCE_MAX_AGE_MS", 2000)
WARMUP_BARS = _env_int("WARMUP_BARS", 30)                     # bars before statistical rules fire
ALERT_COOLDOWN_MS = _env_int("ALERT_COOLDOWN_MS", 30000)
DEDUP_CACHE_SIZE = _env_int("DEDUP_CACHE_SIZE", 200_000)

METRICS_INTERVAL_S = _env_float("METRICS_INTERVAL_S", 5)

# ---- Crash recovery ---------------------------------------------------------
# After a (re)start the processor re-reads this much already-processed history from Kafka to
# rebuild its in-memory state (dedup cache, statistics, open bars) before continuing.
RESTORE_WINDOW_MS = _env_int("RESTORE_WINDOW_MS", 10 * 60 * 1000)
# Producer: where it checkpoints the last trade id per exchange/symbol, and whether to fetch
# trades it missed while it was down from the exchanges' public REST APIs.
PRODUCER_STATE_PATH = _env("PRODUCER_STATE_PATH", "data/producer_state.json")
BACKFILL = _env("BACKFILL", "true").lower() == "true"
BACKFILL_MAX_AGE_MS = _env_int("BACKFILL_MAX_AGE_MS", 60 * 60 * 1000)
COINBASE_REST_URL = _env("COINBASE_REST_URL", "https://api.exchange.coinbase.com")
BINANCE_REST_URL = _env("BINANCE_REST_URL", "https://data-api.binance.vision")

# ---- Sink / dashboard -------------------------------------------------------
SQLITE_PATH = _env("SQLITE_PATH", "data/surveillance.db")
