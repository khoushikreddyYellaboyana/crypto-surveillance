"""Exchange adapters. Each adapter turns a live public WebSocket feed into canonical trades."""
from .binance import BinanceSource
from .coinbase import CoinbaseSource

REGISTRY = {"coinbase": CoinbaseSource, "binance": BinanceSource}
