"""
Async WebSocket feed handlers for Binance and Bybit.
"""
from __future__ import annotations

import asyncio
import time
import logging
from abc import ABC, abstractmethod
from typing import List, Callable, Optional, Dict
import orjson

from .orderbook import L2OrderBook
from .normalizer import (
    normalize_binance_depth,
    normalize_bybit_depth,
    normalize_binance_trade,
    normalize_bybit_trade,
)
from .monitor import LatencyMonitor

logger = logging.getLogger(__name__)


class ExchangeFeed(ABC):
    """Abstract base class for exchange WebSocket feeds."""

    def __init__(self, symbols: List[str], depth: int = 50,
                 on_book_update: Optional[Callable] = None,
                 on_trade: Optional[Callable] = None):
        self.symbols = symbols
        self.depth   = depth
        self.on_book_update = on_book_update
        self.on_trade = on_trade
        self.books: Dict[str, L2OrderBook] = {
            s: L2OrderBook(s, depth) for s in symbols
        }
        self.monitor  = LatencyMonitor()
        self._running = False
        self._ws      = None

    @property
    @abstractmethod
    def ws_url(self) -> str: ...

    @abstractmethod
    def build_subscribe_message(self) -> dict: ...

    @abstractmethod
    def process_message(self, msg: dict): ...

    async def connect(self):
        """Connect to WebSocket feed with auto-reconnect."""
        import websockets

        reconnect_delay = 1.0
        max_reconnects  = 10
        n_reconnects    = 0

        while self._running and n_reconnects < max_reconnects:
            try:
                async with websockets.connect(
                    self.ws_url,
                    ping_interval=20,
                    ping_timeout=10,
                    max_size=10 * 1024 * 1024,
                ) as ws:
                    self._ws = ws
                    n_reconnects = 0
                    reconnect_delay = 1.0

                    # Subscribe
                    sub_msg = self.build_subscribe_message()
                    await ws.send(orjson.dumps(sub_msg).decode())

                    async for raw in ws:
                        if not self._running:
                            break
                        try:
                            msg = orjson.loads(raw)
                            self.process_message(msg)
                        except Exception as e:
                            logger.warning(f"Parse error: {e}")

            except Exception as e:
                if self._running:
                    logger.warning(f"WebSocket disconnected: {e}. "
                                   f"Reconnecting in {reconnect_delay:.1f}s...")
                    n_reconnects += 1
                    await asyncio.sleep(reconnect_delay)
                    reconnect_delay = min(reconnect_delay * 2, 60)

    async def start(self):
        """Start feed (non-blocking)."""
        self._running = True
        asyncio.create_task(self.connect())

    async def stop(self):
        """Stop feed."""
        self._running = False
        if self._ws:
            await self._ws.close()

    def get_book(self, symbol: str) -> Optional[L2OrderBook]:
        return self.books.get(symbol)


class BinanceFeed(ExchangeFeed):
    """
    Binance WebSocket market data feed.
    Subscribes to <symbol>@depth<levels>@100ms streams.
    Binance supports depth 5, 10, or 20 only.
    """

    BASE_WS = "wss://stream.binance.com:9443/stream"
    MAX_DEPTH = 20  # Binance limit for partial book depth

    @property
    def ws_url(self) -> str:
        depth = min(self.depth, self.MAX_DEPTH)
        streams = "/".join(
            f"{s.lower()}@depth{depth}@100ms/{s.lower()}@trade"
            for s in self.symbols
        )
        return f"{self.BASE_WS}?streams={streams}"

    def build_subscribe_message(self) -> dict:
        return {}  # URL-based subscription for Binance

    def process_message(self, msg: dict):
        data = msg.get("data", msg)

        trade_norm = normalize_binance_trade(data)
        if trade_norm and self.on_trade:
            self.on_trade(trade_norm)
            return

        norm = normalize_binance_depth(data)
        if not norm:
            return

        sym  = norm["symbol"]
        book = self.books.get(sym)
        if not book:
            return

        ts_ns = time.monotonic_ns()
        if norm["type"] == "snapshot":
            book.apply_snapshot(norm["bids"], norm["asks"],
                                 norm["update_id"], norm["ts_ms"])
        else:
            book.apply_diff(norm["bids"], norm["asks"],
                             norm["update_id"], norm["ts_ms"])

        self.monitor.record(sym, norm["ts_ms"], ts_ns)
        if self.on_book_update:
            self.on_book_update(book)


class BybitFeed(ExchangeFeed):
    """
    Bybit V5 WebSocket market data feed.
    """

    BASE_WS = "wss://stream.bybit.com/v5/public/spot"

    @property
    def ws_url(self) -> str:
        return self.BASE_WS

    def build_subscribe_message(self) -> dict:
        args = [f"orderbook.{self.depth}.{s}" for s in self.symbols]
        args.extend(f"publicTrade.{s}" for s in self.symbols)
        return {"op": "subscribe", "args": args}

    def process_message(self, msg: dict):
        trade_norm = normalize_bybit_trade(msg)
        if trade_norm and self.on_trade:
            self.on_trade(trade_norm)
            return

        norm = normalize_bybit_depth(msg)
        if not norm:
            return

        sym  = norm["symbol"]
        book = self.books.get(sym)
        if not book:
            return

        ts_ns = time.monotonic_ns()
        if norm["type"] == "snapshot":
            book.apply_snapshot(norm["bids"], norm["asks"],
                                 norm["update_id"], norm["ts_ms"])
        else:
            book.apply_diff(norm["bids"], norm["asks"],
                             norm["update_id"], norm["ts_ms"])

        self.monitor.record(sym, norm["ts_ms"], ts_ns)
        if self.on_book_update:
            self.on_book_update(book)
