"""
Async WebSocket feed handlers for Binance, Bybit, and Gate.io.
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
    normalize_delta_ob,
    normalize_delta_trade,
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
                    logger.warning(f"{self.__class__.__name__} WebSocket disconnected: {e}. "
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
    Binance futures WebSocket market data feed.
    Requires TWO connections: /market for aggTrade, /public for depth.
    Uses depth5@0ms for real-time top 5 level updates.
    """

    BASE_WS = "wss://fstream.binance.com"
    MAX_DEPTH = 5

    def __init__(self, symbols, depth=5, on_book_update=None, on_trade=None):
        super().__init__(symbols, depth, on_book_update, on_trade)
        self._trade_ws = None
        self._depth_ws = None
        self._tasks = []

    @property
    def _trade_url(self) -> str:
        streams = "/".join(f"{s.lower()}@trade" for s in self.symbols)
        return f"{self.BASE_WS}/public/stream?streams={streams}"

    @property
    def _depth_url(self) -> str:
        depth = min(self.depth, self.MAX_DEPTH)
        streams = "/".join(f"{s.lower()}@depth{depth}@0ms" for s in self.symbols)
        return f"{self.BASE_WS}/public/stream?streams={streams}"

    @property
    def ws_url(self) -> str:
        return self._depth_url

    def build_subscribe_message(self) -> dict:
        return {}

    def process_message(self, msg: dict):
        pass

    async def stop(self):
        self._running = False

    async def connect(self):
        import websockets

        while self._running:
            try:
                self._depth_ws = await websockets.connect(
                    self._depth_url, ping_interval=20, ping_timeout=10,
                    max_size=10 * 1024 * 1024)
                self._trade_ws = await websockets.connect(
                    self._trade_url, ping_interval=20, ping_timeout=10,
                    max_size=10 * 1024 * 1024)

                depth_task = asyncio.create_task(
                    self._process_stream(self._depth_ws, is_trade=False))
                trade_task = asyncio.create_task(
                    self._process_stream(self._trade_ws, is_trade=True))
                self._tasks = [depth_task, trade_task]

                await asyncio.gather(*self._tasks)

            except Exception as e:
                if self._running:
                    logger.warning(f"BinanceFeed disconnected: {e}. Reconnecting...")
                    await asyncio.sleep(2)

    async def _process_stream(self, ws, is_trade: bool):
        import websockets
        try:
            async for raw in ws:
                if not self._running:
                    break
                try:
                    msg = orjson.loads(raw)
                    if is_trade:
                        trade_norm = normalize_binance_trade(msg)
                        if trade_norm and self.on_trade:
                            self.on_trade(trade_norm)
                    else:
                        data = msg.get("data", msg)
                        self._handle_depth(data)
                except Exception as e:
                    logger.warning(f"Binance parse error: {e}")
        except websockets.exceptions.ConnectionClosed:
            pass

    def _handle_depth(self, data: dict):
        norm = normalize_binance_depth(data)
        if not norm:
            return

        sym = norm["symbol"]
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
    Bybit V5 WebSocket market data feed (USDT perpetual / futures).
    Valid orderbook levels: 1, 50, 200, 500
    """

    BASE_WS = "wss://stream.bybit.com/v5/public/linear"
    VALID_DEPTHS = [1, 50, 200, 500]

    @property
    def ws_url(self) -> str:
        return self.BASE_WS

    def _bybit_depth(self) -> int:
        for d in self.VALID_DEPTHS:
            if d >= self.depth:
                return d
        return 500

    def build_subscribe_message(self) -> dict:
        depth = self._bybit_depth()
        args = [f"orderbook.{depth}.{s}" for s in self.symbols]
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


class GateIoFeed(ExchangeFeed):
    """
    Gate.io futures WebSocket market data feed.

    Unlike Binance/Bybit, Gate.io does NOT send a full snapshot on connect.
    We must:
    1. Subscribe to WebSocket and buffer deltas
    2. Fetch REST snapshot with with_id=true
    3. Replay buffered deltas that connect to the snapshot
    4. Then continue with live deltas
    """

    BASE_WS = "wss://fx-ws.gateio.ws/v4/ws/usdt"
    REST_BASE = "https://fx-api.gateio.ws/api/v4/futures/usdt/order_book"
    CONTRACTS_API = "https://api.gateio.ws/api/v4/futures/usdt/contracts"
    MAX_DEPTH = 20

    def __init__(self, symbols: List[str], depth: int = 20,
                 on_book_update: Optional[Callable] = None,
                 on_trade: Optional[Callable] = None):
        from collections import deque
        super().__init__(symbols, depth, on_book_update, on_trade)
        self._delta_buffer: Dict[str, deque] = {s: deque() for s in symbols}
        self._snapshot_ids: Dict[str, int] = {}
        self._synced: Dict[str, bool] = {s: False for s in symbols}
        self._trade_ws = None
        self._depth_ws = None
        self._contract_sizes: Dict[str, float] = {}

    def _gate_symbol(self, symbol: str) -> str:
        if symbol.endswith("USDT"):
            return symbol[:-4] + "_USDT"
        return symbol

    async def _fetch_contract_sizes(self):
        """Fetch contract sizes (quanto_multiplier) from Gate.io API."""
        import aiohttp
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(self.CONTRACTS_API) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        for contract in data:
                            name = contract.get("name", "")
                            multiplier = contract.get("quanto_multiplier", "0")
                            if name and multiplier:
                                self._contract_sizes[name] = float(multiplier)
                        logger.info(f"Gate.io: loaded {len(self._contract_sizes)} contract sizes")
                    else:
                        logger.warning(f"Gate.io: failed to fetch contract sizes: HTTP {resp.status}")
        except Exception as e:
            logger.warning(f"Gate.io: failed to fetch contract sizes: {e}")

    def get_contract_size(self, gate_symbol: str) -> float:
        """Get contract size for a symbol, fallback to 0.0001 (BTC default)."""
        return self._contract_sizes.get(gate_symbol, 0.0001)

    @property
    def ws_url(self) -> str:
        return self.BASE_WS

    def build_subscribe_message(self) -> dict:
        return None

    async def connect(self):
        import websockets

        # Fetch contract sizes on startup
        await self._fetch_contract_sizes()

        reconnect_delay = 1.0
        max_reconnects = 10
        n_reconnects = 0

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

                    for symbol in self.symbols:
                        gate_sym = self._gate_symbol(symbol)

                        depth = min(self.depth, self.MAX_DEPTH)
                        freq = "20ms" if depth <= 20 else "100ms"
                        sub_msg = {
                            "time": int(time.time()),
                            "channel": "futures.order_book_update",
                            "event": "subscribe",
                            "payload": [gate_sym, freq, str(depth)],
                        }
                        await ws.send(orjson.dumps(sub_msg).decode())

                        trade_msg = {
                            "time": int(time.time()),
                            "channel": "futures.trades",
                            "event": "subscribe",
                            "payload": [gate_sym],
                        }
                        await ws.send(orjson.dumps(trade_msg).decode())

                    await self._fetch_snapshots()

                    async for raw in ws:
                        if not self._running:
                            break
                        try:
                            msg = orjson.loads(raw)
                            self.process_message(msg)
                        except Exception as e:
                            logger.warning(f"Gate.io parse error: {e}")

            except Exception as e:
                if self._running:
                    logger.warning(f"Gate.io WebSocket disconnected: {e}. "
                                   f"Reconnecting in {reconnect_delay:.1f}s...")
                    n_reconnects += 1
                    await asyncio.sleep(reconnect_delay)
                    reconnect_delay = min(reconnect_delay * 2, 60)

    async def _fetch_snapshots(self):
        import aiohttp
        async with aiohttp.ClientSession() as session:
            for symbol in self.symbols:
                gate_sym = self._gate_symbol(symbol)
                url = f"{self.REST_BASE}?contract={gate_sym}&limit={self.depth}&with_id=true"

                try:
                    async with session.get(url) as resp:
                        if resp.status == 200:
                            data = await resp.json()
                            snapshot_id = data.get("id", 0)
                            self._snapshot_ids[symbol] = snapshot_id

                            from .normalizer import normalize_gateio_snapshot
                            norm = normalize_gateio_snapshot(data, gate_sym)
                            if norm:
                                book = self.books.get(symbol)
                                if book:
                                    book.apply_snapshot(
                                        norm["bids"], norm["asks"],
                                        norm["update_id"], norm["ts_ms"]
                                    )
                                    logger.info(f"Gate.io {symbol} snapshot loaded, "
                                               f"id={snapshot_id}")

                            self._replay_buffered(symbol, snapshot_id)
                            self._synced[symbol] = True
                        else:
                            logger.error(f"Gate.io snapshot failed for {symbol}: "
                                       f"HTTP {resp.status}")
                except Exception as e:
                    logger.error(f"Gate.io snapshot error for {symbol}: {e}")

    def _replay_buffered(self, symbol: str, snapshot_id: int):
        buffer = self._delta_buffer[symbol]
        book = self.books.get(symbol)
        if not book:
            return

        applied = 0
        while buffer:
            norm = buffer[0]
            first_id = norm.get("first_id", 0)
            last_id = norm.get("update_id", 0)

            if last_id < snapshot_id:
                buffer.popleft()
                continue

            if first_id <= snapshot_id + 1:
                buffer.popleft()
                book.apply_diff(norm["bids"], norm["asks"],
                               norm["update_id"], norm["ts_ms"])
                applied += 1
            else:
                logger.warning(f"Gate.io {symbol}: gap in buffered deltas, "
                             f"expected <= {snapshot_id + 1}, got {first_id}")
                break

        logger.info(f"Gate.io {symbol}: replayed {applied} buffered deltas")

    def process_message(self, msg: dict):
        channel = msg.get("channel", "")
        event = msg.get("event", "")

        if event == "subscribe":
            return

        if channel == "futures.trades":
            from .normalizer import normalize_gateio_trade
            gate_sym = msg.get("result", [{}])[0].get("contract", "")
            contract_size = self.get_contract_size(gate_sym)
            trade_norm = normalize_gateio_trade(msg, contract_size)
            if trade_norm and self.on_trade:
                self.on_trade(trade_norm)
            return

        if channel == "futures.order_book_update" and event == "update":
            from .normalizer import normalize_gateio_depth
            norm = normalize_gateio_depth(msg)
            if not norm:
                return

            symbol = norm["symbol"]
            original_symbol = None
            for s in self.books:
                if s.replace("_", "") == symbol or s == symbol:
                    original_symbol = s
                    break

            if not original_symbol:
                return

            if not self._synced.get(original_symbol, False):
                self._delta_buffer[original_symbol].append(norm)
                return

            book = self.books.get(original_symbol)
            if not book:
                return

            last_id = self._snapshot_ids.get(original_symbol, 0)
            first_id = norm.get("first_id", 0)

            if first_id > last_id + 1:
                logger.warning(f"Gate.io {original_symbol}: sequence gap, "
                             f"resyncing (expected {last_id + 1}, got {first_id})")
                self._synced[original_symbol] = False
                self._delta_buffer[original_symbol].clear()
                asyncio.create_task(self._resync_symbol(original_symbol))
                return

            book.apply_diff(norm["bids"], norm["asks"],
                           norm["update_id"], norm["ts_ms"])
            self._snapshot_ids[original_symbol] = norm["update_id"]

            ts_ns = time.monotonic_ns()
            self.monitor.record(original_symbol, norm["ts_ms"], ts_ns)
            if self.on_book_update:
                self.on_book_update(book)

    async def _resync_symbol(self, symbol: str):
        import aiohttp
        gate_sym = self._gate_symbol(symbol)
        url = f"{self.REST_BASE}?contract={gate_sym}&limit={self.depth}&with_id=true"

        async with aiohttp.ClientSession() as session:
            try:
                async with session.get(url) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        snapshot_id = data.get("id", 0)

                        from .normalizer import normalize_gateio_snapshot
                        norm = normalize_gateio_snapshot(data, gate_sym)
                        if norm:
                            book = self.books.get(symbol)
                            if book:
                                book.apply_snapshot(
                                    norm["bids"], norm["asks"],
                                    norm["update_id"], norm["ts_ms"]
                                )

                        self._snapshot_ids[symbol] = snapshot_id
                        self._replay_buffered(symbol, snapshot_id)
                        self._synced[symbol] = True
                        logger.info(f"Gate.io {symbol} resynced, new id={snapshot_id}")
            except Exception as e:
                logger.error(f"Gate.io resync error for {symbol}: {e}")


class DeltaFeed(ExchangeFeed):
    """
    Delta Exchange WebSocket market data feed.

    Uses trades channel only for fair-value computation (last trade price).
    ob_l1 removed — it updates at ~276ms average (not 100ms as documented),
    causing stale fair values and false divergence signals.

    Endpoint: wss://public-socket.india.delta.exchange
    """

    BASE_WS = "wss://public-socket.india.delta.exchange"

    def __init__(self, symbols: List[str], depth: int = 1,
                 on_book_update: Optional[Callable] = None,
                 on_trade: Optional[Callable] = None):
        super().__init__(symbols, depth, on_book_update, on_trade)

    @property
    def ws_url(self) -> str:
        return self.BASE_WS

    def build_subscribe_message(self) -> dict:
        return {
            "type": "subscribe",
            "payload": {
                "channels": [
                    {"name": "trades", "symbols": self.symbols},
                ]
            }
        }

    def process_message(self, msg: dict):
        msg_type = msg.get("type")

        if msg_type == "trades":
            trade_norm = normalize_delta_trade(msg)
            if trade_norm and self.on_trade:
                self.on_trade(trade_norm)
            return
