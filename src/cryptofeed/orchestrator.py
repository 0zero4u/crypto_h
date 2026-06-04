"""
Main orchestrator for cross-exchange divergence strategy.

Connects BinanceFeed, BybitFeed, (optionally) GateIoFeed, and (optionally) DeltaFeed
to the strategy pipeline:
  Book updates → DWMP → GlobalFairValue → Divergence → ZScore → Signal
  Trade updates → TradeCollector → volume weighting
"""
from __future__ import annotations

import asyncio
import logging
from typing import List, Optional, Callable, Dict

from .feed import BinanceFeed, BybitFeed, GateIoFeed, DeltaFeed
from .orderbook import L2OrderBook
from .strategy import (
    Trade, TradeCollector, GlobalFairValue,
    DivergenceTracker, ZScoreSignal, Signal,
)

logger = logging.getLogger(__name__)


class DivergenceOrchestrator:
    """
    Orchestrates the cross-exchange divergence strategy.

    Architecture:
        Binance WS ──► BinanceFeed ──► on_book_update ──► _handle_book
        Bybit WS   ──► BybitFeed   ──► on_book_update ──► _handle_book
        Gate.io WS ──► GateIoFeed  ──► on_book_update ──► _handle_book (optional)
        Delta WS   ──► DeltaFeed   ──► on_book_update ──► _handle_book (optional)
                                     ──► on_trade       ──► _handle_trade
                                        │
                                        ▼
                                    TradeCollector (1-min volume)
                                        │
                                        ▼
                                    GlobalFairValue (volume-weighted DWMP)
                                        │
                                        ▼
                                    DivergenceTracker (5-min baseline)
                                        │
                                        ▼
                                    ZScoreSignal (threshold=3)
                                        │
                                        ▼
                                    on_signal callback
    """

    def __init__(
        self,
        symbols: List[str],
        depth: int = 20,
        n_levels: int = 20,
        volume_window_seconds: int = 60,
        divergence_window_minutes: int = 3,
        z_threshold: float = 2.0,
        min_divergence_pct: float = 0.02,
        on_signal: Optional[Callable[[Signal], None]] = None,
        use_gateio: bool = False,
        use_delta: bool = False,
        delta_symbols: Optional[List[str]] = None,
    ):
        self.symbols = symbols
        self.depth = depth
        self.n_levels = n_levels
        self.on_signal = on_signal
        self.use_gateio = use_gateio
        self.use_delta = use_delta

        # Strategy components
        self.trade_collector = TradeCollector(window_seconds=volume_window_seconds)
        self.gfv = GlobalFairValue(self.trade_collector)
        self.divergence_tracker = DivergenceTracker(window_minutes=divergence_window_minutes)
        self.z_score = ZScoreSignal(threshold=z_threshold, min_divergence_pct=min_divergence_pct)

        # Exchange feeds - Binance/Bybit/Gate.io use same symbols (e.g. BTCUSDT)
        self.binance_feed = BinanceFeed(
            symbols=symbols,
            depth=depth,
            on_book_update=self._handle_book,
            on_trade=self._handle_trade,
        )
        self.bybit_feed = BybitFeed(
            symbols=symbols,
            depth=depth,
            on_book_update=self._handle_book,
            on_trade=self._handle_trade,
        )
        self.gateio_feed: Optional[GateIoFeed] = None
        if use_gateio:
            self.gateio_feed = GateIoFeed(
                symbols=symbols,
                depth=depth,
                on_book_update=self._handle_book,
                on_trade=self._handle_trade,
            )

        # Delta uses different symbols (e.g. BTCUSD instead of BTCUSDT)
        self.delta_feed: Optional[DeltaFeed] = None
        if use_delta:
            delta_syms = delta_symbols or symbols
            self.delta_feed = DeltaFeed(
                symbols=delta_syms,
                depth=depth,
                on_book_update=self._handle_book,
                on_trade=self._handle_trade,
            )

        # Latest DWMPs per exchange
        self._dwmps: Dict[str, float] = {}

    def _handle_trade(self, trade_data: dict):
        """Process incoming trade from any exchange."""
        trade = Trade(
            symbol=trade_data["symbol"],
            exchange=trade_data["exchange"],
            price=trade_data["price"],
            qty=trade_data["qty"],
            ts_ms=trade_data["ts_ms"],
            side=trade_data["side"],
            volume=trade_data.get("volume", 0.0),
        )
        self.trade_collector.add_trade(trade)

    def _handle_book(self, book: L2OrderBook):
        """Process order book update — main strategy pipeline."""
        exchange = self._detect_exchange(book)
        if not exchange:
            return

        # Step 1: Compute DWMP for this exchange
        dwmp = book.dwmp(n_levels=self.n_levels)
        if dwmp is None:
            return

        self._dwmps[exchange] = dwmp
        self.gfv.update_dwmp(exchange, dwmp)

        # Step 2: Compute Global Fair Value
        result = self.gfv.compute()
        if result is None:
            return

        gfv, weights = result

        # Step 3: Compute divergence for this exchange
        divergence_pct = (dwmp - gfv) / gfv * 100 if gfv != 0 else 0.0
        ts_ms = book.exchange_ts_ms or int(__import__('time').time() * 1000)
        self.divergence_tracker.record(exchange, divergence_pct, ts_ms)

        # Step 4: Get rolling baseline stats
        stats = self.divergence_tracker.get_stats(exchange)
        if stats is None:
            return

        mean, std = stats

        # Step 5: Evaluate z-score signal
        signal = self.z_score.evaluate(
            exchange=exchange,
            dwmp=dwmp,
            gfv=gfv,
            divergence_pct=divergence_pct,
            mean=mean,
            std=std,
            ts_ms=ts_ms,
        )

        if signal:
            logger.info(
                f"SIGNAL: {signal.direction.upper()} {exchange} | "
                f"Z={signal.z_score:.2f} D={signal.divergence_pct:.4f}% | "
                f"DWMP={signal.dwmp:.2f} GFV={signal.gfv:.2f}"
            )
            if self.on_signal:
                self.on_signal(signal)

    def _detect_exchange(self, book: L2OrderBook) -> Optional[str]:
        """Detect which exchange a book belongs to based on feed ownership."""
        for sym, b in self.binance_feed.books.items():
            if b is book:
                return "binance"
        for sym, b in self.bybit_feed.books.items():
            if b is book:
                return "bybit"
        if self.gateio_feed:
            for sym, b in self.gateio_feed.books.items():
                if b is book:
                    return "gateio"
        if self.delta_feed:
            for sym, b in self.delta_feed.books.items():
                if b is book:
                    return "delta"
        return None

    async def start(self):
        """Start all exchange feeds."""
        logger.info(f"Starting divergence orchestrator for {self.symbols}")
        await self.binance_feed.start()
        await self.bybit_feed.start()
        if self.gateio_feed:
            await self.gateio_feed.start()
        if self.delta_feed:
            await self.delta_feed.start()

    async def stop(self):
        """Stop all exchange feeds."""
        logger.info("Stopping divergence orchestrator")
        for feed in [self.binance_feed, self.bybit_feed,
                     self.gateio_feed, self.delta_feed]:
            if feed:
                try:
                    await asyncio.wait_for(feed.stop(), timeout=2)
                except asyncio.TimeoutError:
                    pass

    def get_state(self) -> dict:
        """Get current strategy state for monitoring."""
        result = self.gfv.compute()
        gfv = result[0] if result else None

        return {
            "symbols": self.symbols,
            "dwmps": dict(self._dwmps),
            "gfv": gfv,
            "volumes": self.trade_collector.get_volumes(),
            "divergence_stats": {
                ex: self.divergence_tracker.get_stats(ex)
                for ex in self._dwmps
            },
        }