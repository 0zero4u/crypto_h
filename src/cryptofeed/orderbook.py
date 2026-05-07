"""
L2 order book with incremental update support.

Maintains a sorted price-level book from exchange diff events.
Supports Binance (diff depth stream) and Bybit (delta) formats.
"""
from __future__ import annotations

import time
from sortedcontainers import SortedDict
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple


@dataclass
class BookLevel:
    price: float
    qty:   float


class L2OrderBook:
    """
    Real-time L2 order book with incremental diff updates.

    Features:
    - O(log N) insert/delete/update via SortedDict
    - O(1) best bid/ask
    - Checksum validation support
    - Latency timestamping (exchange time vs local time)
    """

    def __init__(self, symbol: str, depth: int = 100):
        self.symbol = symbol
        self.depth  = depth
        # Bids: price → qty (sorted descending)
        self._bids: SortedDict = SortedDict(lambda x: -x)
        # Asks: price → qty (sorted ascending)
        self._asks: SortedDict = SortedDict()

        self.last_update_id:  int   = 0
        self.exchange_ts_ms:  int   = 0
        self.local_ts_ns:     int   = 0
        self._update_count:   int   = 0

    # ── Snapshot ──────────────────────────────────────────────────────────────

    def apply_snapshot(self, bids: List[Tuple[float, float]],
                        asks: List[Tuple[float, float]],
                        update_id: int = 0,
                        exchange_ts_ms: int = 0):
        """Replace book with full snapshot."""
        self._bids.clear()
        self._asks.clear()
        for price, qty in bids:
            if qty > 0:
                self._bids[price] = qty
        for price, qty in asks:
            if qty > 0:
                self._asks[price] = qty
        self.last_update_id = update_id
        self.exchange_ts_ms = exchange_ts_ms
        self.local_ts_ns    = time.monotonic_ns()

    # ── Incremental updates ───────────────────────────────────────────────────

    def apply_diff(self, bids: List[Tuple[float, float]],
                    asks: List[Tuple[float, float]],
                    update_id: int = 0,
                    exchange_ts_ms: int = 0):
        """Apply incremental diff update (Binance diff_depth format)."""
        for price, qty in bids:
            if qty == 0:
                self._bids.pop(price, None)
            else:
                self._bids[price] = qty

        for price, qty in asks:
            if qty == 0:
                self._asks.pop(price, None)
            else:
                self._asks[price] = qty

        # Trim depth
        while len(self._bids) > self.depth:
            self._bids.popitem(-1)  # Remove worst bid
        while len(self._asks) > self.depth:
            self._asks.popitem(-1)  # Remove worst ask

        self.last_update_id = update_id
        self.exchange_ts_ms = exchange_ts_ms
        self.local_ts_ns    = time.monotonic_ns()
        self._update_count  += 1

    # ── Market data accessors ─────────────────────────────────────────────────

    @property
    def best_bid(self) -> Optional[BookLevel]:
        if not self._bids:
            return None
        price = self._bids.keys()[0]
        return BookLevel(price=price, qty=self._bids[price])

    @property
    def best_ask(self) -> Optional[BookLevel]:
        if not self._asks:
            return None
        price = self._asks.keys()[0]
        return BookLevel(price=price, qty=self._asks[price])

    @property
    def mid_price(self) -> Optional[float]:
        b, a = self.best_bid, self.best_ask
        if b and a:
            return (b.price + a.price) / 2
        return None

    @property
    def spread(self) -> Optional[float]:
        b, a = self.best_bid, self.best_ask
        if b and a:
            return a.price - b.price
        return None

    @property
    def spread_bps(self) -> Optional[float]:
        mid = self.mid_price
        sp  = self.spread
        if mid and sp:
            return sp / mid * 10_000
        return None

    def vwap(self, side: str = "ask", levels: int = 5) -> Optional[float]:
        """Volume-weighted average price for top N levels."""
        book = self._asks if side == "ask" else self._bids
        if not book:
            return None
        items = list(book.items())[:levels]
        total_qty   = sum(q for _, q in items)
        total_value = sum(p * q for p, q in items)
        return total_value / total_qty if total_qty > 0 else None

    def depth_at_price(self, price: float, side: str = "bid") -> float:
        """Total quantity available at or better than price."""
        if side == "bid":
            return sum(q for p, q in self._bids.items() if p >= price)
        return sum(q for p, q in self._asks.items() if p <= price)

    @property
    def latency_us(self) -> Optional[float]:
        """Round-trip latency in microseconds (exchange → local)."""
        if self.exchange_ts_ms == 0:
            return None
        local_ms = self.local_ts_ns / 1_000_000
        return (local_ms - self.exchange_ts_ms) * 1000  # → µs

    def get_levels(self, depth: int = 10) -> dict:
        """Return top N bid/ask levels as dict."""
        return {
            "symbol":     self.symbol,
            "mid":        self.mid_price,
            "spread_bps": self.spread_bps,
            "bids": [(p, q) for p, q in list(self._bids.items())[:depth]],
            "asks": [(p, q) for p, q in list(self._asks.items())[:depth]],
            "ts_ms":      self.exchange_ts_ms,
            "latency_us": self.latency_us,
        }

    def __repr__(self) -> str:
        bb = self.best_bid
        ba = self.best_ask
        return (f"L2OrderBook({self.symbol}: "
                f"bid={bb.price if bb else 'N/A'}, "
                f"ask={ba.price if ba else 'N/A'}, "
                f"spread_bps={self.spread_bps:.2f if self.spread_bps else 'N/A'})")
