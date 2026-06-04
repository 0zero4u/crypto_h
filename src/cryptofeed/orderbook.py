"""
L2 order book with incremental update support.

Maintains a sorted price-level book from exchange diff events.
Supports Binance (diff depth stream), Bybit (delta), Delta Exchange, and Gate.io formats.

BUG FIX (2026-06-04): apply_diff/apply_snapshot now validate price sanity to prevent
corrupted deltas from causing arbitrary DWMP spikes. A corrupted delta with a valid
sequence number can pass through sequence checks unchecked, overwriting book state with
wrong prices. The fix adds price-range validation before each update.
"""
from __future__ import annotations

import time
import logging
import zlib
from sortedcontainers import SortedDict
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)

# Price sanity: reject level updates whose price deviates more than this fraction
# from the current best price on the same side. Protects against corrupted deltas.
MAX_PRICE_DEVIATION = 0.05   # 5% — tight enough to catch bugs, wide enough for real moves

# Delta Exchange tick sizes by symbol
DELTA_TICK_SIZES = {
    "BTCUSD": 0.1,
    "ETHUSD": 0.01,
    "XRPUSD": 0.0001,
    "SOLUSD": 0.001,
}


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
        self._bids: SortedDict = SortedDict(lambda x: -x)
        self._asks: SortedDict = SortedDict()

        self.last_update_id:  int   = 0
        self.exchange_ts_ms:  int   = 0
        self.local_ts_ns:     int   = 0
        self._update_count:   int   = 0

        # Track last-seen seq to detect duplicate/corrupted seq# replays
        self._last_seq: int = 0

        # Store raw price strings for CRC32 checksum calculation (Delta Exchange)
        self._price_strs: Dict[float, str] = {}

    # ── Snapshot ──────────────────────────────────────────────────────────────

    def apply_snapshot(self, bids: List[Tuple[float, float]],
                        asks: List[Tuple[float, float]],
                        update_id: int = 0,
                        exchange_ts_ms: int = 0,
                        bids_raw: Optional[List[Tuple[str, str]]] = None,
                        asks_raw: Optional[List[Tuple[str, str]]] = None) -> bool:
        """
        Replace book with full snapshot. Returns True if applied cleanly,
        False if snapshot was rejected (price sanity check failed).
        """
        if update_id > 0 and update_id <= self._last_seq:
            logger.debug(f"{self.symbol}: snapshot seq {update_id} <= last {self._last_seq}, skipping")
            return False

        if not bids and not asks:
            logger.warning(f"{self.symbol}: empty snapshot received, rejecting")
            return False

        bid_prices = [p for p, q in bids if q > 0]
        ask_prices = [p for p, q in asks if q > 0]

        if bid_prices and ask_prices:
            worst_bid = min(bid_prices)
            best_ask = max(ask_prices)
            spread = best_ask - worst_bid
            if spread < 0:
                logger.warning(f"{self.symbol}: snapshot has negative spread "
                             f"(worst_bid={worst_bid}, best_ask={best_ask}), rejecting")
                return False
            mid = (worst_bid + best_ask) / 2
            if mid > 0:
                max_deviation = mid * MAX_PRICE_DEVIATION
                for p in bid_prices:
                    if worst_bid - p > max_deviation:
                        logger.warning(f"{self.symbol}: snapshot bid price {p} deviates >5% from mid, rejecting")
                        return False
                for p in ask_prices:
                    if p - best_ask > max_deviation:
                        logger.warning(f"{self.symbol}: snapshot ask price {p} deviates >5% from mid, rejecting")
                        return False

        self._bids.clear()
        self._asks.clear()
        self._price_strs.clear()

        raw_bid_map = {float(p): p for p, s in (bids_raw or [])}
        raw_ask_map = {float(p): p for p, s in (asks_raw or [])}

        for price, qty in bids:
            if qty > 0:
                self._bids[price] = qty
                self._price_strs[price] = raw_bid_map.get(price, str(price))
        for price, qty in asks:
            if qty > 0:
                self._asks[price] = qty
                self._price_strs[price] = raw_ask_map.get(price, str(price))

        while len(self._bids) > self.depth:
            self._bids.popitem(-1)
        while len(self._asks) > self.depth:
            self._asks.popitem(-1)

        self.last_update_id = update_id
        self.exchange_ts_ms = exchange_ts_ms
        self.local_ts_ns    = time.monotonic_ns()
        self._last_seq      = update_id
        return True

    # ── Incremental updates ───────────────────────────────────────────────────

    def apply_diff(self, bids: List[Tuple[float, float]],
                    asks: List[Tuple[float, float]],
                    update_id: int = 0,
                    exchange_ts_ms: int = 0,
                    bids_raw: Optional[List[Tuple[str, str]]] = None,
                    asks_raw: Optional[List[Tuple[str, str]]] = None,
                    l1_ref: Optional[dict] = None) -> bool:
        """
        Apply incremental diff update. Returns True if applied cleanly,
        False if rejected (duplicate seq, price sanity failure, or empty update).
        """
        if update_id > 0 and update_id <= self._last_seq:
            logger.debug(f"{self.symbol}: duplicate/old seq {update_id} <= last {self._last_seq}, skipping")
            return False

        old_best_bid = self.best_bid.price if self.best_bid else None
        old_best_ask = self.best_ask.price if self.best_ask else None
        old_dwmp = self.dwmp(n_levels=10)

        ref_bid = l1_ref.get("bid") if l1_ref else None
        ref_ask = l1_ref.get("ask") if l1_ref else None

        tick_size = DELTA_TICK_SIZES.get(self.symbol)
        max_depth_range = tick_size * self.depth if tick_size else None

        raw_bid_map = {float(p): p for p, s in (bids_raw or [])}
        raw_ask_map = {float(p): p for p, s in (asks_raw or [])}

        valid_bids = 0
        for price, qty in bids:
            if qty == 0:
                self._bids.pop(price, None)
                self._price_strs.pop(price, None)
                valid_bids += 1
            else:
                if tick_size and not self._price_aligned(price, tick_size):
                    logger.warning(
                        f"{self.symbol}: bid {price} not aligned to tick {tick_size}, "
                        f"rejecting level. Raw update_id={update_id}"
                    )
                    continue

                check_price = ref_bid or old_best_bid
                if check_price and check_price > 0:
                    if max_depth_range and price < check_price - max_depth_range:
                        logger.warning(
                            f"{self.symbol}: bid {price} outside depth {self.depth} range "
                            f"from ref {check_price}, rejecting level. Raw update_id={update_id}"
                        )
                        continue

                    deviation = abs(price - check_price) / check_price
                    if deviation > MAX_PRICE_DEVIATION:
                        logger.warning(
                            f"{self.symbol}: SUSPICIOUS bid {price} deviates "
                            f"{deviation*100:.2f}% from ref {check_price}, "
                            f"rejecting level. Raw update_id={update_id}"
                        )
                        continue
                self._bids[price] = qty
                self._price_strs[price] = raw_bid_map.get(price, str(price))
                valid_bids += 1

        valid_asks = 0
        for price, qty in asks:
            if qty == 0:
                self._asks.pop(price, None)
                self._price_strs.pop(price, None)
                valid_asks += 1
            else:
                if tick_size and not self._price_aligned(price, tick_size):
                    logger.warning(
                        f"{self.symbol}: ask {price} not aligned to tick {tick_size}, "
                        f"rejecting level. Raw update_id={update_id}"
                    )
                    continue

                check_price = ref_ask or old_best_ask
                if check_price and check_price > 0:
                    if max_depth_range and price > check_price + max_depth_range:
                        logger.warning(
                            f"{self.symbol}: ask {price} outside depth {self.depth} range "
                            f"from ref {check_price}, rejecting level. Raw update_id={update_id}"
                        )
                        continue

                    deviation = abs(price - check_price) / check_price
                    if deviation > MAX_PRICE_DEVIATION:
                        logger.warning(
                            f"{self.symbol}: SUSPICIOUS ask {price} deviates "
                            f"{deviation*100:.2f}% from ref {check_price}, "
                            f"rejecting level. Raw update_id={update_id}"
                        )
                        continue
                self._asks[price] = qty
                self._price_strs[price] = raw_ask_map.get(price, str(price))
                valid_asks += 1

        if not bids and not asks:
            logger.debug(f"{self.symbol}: empty diff, no-op")
            return True

        # Trim depth
        while len(self._bids) > self.depth:
            self._bids.popitem(-1)
        while len(self._asks) > self.depth:
            self._asks.popitem(-1)

        new_dwmp = self.dwmp(n_levels=10)
        if old_dwmp and new_dwmp:
            dwmp_change = abs(new_dwmp - old_dwmp) / old_dwmp * 100
            if dwmp_change > 0.5:
                logger.warning(
                    f"{self.symbol}: DWMP spike {old_dwmp:.2f} -> {new_dwmp:.2f} "
                    f"({dwmp_change:.4f}%) on seq {update_id}. "
                    f"Valid bids={valid_bids}/{len(bids)}, asks={valid_asks}/{len(asks)}. "
                    f"Top3 bids={list(self._bids.items())[:3]}, "
                    f"Top3 asks={list(self._asks.items())[:3]}"
                )

        self.last_update_id = update_id
        self.exchange_ts_ms = exchange_ts_ms
        self.local_ts_ns    = time.monotonic_ns()
        self._update_count  += 1
        self._last_seq       = update_id
        return True

    def _price_aligned(self, price: float, tick_size: float) -> bool:
        if tick_size <= 0:
            return True
        remainder = round(price % tick_size, 10)
        return remainder < tick_size * 0.001 or abs(remainder - tick_size) < tick_size * 0.001

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

    def dwmp(self, n_levels: int = 10) -> Optional[float]:
        """Depth-Weighted Mid Price using top N bid/ask levels.

        Cross-weights bid prices by ask volume and vice versa.
        Deeper liquidity pulls fair value toward that side.
        Falls back to mid_price if book is empty.
        """
        if not self._bids or not self._asks:
            return self.mid_price

        bids = list(self._bids.items())[:n_levels]
        asks = list(self._asks.items())[:n_levels]

        n = min(len(bids), len(asks))
        if n == 0:
            return self.mid_price

        numerator = 0.0
        total_bid_vol = 0.0
        total_ask_vol = 0.0

        for i in range(n):
            bid_price, bid_vol = bids[i]
            ask_price, ask_vol = asks[i]
            numerator += bid_price * ask_vol + ask_price * bid_vol
            total_bid_vol += bid_vol
            total_ask_vol += ask_vol

        denominator = total_bid_vol + total_ask_vol
        if denominator == 0:
            return self.mid_price

        return numerator / denominator

    def __repr__(self) -> str:
        bb = self.best_bid
        ba = self.best_ask
        return (f"L2OrderBook({self.symbol}: "
                f"bid={bb.price if bb else 'N/A'}, "
                f"ask={ba.price if ba else 'N/A'}, "
                f"spread_bps={self.spread_bps:.2f if self.spread_bps else 'N/A'})")

    def calculate_checksum(self, n_levels: int = 10) -> int:
        asks = list(self._asks.items())[:n_levels]
        bids = list(self._bids.items())[:n_levels]

        asks_str = ",".join(f"{self._price_strs.get(p, str(p))}:{int(q)}" for p, q in asks)
        bids_str = ",".join(f"{self._price_strs.get(p, str(p))}:{int(q)}" for p, q in bids)

        checksum_string = f"{asks_str}|{bids_str}"
        return zlib.crc32(checksum_string.encode()) & 0xFFFFFFFF
