"""
Cross-exchange divergence strategy using last-trade-price fair value.

Implements:
- TradeCollector: rolling volume tracking per exchange
- GlobalFairValue: volume-weighted average of last trade prices
- DivergenceTracker: rolling mean/std of exchange divergences
- ZScoreSignal: signal generation when divergence exceeds threshold
"""
from __future__ import annotations

import time
import numpy as np
from collections import deque
from dataclasses import dataclass
from typing import Dict, Deque, Optional, Tuple


@dataclass
class Trade:
    """Normalized trade from any exchange."""
    symbol: str
    exchange: str
    price: float
    qty: float
    ts_ms: int
    side: str  # "buy" or "sell"
    volume: float = 0.0  # USD volume (auto-computed if 0)


@dataclass
class Signal:
    """Trading signal emitted when divergence is abnormal."""
    exchange: str
    direction: str  # "long" or "short"
    z_score: float
    divergence_pct: float
    gfv: float
    dwmp: float
    ts_ms: int
    net_divergence_pct: float = 0.0  # after fees


class TradeCollector:
    """Collects and stores recent trades per exchange for volume calculation."""

    def __init__(self, window_seconds: int = 60):
        self.window_seconds = window_seconds
        self._trades: Dict[str, Deque[Trade]] = {}

    def add_trade(self, trade: Trade):
        """Add a trade and prune old entries."""
        if trade.exchange not in self._trades:
            self._trades[trade.exchange] = deque()
        self._trades[trade.exchange].append(trade)
        self._prune(trade.exchange)

    def _prune(self, exchange: str):
        """Remove trades older than window."""
        cutoff_ms = int(time.time() * 1000) - (self.window_seconds * 1000)
        trades = self._trades.get(exchange)
        if not trades:
            return
        while trades and trades[0].ts_ms < cutoff_ms:
            trades.popleft()

    def _trade_volume(self, t: Trade) -> float:
        return t.volume if t.volume > 0 else t.price * t.qty

    def get_volume(self, exchange: str) -> float:
        """Get total executed quote volume in the window for an exchange."""
        if exchange not in self._trades:
            return 0.0
        self._prune(exchange)
        return sum(self._trade_volume(t) for t in self._trades[exchange])

    def get_volumes(self) -> Dict[str, float]:
        """Get quote volumes for all exchanges."""
        for exchange in list(self._trades.keys()):
            self._prune(exchange)
        return {
            ex: sum(self._trade_volume(t) for t in trades)
            for ex, trades in self._trades.items()
        }


class GlobalFairValue:
    """
    Computes Global Fair Value (GFV) as volume-weighted average of
    per-exchange last trade prices.

    GFV = sum(Price_j * V_j) / sum(V_j)

    Where V_j is 1-minute rolling executed volume per exchange.
    """

    def __init__(self, trade_collector: TradeCollector):
        self.trade_collector = trade_collector
        self._prices: Dict[str, float] = {}

    def update_price(self, exchange: str, price: float):
        """Update exchange with latest trade price."""
        if price > 0:
            self._prices[exchange] = price

    def compute(self) -> Optional[Tuple[float, Dict[str, float]]]:
        """
        Compute Global Fair Value.

        Returns: (gfv, weights_dict) or None if insufficient data
        """
        volumes = self.trade_collector.get_volumes()

        if not volumes or not self._prices:
            return None

        valid_exchanges = [
            ex for ex in self._prices 
            if volumes.get(ex, 0) > 0 and self._prices[ex] > 0
        ]

        if not valid_exchanges:
            return None

        total_volume = sum(volumes[ex] for ex in valid_exchanges)
        if total_volume == 0:
            return None

        gfv = sum(self._prices[ex] * volumes[ex] for ex in valid_exchanges) / total_volume
        weights = {ex: volumes[ex] / total_volume for ex in valid_exchanges}

        return gfv, weights


class DivergenceTracker:
    """
    Tracks divergence of each exchange's price from Global Fair Value.

    D_j = (Price_j - GFV) / GFV * 100  (in percent)

    Maintains rolling 3-minute baseline (mean, std) per exchange.
    This absorbs permanent exchange differences (e.g., Bybit naturally +0.03%).
    """

    def __init__(self, window_minutes: int = 3):
        self.window_minutes = window_minutes
        self._divergences: Dict[str, Deque[Tuple[float, int]]] = {}

    def record(self, exchange: str, divergence_pct: float, ts_ms: int):
        """Record a divergence observation."""
        if exchange not in self._divergences:
            self._divergences[exchange] = deque()
        self._divergences[exchange].append((divergence_pct, ts_ms))
        self._prune(exchange)

    def _prune(self, exchange: str):
        """Remove entries older than window."""
        cutoff_ms = int(time.time() * 1000) - (self.window_minutes * 60 * 1000)
        entries = self._divergences.get(exchange)
        if not entries:
            return
        while entries and entries[0][1] < cutoff_ms:
            entries.popleft()

    def get_stats(self, exchange: str) -> Optional[Tuple[float, float]]:
        """
        Get rolling mean and std of divergence for an exchange.

        Returns: (mean, std) or None if insufficient data (< 10 samples)
        """
        if exchange not in self._divergences:
            return None

        self._prune(exchange)
        entries = self._divergences[exchange]

        if len(entries) < 10:
            return None

        divergences = [d for d, _ in entries]
        arr = np.array(divergences)

        return float(arr.mean()), float(arr.std())


class ZScoreSignal:
    """
    Generates trading signals when exchange divergence exceeds z-score threshold.

    Short signal: D > 0 and Z > threshold (exchange rich vs market)
    Long signal:  D < 0 and Z < -threshold (exchange cheap vs market)

    The z-score measures how many standard deviations the current divergence
    is from the rolling baseline, automatically handling permanent exchange offsets.
    """

    ROUND_TRIP_FEE = 0.0006  # 0.06% round-trip

    def __init__(self, threshold: float = 2.0, min_divergence_pct: float = 0.02):
        self.threshold = threshold
        self.min_divergence_pct = min_divergence_pct

    def evaluate(self, exchange: str, dwmp: float, gfv: float,
                 divergence_pct: float, mean: float, std: float,
                 ts_ms: int) -> Optional[Signal]:
        """
        Evaluate if current divergence warrants a signal.
        Requires both Z > threshold AND |divergence| > min_divergence_pct.
        """
        if std < 1e-10:
            return None

        if abs(divergence_pct) < self.min_divergence_pct:
            return None

        z_score = (divergence_pct - mean) / std

        if divergence_pct > 0 and z_score > self.threshold:
            net = divergence_pct - self.ROUND_TRIP_FEE
            return Signal(
                exchange=exchange,
                direction="short",
                z_score=z_score,
                divergence_pct=divergence_pct,
                gfv=gfv,
                dwmp=dwmp,
                ts_ms=ts_ms,
                net_divergence_pct=net,
            )

        if divergence_pct < 0 and z_score < -self.threshold:
            net = abs(divergence_pct) - self.ROUND_TRIP_FEE
            return Signal(
                exchange=exchange,
                direction="long",
                z_score=z_score,
                divergence_pct=divergence_pct,
                gfv=gfv,
                dwmp=dwmp,
                ts_ms=ts_ms,
                net_divergence_pct=net,
            )

        return None
