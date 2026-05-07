"""
Latency monitoring and feed statistics.
"""
from __future__ import annotations

import time
import numpy as np
from collections import deque
from dataclasses import dataclass, field
from typing import Deque, Dict, Optional


@dataclass
class FeedStats:
    symbol:           str
    msg_count:        int   = 0
    update_count:     int   = 0
    error_count:      int   = 0
    reconnect_count:  int   = 0
    mean_latency_us:  float = 0.0
    p50_latency_us:   float = 0.0
    p99_latency_us:   float = 0.0
    max_latency_us:   float = 0.0
    throughput_mps:   float = 0.0  # messages/second
    last_ts:          float = 0.0


class LatencyMonitor:
    """
    Real-time latency monitoring for WebSocket feed handlers.
    
    Tracks exchange-to-local latency with percentile statistics.
    Supports rolling window for recent latency computation.
    """

    def __init__(self, window: int = 1000):
        self.window = window
        self._latencies: Dict[str, Deque[float]] = {}
        self._msg_counts: Dict[str, int] = {}
        self._errors: Dict[str, int] = {}
        self._start_times: Dict[str, float] = {}

    def record(self, symbol: str, exchange_ts_ms: int, local_ts_ns: int):
        """Record a latency measurement."""
        if symbol not in self._latencies:
            self._latencies[symbol]   = deque(maxlen=self.window)
            self._msg_counts[symbol]  = 0
            self._errors[symbol]      = 0
            self._start_times[symbol] = time.time()

        local_ms  = local_ts_ns / 1_000_000
        latency_us = (local_ms - exchange_ts_ms) * 1000  # → µs

        if latency_us > 0:  # Ignore negative (clock skew)
            self._latencies[symbol].append(latency_us)
        self._msg_counts[symbol] += 1

    def record_error(self, symbol: str):
        self._errors.setdefault(symbol, 0)
        self._errors[symbol] += 1

    def get_stats(self, symbol: str) -> Optional[FeedStats]:
        if symbol not in self._latencies:
            return None

        lats = list(self._latencies[symbol])
        elapsed = time.time() - self._start_times.get(symbol, time.time())

        stats = FeedStats(symbol=symbol)
        stats.msg_count   = self._msg_counts[symbol]
        stats.error_count = self._errors.get(symbol, 0)

        if lats:
            arr = np.array(lats)
            stats.mean_latency_us = float(arr.mean())
            stats.p50_latency_us  = float(np.percentile(arr, 50))
            stats.p99_latency_us  = float(np.percentile(arr, 99))
            stats.max_latency_us  = float(arr.max())

        if elapsed > 0:
            stats.throughput_mps = stats.msg_count / elapsed

        return stats

    def print_stats(self, symbol: str):
        s = self.get_stats(symbol)
        if not s:
            print(f"No data for {symbol}")
            return
        print(f"\n── Feed Stats: {symbol} ───────────────────────")
        print(f"  Messages       : {s.msg_count:,}")
        print(f"  Throughput     : {s.throughput_mps:.1f} msg/s")
        print(f"  Mean latency   : {s.mean_latency_us:.1f} µs")
        print(f"  P50 latency    : {s.p50_latency_us:.1f} µs")
        print(f"  P99 latency    : {s.p99_latency_us:.1f} µs")
        print(f"  Max latency    : {s.max_latency_us:.1f} µs")
        print(f"  Errors         : {s.error_count}")
